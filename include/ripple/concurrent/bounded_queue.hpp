#pragma once

#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>
#include <utility>

namespace ripple {

/// A fixed-capacity queue that blocks producers when full and consumers when
/// empty.
///
/// ## This is the backpressure mechanism
///
/// The capacity is the whole point. When a downstream stage cannot keep up, its
/// input queue fills, the upstream stage blocks in `push`, so it stops draining
/// *its* input queue, which fills in turn -- all the way back to the source,
/// which stops reading. The pipeline throttles itself to the speed of its
/// slowest stage, with no coordination, no measurement, and no "slow down"
/// message. It is an emergent property of the queue being bounded.
///
/// An unbounded queue does not degrade more gracefully, it fails differently and
/// worse: the slow consumer's backlog grows until the process is killed by the
/// OOM killer -- under precisely the load conditions where you most needed the
/// system to stay up.
///
/// ## Shutdown
///
/// `close()` is what makes graceful shutdown possible, and it is deliberately
/// asymmetric:
///   - producers blocked in `push` wake and get `false`;
///   - consumers blocked in `pop` wake, but keep receiving **already-queued
///     items** until the queue is drained, and only then get `nullopt`.
///
/// That asymmetry is the "drain in-flight work" requirement. A close that
/// discarded queued items would silently lose whatever was in flight at
/// shutdown, which for a checkpointed pipeline means losing records the source
/// has already marked as read.
template<typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity) : capacity_(capacity) {
        assert(capacity > 0 && "a zero-capacity queue can never make progress");
    }

    // Holds a mutex and a condition variable, neither of which is movable, and
    // threads may be blocked on it at any moment. Copying or moving a queue out
    // from under a blocked waiter has no sensible meaning.
    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;
    BoundedQueue(BoundedQueue&&) = delete;
    BoundedQueue& operator=(BoundedQueue&&) = delete;
    ~BoundedQueue() = default;

    /// Blocks while the queue is full. Returns false if the queue is closed,
    /// in which case the value is not enqueued.
    bool push(T value) {
        std::unique_lock<std::mutex> lock(mutex_);

        if (queue_.size() >= capacity_ && !closed_) {
            ++push_block_count_;
        }

        // ## Why `wait` takes a predicate
        //
        // Two separate reasons, and only knowing one of them leads to code that
        // is wrong in the other case.
        //
        // 1. **Spurious wakeups.** `wait` is permitted to return without any
        //    notification at all -- the standard allows it because on some
        //    platforms preventing it costs more than re-checking. Code that
        //    assumes "woke up, therefore the condition holds" reads from an
        //    empty queue.
        //
        // 2. **Stolen wakeups.** Even a genuine notification proves nothing by
        //    the time this thread runs. Between the notifier releasing the lock
        //    and this thread reacquiring it, another consumer can take the slot.
        //    The condition was true when signalled and false on arrival.
        //
        // The predicate form is `while (!pred()) wait(lock);` -- it re-checks
        // after every wake, so both cases are handled by construction.
        not_full_.wait(lock, [this] { return queue_.size() < capacity_ || closed_; });

        if (closed_) {
            return false;
        }

        queue_.push_back(std::move(value));

        // `notify_one`, not `notify_all`: exactly one item became available, so
        // exactly one consumer can make progress. Waking all of them would have
        // them contend for the mutex and all but one go straight back to sleep
        // -- a thundering herd that costs context switches and buys nothing.
        not_empty_.notify_one();
        return true;
    }

    /// Blocks while the queue is empty. Returns `nullopt` only once the queue is
    /// both closed **and** drained.
    [[nodiscard]] std::optional<T> pop() {
        std::unique_lock<std::mutex> lock(mutex_);

        if (queue_.empty() && !closed_) {
            ++pop_block_count_;
        }

        not_empty_.wait(lock, [this] { return !queue_.empty() || closed_; });

        // Closed and empty: the stream is genuinely over. Checking emptiness
        // rather than `closed_` here is what drains in-flight work -- a closed
        // queue still hands out everything it already holds.
        if (queue_.empty()) {
            return std::nullopt;
        }

        T value = std::move(queue_.front());
        queue_.pop_front();
        not_full_.notify_one();
        return value;
    }

    /// Non-blocking. Returns `nullopt` if the queue is momentarily empty, which
    /// is not the same as being finished.
    [[nodiscard]] std::optional<T> try_pop() {
        const std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return std::nullopt;
        }
        T value = std::move(queue_.front());
        queue_.pop_front();
        not_full_.notify_one();
        return value;
    }

    /// Stops accepting new items and wakes everyone currently blocked.
    /// Idempotent, and safe to call from any thread.
    void close() {
        {
            const std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        // ## Why the flag is set under the lock
        //
        // This is the lost-wakeup bug, and it is worth being precise about.
        //
        // `cv.wait(lock, pred)` expands to `while (!pred()) wait(lock)`. The
        // mutex is held while `pred()` runs, and `wait` then *atomically*
        // releases the mutex and enqueues this thread on the condition
        // variable. Atomic with respect to the mutex -- so a thread that also
        // takes the mutex cannot slip between those two steps.
        //
        // Set `closed_ = true` without the lock and it can. A consumer
        // evaluates the predicate, reads `closed_ == false`, decides to sleep;
        // `close()` then sets the flag and calls `notify_all()` before the
        // consumer has enqueued itself; the consumer enqueues and sleeps
        // forever, having missed the only notification it was ever going to
        // get. The program hangs at shutdown, occasionally, on some machines.
        //
        // (It is also a plain data race on `closed_`, which ThreadSanitizer
        // reports even on runs where the timing happens to work out.)
        //
        // Notifying *outside* the lock, as here, is fine and slightly cheaper:
        // a thread woken while the notifier still holds the mutex would only
        // block again immediately on acquiring it.
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    [[nodiscard]] bool is_closed() const {
        const std::lock_guard<std::mutex> lock(mutex_);
        return closed_;
    }

    [[nodiscard]] std::size_t size() const {
        const std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    [[nodiscard]] bool empty() const { return size() == 0; }

    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

    /// How many times a producer had to block because this queue was full.
    ///
    /// The instrumentation Stage 6 needs to *show* backpressure rather than
    /// assert it: a rising count on one queue localises the slow stage
    /// immediately, and it is the first number to look at when throughput is
    /// lower than expected.
    [[nodiscard]] std::size_t push_block_count() const {
        const std::lock_guard<std::mutex> lock(mutex_);
        return push_block_count_;
    }

    /// How many times a consumer had to block because this queue was empty --
    /// the mirror image, indicating a stage that is starved rather than
    /// saturated.
    [[nodiscard]] std::size_t pop_block_count() const {
        const std::lock_guard<std::mutex> lock(mutex_);
        return pop_block_count_;
    }

private:
    /// `mutable` so the observers above can be `const`. They genuinely do not
    /// change the queue's logical state; they just cannot read it safely
    /// without the lock, and an unsynchronised read of `queue_.size()` while
    /// another thread mutates the deque is a data race, not merely a stale
    /// answer.
    mutable std::mutex mutex_;

    /// Two condition variables rather than one.
    ///
    /// With a single variable, a producer freeing a slot would have to
    /// `notify_all` -- it cannot tell whether the waiters are producers or
    /// consumers -- waking every blocked thread so that one can proceed. Two
    /// variables let each notification reach exactly the threads that can act
    /// on it.
    std::condition_variable not_full_;
    std::condition_variable not_empty_;

    std::deque<T> queue_;
    std::size_t capacity_;
    bool closed_ = false;

    std::size_t push_block_count_ = 0;
    std::size_t pop_block_count_ = 0;
};

} // namespace ripple
