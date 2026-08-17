#pragma once

#include <cstddef>
#include <functional>
#include <mutex>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace ripple {

/// A worker that finished by throwing.
struct WorkerFailure {
    std::string worker;
    std::string what;
};

/// Owns a set of worker threads and guarantees they are stopped and joined.
///
/// ## Why `std::jthread`
///
/// `std::thread`'s destructor calls `std::terminate()` if the thread is still
/// joinable. That makes every `std::thread` a hand-written `join()` on *every*
/// exit path -- including the ones an exception takes -- and the one that gets
/// forgotten kills the process. `std::jthread` requests a stop and joins in its
/// own destructor, so correct shutdown is the default rather than a discipline.
///
/// This class holds `std::vector<std::jthread>`, which already gives correct
/// RAII shutdown for free. What it adds is naming, failure capture, and an
/// explicit `request_stop()`/`join()` split, which matters because shutdown
/// order is not arbitrary (see below).
///
/// ## The shutdown trap: a stop request does not wake a blocked thread
///
/// `stop_token` is **cooperative**. It sets a flag and fires callbacks; it does
/// not interrupt anything. A worker sitting in `BoundedQueue::pop()` is blocked
/// inside `std::condition_variable::wait`, which knows nothing about stop
/// tokens, and will sit there forever no matter how many times you call
/// `request_stop()`.
///
/// So shutting down a pipeline is two steps, in this order:
///   1. **close the queues**, which wakes every blocked producer and consumer;
///   2. **request stop and join**, which this class's destructor does.
///
/// Do only the second and you get a hang -- intermittently, usually under load,
/// usually not on the machine you are testing on. This is the single most
/// common way a multi-stage pipeline fails to shut down, and it is why `close()`
/// exists on the queue rather than shutdown being expressible with stop tokens
/// alone.
///
/// (`std::condition_variable_any` does offer a stop-token-aware `wait`, which
/// would collapse the two steps into one. It is rejected here: it works with
/// any lockable rather than only `unique_lock<mutex>`, and pays for that
/// generality on every wait. Closing the queue is also the more honest model --
/// "this stream is finished" is information the *queue* has, not the thread.)
class WorkerGroup {
public:
    WorkerGroup() = default;

    /// Requests a stop and joins every worker.
    ///
    /// Note what this destructor cannot do: it cannot wake a worker blocked on
    /// a queue. Close the queues first, or this will block forever.
    ~WorkerGroup() {
        request_stop();
        join();
    }

    WorkerGroup(const WorkerGroup&) = delete;
    WorkerGroup& operator=(const WorkerGroup&) = delete;
    WorkerGroup(WorkerGroup&&) = delete;
    WorkerGroup& operator=(WorkerGroup&&) = delete;

    /// Starts a worker. `body` should return when `token.stop_requested()`, or
    /// when its input is exhausted.
    ///
    /// `std::function` here is deliberate and not a contradiction of D-014:
    /// this is called once per worker at startup, not once per record. There is
    /// no hot path to protect.
    void spawn(std::string name, std::function<void(std::stop_token)> body) {
        workers_.emplace_back(
            [this, name = std::move(name), body = std::move(body)](std::stop_token token) {
                // An exception escaping a thread's entry point calls
                // std::terminate -- the whole process dies with no diagnostic
                // beyond a stack trace from the wrong thread. Catching it here
                // turns "the process vanished" into a recorded failure the
                // owner can inspect after joining.
                try {
                    body(std::move(token));
                } catch (const std::exception& error) {
                    record_failure(name, error.what());
                } catch (...) {
                    record_failure(name, "unknown exception");
                }
            });
    }

    /// Cooperative only. Wakes nothing that is blocked -- see the class comment.
    void request_stop() noexcept {
        for (std::jthread& worker : workers_) {
            worker.request_stop();
        }
    }

    /// Blocks until every worker has returned. Idempotent.
    void join() {
        for (std::jthread& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    [[nodiscard]] std::size_t size() const noexcept { return workers_.size(); }

    /// Workers that terminated by throwing. Only meaningful after `join()`.
    [[nodiscard]] std::vector<WorkerFailure> failures() const {
        const std::lock_guard<std::mutex> lock(failures_mutex_);
        return failures_;
    }

private:
    void record_failure(const std::string& worker, const std::string& what) {
        const std::lock_guard<std::mutex> lock(failures_mutex_);
        failures_.push_back(WorkerFailure{worker, what});
    }

    mutable std::mutex failures_mutex_;
    std::vector<WorkerFailure> failures_;

    /// Declared last so it is destroyed **first**: members are destroyed in
    /// reverse declaration order, so the jthreads' own destructors join before
    /// `failures_` and its mutex are gone. The explicit `join()` in the
    /// destructor body already guarantees this, but ordering the members
    /// correctly means the class stays sound even if that line is ever removed.
    std::vector<std::jthread> workers_;
};

} // namespace ripple
