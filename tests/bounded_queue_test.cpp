#include <ripple/concurrent/bounded_queue.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <future>
#include <optional>
#include <thread>
#include <vector>

namespace {

using ripple::BoundedQueue;

// How long to wait before concluding an operation is genuinely blocked. Long
// enough not to be flaky under ThreadSanitizer's slowdown, short enough that the
// suite stays quick.
constexpr auto kBlockedProbe = std::chrono::milliseconds{150};

// ---------------------------------------------------------------------------
// Single-threaded behaviour
// ---------------------------------------------------------------------------

TEST(BoundedQueueTest, DeliversInFifoOrder) {
    BoundedQueue<int> queue(4);
    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));
    EXPECT_TRUE(queue.push(3));

    EXPECT_EQ(queue.pop(), std::optional<int>{1});
    EXPECT_EQ(queue.pop(), std::optional<int>{2});
    EXPECT_EQ(queue.pop(), std::optional<int>{3});
}

TEST(BoundedQueueTest, TracksSizeAndCapacity) {
    BoundedQueue<int> queue(2);
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.capacity(), 2U);

    EXPECT_TRUE(queue.push(1));
    EXPECT_EQ(queue.size(), 1U);
    EXPECT_FALSE(queue.empty());
}

TEST(BoundedQueueTest, TryPopDoesNotBlockOnAnEmptyQueue) {
    BoundedQueue<int> queue(2);
    EXPECT_FALSE(queue.try_pop().has_value());
    EXPECT_TRUE(queue.push(7));
    EXPECT_EQ(queue.try_pop(), std::optional<int>{7});
}

// ---------------------------------------------------------------------------
// Blocking -- the property that makes this backpressure
// ---------------------------------------------------------------------------

// Protects: a full queue actually blocks the producer.
//
// This is the entire backpressure mechanism. If `push` silently grew the queue
// instead of blocking, a slow consumer's backlog would grow until the process
// was OOM-killed -- under exactly the load where you most need it to survive.
//
// Asserted by starting a push that cannot succeed and confirming it has *not*
// completed after a delay, then freeing a slot and confirming it does.
TEST(BoundedQueueTest, PushBlocksWhenFull) {
    BoundedQueue<int> queue(2);
    ASSERT_TRUE(queue.push(1));
    ASSERT_TRUE(queue.push(2));

    std::future<bool> blocked_push =
        std::async(std::launch::async, [&queue] { return queue.push(3); });

    EXPECT_EQ(blocked_push.wait_for(kBlockedProbe), std::future_status::timeout)
        << "push returned on a full queue instead of blocking";

    EXPECT_EQ(queue.pop(), std::optional<int>{1}); // frees a slot

    ASSERT_EQ(blocked_push.wait_for(std::chrono::seconds{2}), std::future_status::ready)
        << "freeing a slot did not wake the blocked producer";
    EXPECT_TRUE(blocked_push.get());
}

// Protects: an empty queue blocks the consumer rather than spinning or
// returning a default-constructed value.
TEST(BoundedQueueTest, PopBlocksWhenEmpty) {
    BoundedQueue<int> queue(2);

    std::future<std::optional<int>> blocked_pop =
        std::async(std::launch::async, [&queue] { return queue.pop(); });

    EXPECT_EQ(blocked_pop.wait_for(kBlockedProbe), std::future_status::timeout);

    ASSERT_TRUE(queue.push(42));

    ASSERT_EQ(blocked_pop.wait_for(std::chrono::seconds{2}), std::future_status::ready);
    EXPECT_EQ(blocked_pop.get(), std::optional<int>{42});
}

// ---------------------------------------------------------------------------
// Close -- what makes graceful shutdown possible
// ---------------------------------------------------------------------------

// Protects: closing wakes a producer blocked on a full queue.
//
// The classic pipeline hang. A consumer thread exits, its queue stays full, and
// the producer blocked in `push` waits forever -- so shutdown never completes.
// It hangs intermittently, usually under load, usually not on the machine you
// tested on.
TEST(BoundedQueueTest, CloseWakesABlockedProducer) {
    BoundedQueue<int> queue(1);
    ASSERT_TRUE(queue.push(1));

    std::future<bool> blocked_push =
        std::async(std::launch::async, [&queue] { return queue.push(2); });
    ASSERT_EQ(blocked_push.wait_for(kBlockedProbe), std::future_status::timeout);

    queue.close();

    ASSERT_EQ(blocked_push.wait_for(std::chrono::seconds{2}), std::future_status::ready)
        << "close() did not wake the blocked producer -- this is the shutdown hang";
    EXPECT_FALSE(blocked_push.get()) << "a push into a closed queue must report failure";
}

// Protects: closing wakes a consumer blocked on an empty queue, and tells it
// the stream is over rather than leaving it waiting.
TEST(BoundedQueueTest, CloseWakesABlockedConsumer) {
    BoundedQueue<int> queue(2);

    std::future<std::optional<int>> blocked_pop =
        std::async(std::launch::async, [&queue] { return queue.pop(); });
    ASSERT_EQ(blocked_pop.wait_for(kBlockedProbe), std::future_status::timeout);

    queue.close();

    ASSERT_EQ(blocked_pop.wait_for(std::chrono::seconds{2}), std::future_status::ready);
    EXPECT_FALSE(blocked_pop.get().has_value());
}

// Protects: THE draining requirement.
//
// A close that discarded queued items would silently lose whatever was in
// flight at shutdown. For a checkpointed pipeline that means losing records the
// source has already marked as read -- data loss that looks like a clean
// shutdown.
TEST(BoundedQueueTest, CloseDrainsAlreadyQueuedItemsBeforeReportingEmpty) {
    BoundedQueue<int> queue(4);
    ASSERT_TRUE(queue.push(1));
    ASSERT_TRUE(queue.push(2));

    queue.close();

    EXPECT_EQ(queue.pop(), std::optional<int>{1});
    EXPECT_EQ(queue.pop(), std::optional<int>{2});
    EXPECT_FALSE(queue.pop().has_value()) << "only now is the stream over";
}

TEST(BoundedQueueTest, PushAfterCloseFailsWithoutEnqueueing) {
    BoundedQueue<int> queue(4);
    queue.close();

    EXPECT_FALSE(queue.push(1));
    EXPECT_EQ(queue.size(), 0U);
    EXPECT_TRUE(queue.is_closed());
}

TEST(BoundedQueueTest, CloseIsIdempotent) {
    BoundedQueue<int> queue(4);
    queue.close();
    queue.close();
    EXPECT_TRUE(queue.is_closed());
    EXPECT_FALSE(queue.pop().has_value());
}

// ---------------------------------------------------------------------------
// Instrumentation
// ---------------------------------------------------------------------------

// Protects: the counter Stage 6 uses to make backpressure visible rather than
// merely asserted. A rising push-block count on one queue localises the slow
// stage immediately.
TEST(BoundedQueueTest, CountsProducerBlocksForBackpressureVisibility) {
    BoundedQueue<int> queue(1);
    ASSERT_TRUE(queue.push(1));
    EXPECT_EQ(queue.push_block_count(), 0U);

    std::future<bool> blocked_push =
        std::async(std::launch::async, [&queue] { return queue.push(2); });
    ASSERT_EQ(blocked_push.wait_for(kBlockedProbe), std::future_status::timeout);

    EXPECT_EQ(queue.push_block_count(), 1U);

    (void)queue.pop();
    ASSERT_EQ(blocked_push.wait_for(std::chrono::seconds{2}), std::future_status::ready);
    (void)blocked_push.get();
}

TEST(BoundedQueueTest, CountsConsumerBlocksWhenStarved) {
    BoundedQueue<int> queue(4);

    std::future<std::optional<int>> blocked_pop =
        std::async(std::launch::async, [&queue] { return queue.pop(); });
    ASSERT_EQ(blocked_pop.wait_for(kBlockedProbe), std::future_status::timeout);

    EXPECT_EQ(queue.pop_block_count(), 1U);

    queue.close();
    (void)blocked_pop.get();
}

// ---------------------------------------------------------------------------
// Contention
// ---------------------------------------------------------------------------

// Protects: no item is lost or duplicated under concurrent producers and
// consumers.
//
// This is the test ThreadSanitizer runs against. Its value is not the assertion
// -- a racy queue can pass a correctness check on any given run -- but the
// unsynchronised access TSan observes while it executes. The assertion catches
// logic errors; TSan catches the races that have not happened to manifest yet.
// Neither substitutes for the other.
TEST(BoundedQueueTest, LosesNoItemsUnderConcurrentProducersAndConsumers) {
    constexpr int kProducers = 4;
    constexpr int kConsumers = 4;
    constexpr int kPerProducer = 500;

    BoundedQueue<int> queue(16); // small on purpose: forces real blocking
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    std::vector<std::vector<int>> received(kConsumers);

    producers.reserve(kProducers);
    for (int producer = 0; producer < kProducers; ++producer) {
        producers.emplace_back([&queue, producer] {
            for (int i = 0; i < kPerProducer; ++i) {
                ASSERT_TRUE(queue.push(producer * kPerProducer + i));
            }
        });
    }

    consumers.reserve(kConsumers);
    for (int consumer = 0; consumer < kConsumers; ++consumer) {
        consumers.emplace_back([&queue, &received, consumer] {
            while (std::optional<int> item = queue.pop()) {
                received[static_cast<std::size_t>(consumer)].push_back(*item);
            }
        });
    }

    for (std::thread& producer : producers) {
        producer.join();
    }
    // Only safe once every producer has finished: closing earlier would reject
    // pushes that were still legitimately coming.
    queue.close();
    for (std::thread& consumer : consumers) {
        consumer.join();
    }

    std::vector<int> all;
    for (const std::vector<int>& per_consumer : received) {
        all.insert(all.end(), per_consumer.begin(), per_consumer.end());
    }
    std::sort(all.begin(), all.end());

    ASSERT_EQ(all.size(), static_cast<std::size_t>(kProducers * kPerProducer))
        << "items were lost or duplicated";
    for (std::size_t i = 0; i < all.size(); ++i) {
        ASSERT_EQ(all[i], static_cast<int>(i)) << "item " << i << " missing or duplicated";
    }
}

// Protects: close() racing against concurrent pops and observers.
//
// This test exists because of a specific discovery: the timing-based tests above
// do NOT catch an unsynchronised write to `closed_`. They sleep std::chrono::milliseconds{150}
// before closing, so the consumer is parked inside `wait` long before the writer runs, and
// ThreadSanitizer only reports a race while it still holds the earlier access in its shadow history
// -- a gap that large evicts it.
//
// Removing the lock from close() was verified to leave every other test in this
// file green under TSan, and to fail this one immediately:
//
//   WARNING: ThreadSanitizer: data race
//     Write of size 1 by thread T2:                            <- close(), unlocked
//     Previous read of size 1 by thread T1 (mutexes: write M0): <- predicate, locked
//
// The lesson generalises past this queue: a green TSan run over tests that never
// actually contend proves nothing. Concurrency tests have to create genuine
// overlap, not merely involve more than one thread.
TEST(BoundedQueueTest, CloseIsSafeAgainstConcurrentPopsAndObservers) {
    for (int trial = 0; trial < 50; ++trial) {
        BoundedQueue<int> queue(4);
        std::vector<std::thread> threads;

        threads.emplace_back([&queue] {
            for (int i = 0; i < 20; ++i) {
                (void)queue.pop();
            }
        });
        threads.emplace_back([&queue] {
            for (int i = 0; i < 20; ++i) {
                (void)queue.push(i);
            }
        });
        // No sleep anywhere: close() must land while the others are mid-flight.
        threads.emplace_back([&queue] { queue.close(); });
        threads.emplace_back([&queue] { (void)queue.is_closed(); });

        for (std::thread& thread : threads) {
            thread.join();
        }
    }
    SUCCEED();
}

// Protects: backpressure propagating from a slow consumer to a fast producer.
//
// A demonstration rather than an edge case -- the producer wants to run flat out
// and is forced to the consumer's pace by nothing more than the queue being
// bounded. No coordination, no signalling, no rate limiter.
TEST(BoundedQueueTest, SlowConsumerThrottlesAFastProducer) {
    BoundedQueue<int> queue(2);
    std::atomic<int> produced{0};

    std::thread producer([&queue, &produced] {
        for (int i = 0; i < 20; ++i) {
            ASSERT_TRUE(queue.push(i));
            produced.fetch_add(1, std::memory_order_relaxed);
        }
    });

    std::this_thread::sleep_for(kBlockedProbe);
    // Capacity 2 plus one item held in the blocked push: the producer cannot
    // have run away from a consumer that has taken nothing.
    EXPECT_LE(produced.load(std::memory_order_relaxed), 3)
        << "producer outran a consumer that has consumed nothing";

    for (int i = 0; i < 20; ++i) {
        EXPECT_EQ(queue.pop(), std::optional<int>{i});
    }
    producer.join();

    EXPECT_GT(queue.push_block_count(), 0U) << "backpressure never actually engaged";
}

} // namespace
