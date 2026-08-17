#include <ripple/concurrent/bounded_queue.hpp>
#include <ripple/concurrent/worker_group.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <thread>

namespace {

using ripple::BoundedQueue;
using ripple::WorkerGroup;

constexpr auto kBlockedProbe = std::chrono::milliseconds{150};

// Protects: the RAII contract. The destructor stops and joins, so no explicit
// join is needed on any exit path -- including the one an exception takes.
//
// The reason for preferring jthread over thread: std::thread's destructor calls
// std::terminate() if the thread is still joinable, which makes every thread a
// hand-written join on every exit path, and the one you forget kills the
// process.
TEST(WorkerGroupTest, DestructorStopsAndJoinsWithoutAnExplicitJoin) {
    std::atomic<bool> ran{false};
    std::atomic<bool> finished{false};
    {
        WorkerGroup workers;
        workers.spawn("worker", [&](const std::stop_token& token) {
            ran.store(true);
            while (!token.stop_requested()) {
                std::this_thread::sleep_for(std::chrono::milliseconds{1});
            }
            finished.store(true);
        });
        EXPECT_EQ(workers.size(), 1U);
    }
    EXPECT_TRUE(ran.load());
    EXPECT_TRUE(finished.load()) << "destructor returned before the worker finished";
}

TEST(WorkerGroupTest, RunsEveryWorkerSpawned) {
    std::atomic<int> started{0};
    {
        WorkerGroup workers;
        for (int i = 0; i < 4; ++i) {
            workers.spawn("worker", [&started](const std::stop_token&) {
                started.fetch_add(1, std::memory_order_relaxed);
            });
        }
        EXPECT_EQ(workers.size(), 4U);
    }
    EXPECT_EQ(started.load(), 4);
}

TEST(WorkerGroupTest, JoinIsIdempotent) {
    WorkerGroup workers;
    workers.spawn("worker", [](const std::stop_token&) {});
    workers.join();
    workers.join(); // must not throw or block
    SUCCEED();
}

// Protects: an exception escaping a worker is captured rather than killing the
// process.
//
// An exception leaving a thread's entry point calls std::terminate -- the whole
// process dies, with a stack trace from the wrong thread and no indication of
// which worker was at fault.
TEST(WorkerGroupTest, CapturesWorkerExceptionsInsteadOfTerminating) {
    WorkerGroup workers;
    workers.spawn("faulty",
                  [](const std::stop_token&) { throw std::runtime_error("deliberate failure"); });
    workers.join();

    const auto failures = workers.failures();
    ASSERT_EQ(failures.size(), 1U);
    EXPECT_EQ(failures[0].worker, "faulty");
    EXPECT_EQ(failures[0].what, "deliberate failure");
}

// ---------------------------------------------------------------------------
// The shutdown trap
// ---------------------------------------------------------------------------

// Protects: knowledge of the single most common multi-stage-pipeline hang, by
// asserting the trap directly.
//
// `stop_token` is **cooperative**: it sets a flag and runs callbacks. It does
// not interrupt anything. A worker blocked inside
// `std::condition_variable::wait` -- which is where `BoundedQueue::pop()` puts
// it -- knows nothing about stop tokens and will sit there forever regardless of
// how many times `request_stop()` is called.
//
// So this test deliberately asserts that request_stop does NOT wake the worker.
// That is not a defect being tolerated; it is the contract, and encoding it here
// means nobody later "fixes" shutdown by adding another request_stop and
// declaring victory.
//
// Correct shutdown is two steps, in order: close the queues, then stop and join.
TEST(WorkerGroupTest, StopRequestAloneDoesNotWakeAWorkerBlockedOnAQueue) {
    BoundedQueue<int> queue(4);
    std::atomic<bool> exited{false};

    {
        WorkerGroup workers;
        workers.spawn("consumer", [&](const std::stop_token& token) {
            while (!token.stop_requested()) {
                const std::optional<int> item = queue.pop();
                if (!item.has_value()) {
                    break;
                }
            }
            exited.store(true);
        });

        std::this_thread::sleep_for(kBlockedProbe); // let it reach pop()

        workers.request_stop();
        std::this_thread::sleep_for(kBlockedProbe);

        EXPECT_FALSE(exited.load())
            << "a stop request woke a thread blocked in condition_variable::wait -- "
               "if this ever passes, the shutdown model has changed";

        // THIS is what actually releases it. Without this line the destructor
        // below would block forever and the test would hit its CTest timeout.
        queue.close();
    }

    EXPECT_TRUE(exited.load());
}

// Protects: the correct shutdown sequence works even with work still in flight,
// and that in-flight work is drained rather than dropped.
TEST(WorkerGroupTest, ClosingQueuesThenJoiningDrainsInFlightWork) {
    BoundedQueue<int> queue(64);
    std::atomic<int> consumed{0};

    for (int i = 0; i < 50; ++i) {
        ASSERT_TRUE(queue.push(i));
    }

    {
        WorkerGroup workers;
        workers.spawn("consumer", [&](const std::stop_token&) {
            while (queue.pop().has_value()) {
                consumed.fetch_add(1, std::memory_order_relaxed);
            }
        });

        queue.close(); // step 1: wake and terminate the stream
    } // step 2: destructor stops and joins

    EXPECT_EQ(consumed.load(), 50) << "in-flight work was dropped at shutdown";
}

// Protects: shutdown of a two-stage pipeline, which is where ordering starts to
// matter.
//
// Stage A feeds stage B. Closing A's output queue lets A's consumer finish, but
// B's queue must also be closed or B hangs. Getting this order wrong is how a
// pipeline shuts down cleanly in testing and hangs in production, where the
// queues are actually full.
TEST(WorkerGroupTest, ShutsDownATwoStagePipelineWithoutHanging) {
    BoundedQueue<int> stage_one_to_two(4);
    BoundedQueue<int> stage_two_to_sink(4);
    std::atomic<int> sunk{0};

    {
        WorkerGroup workers;

        workers.spawn("stage-two", [&](const std::stop_token&) {
            while (const std::optional<int> item = stage_one_to_two.pop()) {
                if (!stage_two_to_sink.push(*item * 2)) {
                    break;
                }
            }
            // Closing downstream on the way out is what propagates "the stream
            // is over" along the pipeline. Omitting it leaves the sink blocked.
            stage_two_to_sink.close();
        });

        workers.spawn("sink", [&](const std::stop_token&) {
            while (stage_two_to_sink.pop().has_value()) {
                sunk.fetch_add(1, std::memory_order_relaxed);
            }
        });

        for (int i = 0; i < 100; ++i) {
            ASSERT_TRUE(stage_one_to_two.push(i));
        }
        stage_one_to_two.close(); // close only the head; the rest cascades
    }

    EXPECT_EQ(sunk.load(), 100);
}

} // namespace
