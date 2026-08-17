#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <stop_token>
#include <thread>

// Compile-time gate. If the toolchain quietly fell back to an older standard,
// std::jthread and std::stop_token vanish and Stage 5's thread lifecycle plan
// with it. Better to fail here, loudly, than to discover it five stages later.
static_assert(__cplusplus >= 202002L, "Ripple requires C++20");

namespace {

// Protects: that std::jthread really joins in its destructor.
//
// The whole reason we prefer jthread over std::thread is that std::thread
// calls std::terminate() if it is destroyed while still joinable -- so every
// std::thread needs a hand-written join() on every exit path, including the
// exception path. jthread makes the join automatic and unmissable. If that
// property did not hold, `counter` would be read while the worker is still
// writing it, which is a data race.
TEST(Scaffolding, JthreadJoinsOnDestruction) {
    std::atomic<int> counter{0};
    {
        const std::jthread worker([&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
    } // destructor joins here
    EXPECT_EQ(counter.load(std::memory_order_relaxed), 1);
}

// Protects: that a jthread's destructor requests a stop *before* it joins.
//
// This is the cooperative-cancellation contract Stage 5's shutdown path is
// built on. If the destructor only joined without requesting a stop, this
// worker would loop forever and the test would hang rather than fail -- which
// is exactly the shutdown bug we are trying to make impossible later.
TEST(Scaffolding, StopTokenSignalsCooperativeShutdown) {
    std::atomic<bool> observed_stop{false};
    {
        const std::jthread worker([&observed_stop](const std::stop_token& token) {
            while (!token.stop_requested()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            observed_stop.store(true, std::memory_order_relaxed);
        });
    }
    EXPECT_TRUE(observed_stop.load(std::memory_order_relaxed));
}

} // namespace
