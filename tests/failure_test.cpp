// What happens when a piece of the engine fails mid-run.
//
// These tests exist because the answer used to be "it deadlocks". `run_subtask`
// announced end-of-channel as the last statement of the function, so an
// exception skipped it -- the fan-in sink then waited forever for a channel that
// would never report, and `WorkerGroup`'s destructor joined that thread. A second
// deadlock sat behind it: the source kept pushing into the dead subtask's input
// queue until it filled, then blocked in `push`.
//
// Both are the same mistake as writing `join()` on the happy path only: cleanup
// expressed as a statement is cleanup the exception path skips. The fix is RAII,
// which is what the project's own hygiene rules already required.
//
// Every test target carries a CTest timeout, so a regression here fails the
// build rather than hanging CI forever.

#include <ripple/aggregators.hpp>
#include <ripple/collector.hpp>
#include <ripple/operator.hpp>
#include <ripple/operators/keyed.hpp>
#include <ripple/parallel/parallel_pipeline.hpp>
#include <ripple/record.hpp>
#include <ripple/sink.hpp>
#include <ripple/state/state_backend.hpp>
#include <ripple/timestamp.hpp>

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

using ripple::ParallelConfig;
using ripple::Record;

struct Trip {
    std::string zone;
    std::int64_t fare = 0;
};

using Output = ripple::KeyedValue<std::string, std::int64_t>;

const auto kZoneOf = [](const Trip& trip) { return trip.zone; };
const auto kFareOf = [](const Trip& trip) { return trip.fare; };

std::vector<Record<Trip>> make_trips(std::size_t count) {
    std::vector<Record<Trip>> trips;
    trips.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        trips.push_back(
            ripple::make_record(Trip{"zone-" + std::to_string(i % 7), 1},
                                ripple::timestamp_from_millis(static_cast<std::int64_t>(i))));
    }
    return trips;
}

/// Throws once it has seen `fail_after` records.
class ThrowingOperator final : public ripple::Operator<Trip, Output> {
public:
    explicit ThrowingOperator(int fail_after) : fail_after_(fail_after) {}

    // The payload is read and the record dropped when the throw happens, which
    // the check cannot model.
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    void process(Record<Trip>&& record, ripple::Collector<Output>& out) override {
        if (++seen_ == fail_after_) {
            throw std::runtime_error("operator blew up");
        }
        out.collect(
            Record<Output>{Output{record.value.zone, record.value.fare}, record.event_time});
    }

    [[nodiscard]] std::string_view name() const noexcept override { return "throwing"; }

private:
    int fail_after_;
    int seen_ = 0;
};

/// Throws after a few writes.
class ThrowingSink final : public ripple::Sink<Output> {
public:
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    void write(Record<Output>&& /*record*/) override {
        if (++written_ == 5) {
            throw std::runtime_error("sink blew up");
        }
    }

    [[nodiscard]] std::string_view name() const noexcept override { return "throwing-sink"; }

private:
    int written_ = 0;
};

auto working_factory() {
    return [](std::size_t, ripple::StateBackend& backend) {
        return std::unique_ptr<ripple::Operator<Trip, Output>>(ripple::make_keyed_aggregate<Trip>(
            backend, kZoneOf, kFareOf, ripple::SumAggregator<std::int64_t>{}));
    };
}

// Protects: an operator that throws shuts the pipeline down instead of hanging.
//
// The regression this exists for is a deadlock, so the assertion that matters is
// simply that `run` returns at all -- the CTest timeout is the real check.
TEST(FailureTest, AThrowingOperatorDoesNotDeadlockThePipeline) {
    ripple::CollectingSink<Output> sink;
    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 8}, kZoneOf,
        [](std::size_t, ripple::StateBackend&) {
            return std::unique_ptr<ripple::Operator<Trip, Output>>(
                std::make_unique<ThrowingOperator>(5));
        });

    pipeline.run(make_trips(500), sink);

    SUCCEED() << "run() returned rather than deadlocking";
}

// Protects: a failed run is distinguishable from a successful one.
//
// The more important half. Shutting down cleanly on failure is only an
// improvement if the caller can tell it happened -- otherwise the engine has
// merely converted a hang into a silent, incomplete result, which is worse.
TEST(FailureTest, ReportsWorkerFailuresSoAFailedRunIsNotMistakenForACleanOne) {
    ripple::CollectingSink<Output> sink;
    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 8}, kZoneOf,
        [](std::size_t, ripple::StateBackend&) {
            return std::unique_ptr<ripple::Operator<Trip, Output>>(
                std::make_unique<ThrowingOperator>(5));
        });

    pipeline.run(make_trips(500), sink);

    const auto& failures = pipeline.metrics().worker_failures;
    ASSERT_FALSE(failures.empty()) << "a run that lost data reported no failure";
    EXPECT_EQ(failures[0].what, "operator blew up");
    EXPECT_TRUE(failures[0].worker.starts_with("subtask-"));

    EXPECT_LT(sink.records().size(), 500U)
        << "the run should be incomplete -- this test is not exercising a failure";
}

// Protects: a sink that throws does not leave every subtask blocked pushing into
// a queue nobody will ever drain.
//
// The mirror image of the operator case, and the reason the sink closes its
// queue from a destructor rather than as a trailing statement.
TEST(FailureTest, AThrowingSinkDoesNotDeadlockTheSubtasks) {
    ThrowingSink sink;
    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 4}, kZoneOf, working_factory());

    pipeline.run(make_trips(500), sink);

    const auto& failures = pipeline.metrics().worker_failures;
    ASSERT_FALSE(failures.empty());
    EXPECT_EQ(failures[0].worker, "sink");
}

// Protects: the pipeline terminates when subtasks fail immediately, before any
// of them has done useful work.
//
// Note that *not every* subtask necessarily reports a failure, and that is the
// correct behaviour rather than a gap: the first subtask to die closes its input
// queue, the source's next push into that partition fails, and the source stops
// feeding altogether. A subtask that never received a record never ran the code
// that throws, so it exits normally when its queue is closed.
//
// The assertion is therefore "at least one failure and no output", not "exactly
// four failures" -- which is what the first version of this test asserted, and it
// was the test that was wrong.
TEST(FailureTest, TerminatesWhenSubtasksFailImmediately) {
    ripple::CollectingSink<Output> sink;
    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 4}, kZoneOf,
        [](std::size_t, ripple::StateBackend&) {
            return std::unique_ptr<ripple::Operator<Trip, Output>>(
                std::make_unique<ThrowingOperator>(1));
        });

    pipeline.run(make_trips(500), sink);

    EXPECT_FALSE(pipeline.metrics().worker_failures.empty());
    EXPECT_LE(pipeline.metrics().worker_failures.size(), 4U);
    EXPECT_TRUE(sink.records().empty());
}

// Protects: a healthy run still reports no failures.
//
// The negative control. Without it the assertions above could pass on an engine
// that reported a failure for every run.
TEST(FailureTest, ReportsNoFailuresForAHealthyRun) {
    ripple::CollectingSink<Output> sink;
    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 8}, kZoneOf, working_factory());

    pipeline.run(make_trips(500), sink);

    EXPECT_TRUE(pipeline.metrics().worker_failures.empty());
    EXPECT_EQ(sink.records().size(), 500U);
}

} // namespace
