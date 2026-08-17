#include <ripple/aggregators.hpp>
#include <ripple/checkpoint/checkpoint_coordinator.hpp>
#include <ripple/operator.hpp>
#include <ripple/operators/keyed.hpp>
#include <ripple/parallel/parallel_pipeline.hpp>
#include <ripple/record.hpp>
#include <ripple/sink.hpp>
#include <ripple/sinks/idempotent_sink.hpp>
#include <ripple/state/state_backend.hpp>
#include <ripple/timestamp.hpp>

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <vector>

namespace {

using ripple::CheckpointCoordinator;
using ripple::CompletedCheckpoint;
using ripple::ParallelConfig;
using ripple::Record;
using ripple::RunOptions;
using ripple::SumAggregator;

constexpr std::size_t kParallelism = 4;
constexpr std::size_t kCheckpointInterval = 15;

struct Trip {
    std::string zone;
    std::int64_t fare;
};

using Output = ripple::KeyedValue<std::string, std::int64_t>;

const auto kZoneOf = [](const Trip& trip) { return trip.zone; };
const auto kFareOf = [](const Trip& trip) { return trip.fare; };
const auto kZoneHash = [](const Trip& trip) { return std::hash<std::string>{}(trip.zone); };
const auto kOutputKey = [](const Output& out) { return out.key; };

using Sink = ripple::IdempotentSink<Output, decltype(kOutputKey)>;

auto make_operator_factory() {
    return [](std::size_t /*subtask*/, ripple::StateBackend& backend) {
        return std::unique_ptr<ripple::Operator<Trip, Output>>(ripple::make_keyed_aggregate<Trip>(
            backend, kZoneOf, kFareOf, SumAggregator<std::int64_t>{}));
    };
}

auto make_pipeline() {
    return ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = kParallelism,
                       .queue_capacity = 8,
                       .checkpoint_interval_records = kCheckpointInterval},
        kZoneHash, make_operator_factory());
}

/// Every fare is 1, so a zone's final running total equals the number of trips
/// it had -- which makes double-counting immediately visible as a number that is
/// too large.
std::vector<Record<Trip>> make_trips(const std::vector<std::string>& zones, int per_zone) {
    std::vector<Record<Trip>> trips;
    std::int64_t millis = 0;
    for (int i = 0; i < per_zone; ++i) {
        for (const std::string& zone : zones) {
            trips.push_back(
                ripple::make_record(Trip{zone, 1}, ripple::timestamp_from_millis(millis)));
            millis += 10;
        }
    }
    return trips;
}

/// Fetches the latest completed checkpoint, failing the test if there is none.
/// Returning by value through a guarded branch keeps the optional access
/// checked, which ASSERT_TRUE alone does not express to static analysis.
CompletedCheckpoint require_latest(const CheckpointCoordinator& coordinator) {
    const auto checkpoint = coordinator.latest_completed();
    if (!checkpoint.has_value()) {
        ADD_FAILURE() << "no checkpoint completed";
        return {};
    }
    return *checkpoint;
}

/// Null when nothing was checkpointed -- which is a legitimate state, not an
/// error: a crash before the first checkpoint completes means replaying from the
/// beginning.
const CompletedCheckpoint* restore_point(const std::optional<CompletedCheckpoint>& checkpoint) {
    if (!checkpoint.has_value()) {
        return nullptr;
    }
    return &checkpoint.value();
}

std::map<std::string, std::int64_t> final_totals(const Sink& sink) {
    std::map<std::string, std::int64_t> totals;
    for (const auto& [key, value] : sink.values()) {
        totals[key] = value.value;
    }
    return totals;
}

const std::vector<std::string> kZones{"midtown", "brooklyn", "queens", "bronx", "harlem"};

/// Runs to completion with no failure. The answer everything else is compared
/// against.
std::map<std::string, std::int64_t> uninterrupted_totals(const std::vector<Record<Trip>>& input) {
    CheckpointCoordinator coordinator(kParallelism + 1);
    Sink sink(kOutputKey);
    auto pipeline = make_pipeline();
    pipeline.run(input, sink, &coordinator);
    return final_totals(sink);
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

// Protects: a checkpoint's state can be loaded back into a running pipeline.
//
// Restoring means resuming with state already populated *and* the source rewound
// to the offset in that same checkpoint. The two are one fact about one cut:
// restore the state but start the source from scratch and every record before the
// cut is counted twice; rewind the source but start with empty state and
// everything before the cut is lost.
TEST(RecoveryTest, ResumesFromACheckpointWithStateAndOffsetTogether) {
    const std::vector<Record<Trip>> input = make_trips(kZones, 20);
    const auto expected = uninterrupted_totals(input);

    // Phase 1: stop partway through.
    CheckpointCoordinator first_run(kParallelism + 1);
    Sink sink(kOutputKey);
    {
        auto pipeline = make_pipeline();
        pipeline.run(input, sink, &first_run, RunOptions{.fail_after_records = 60});
    }

    const CompletedCheckpoint checkpoint = require_latest(first_run);
    EXPECT_LE(checkpoint.source_offset, 60U);

    // Phase 2: same sink -- it survived the crash, as a real database would.
    CheckpointCoordinator second_run(kParallelism + 1);
    {
        auto pipeline = make_pipeline();
        pipeline.run(input, sink, &second_run, RunOptions{.restore_from = &checkpoint});
    }

    EXPECT_EQ(final_totals(sink), expected);
}

// Protects: THE distinction the phrase "exactly-once" hides.
//
// Delivery is at-least-once -- the sink genuinely receives more writes than there
// were input records, because everything between the checkpoint and the crash is
// re-sent. The *effect* is exactly-once, because those replayed records are
// applied to state that was rewound to before them, and the sink upserts rather
// than accumulates.
//
// Asserting both halves in one test is deliberate: the interesting claim is not
// "the answer is right", it is "the answer is right *despite* duplicate
// delivery".
TEST(RecoveryTest, DeliveryIsAtLeastOnceWhileTheEffectIsExactlyOnce) {
    const std::vector<Record<Trip>> input = make_trips(kZones, 20);
    const auto expected = uninterrupted_totals(input);

    CheckpointCoordinator first_run(kParallelism + 1);
    Sink sink(kOutputKey);
    {
        auto pipeline = make_pipeline();
        pipeline.run(input, sink, &first_run, RunOptions{.fail_after_records = 70});
    }
    const CompletedCheckpoint checkpoint = require_latest(first_run);

    const std::size_t writes_before_failure = sink.write_count();

    CheckpointCoordinator second_run(kParallelism + 1);
    {
        auto pipeline = make_pipeline();
        pipeline.run(input, sink, &second_run, RunOptions{.restore_from = &checkpoint});
    }

    EXPECT_GT(sink.write_count(), input.size())
        << "no records were re-delivered, so this test is not exercising replay";
    EXPECT_GT(writes_before_failure, checkpoint.source_offset)
        << "the sink had gone past the checkpoint, which is what makes replay observable";

    EXPECT_EQ(final_totals(sink), expected) << "duplicate delivery changed the answer";
}

// Protects: replay begins exactly at the checkpoint's offset -- no gap, no
// overlap in what the *source* re-reads.
TEST(RecoveryTest, ReplaysFromExactlyTheCheckpointOffset) {
    const std::vector<Record<Trip>> input = make_trips(kZones, 20);

    CheckpointCoordinator first_run(kParallelism + 1);
    Sink discarded(kOutputKey);
    {
        auto pipeline = make_pipeline();
        pipeline.run(input, discarded, &first_run, RunOptions{.fail_after_records = 55});
    }
    const CompletedCheckpoint checkpoint = require_latest(first_run);

    CheckpointCoordinator second_run(kParallelism + 1);
    Sink replayed(kOutputKey);
    {
        auto pipeline = make_pipeline();
        pipeline.run(input, replayed, &second_run, RunOptions{.restore_from = &checkpoint});
        EXPECT_EQ(replayed.write_count(), input.size() - checkpoint.source_offset)
            << "the source did not resume at the checkpoint offset";
    }
}

// Protects: recovery works even when nothing was checkpointed yet.
//
// A crash before the first checkpoint completes means there is nothing to
// restore, so the job replays from the beginning. That is correct rather than
// exceptional, and the idempotent sink absorbs the full re-delivery.
TEST(RecoveryTest, ReplaysFromTheBeginningWhenNoCheckpointCompleted) {
    const std::vector<Record<Trip>> input = make_trips(kZones, 20);
    const auto expected = uninterrupted_totals(input);

    CheckpointCoordinator first_run(kParallelism + 1);
    Sink sink(kOutputKey);
    {
        auto pipeline = make_pipeline();
        // Fewer records than one checkpoint interval: nothing can complete.
        pipeline.run(input, sink, &first_run, RunOptions{.fail_after_records = 5});
    }
    ASSERT_EQ(first_run.completed_count(), 0U);

    CheckpointCoordinator second_run(kParallelism + 1);
    {
        auto pipeline = make_pipeline();
        pipeline.run(input, sink, &second_run); // no restore: start over
    }

    EXPECT_EQ(final_totals(sink), expected);
}

// ---------------------------------------------------------------------------
// Fault injection harness
// ---------------------------------------------------------------------------

// Protects: the end-to-end claim, across many failure points rather than one
// hand-picked one.
//
// Kills the job at pseudo-random offsets, recovers from whatever checkpoint had
// completed, resumes, and asserts the final state matches an uninterrupted run
// every time. A seeded generator keeps failures reproducible -- a fault-injection
// harness that cannot replay its own failure is close to useless.
//
// The kill points deliberately include offsets before the first checkpoint,
// immediately after a checkpoint, and mid-interval, since those exercise
// different amounts of replay.
TEST(FaultInjectionTest, ConvergesToTheCorrectStateFromAnyFailurePoint) {
    const std::vector<Record<Trip>> input = make_trips(kZones, 24);
    const auto expected = uninterrupted_totals(input);
    ASSERT_FALSE(expected.empty());

    std::mt19937 generator(20260817); // fixed seed: failures must be reproducible
    std::uniform_int_distribution<std::size_t> kill_at(1, input.size() - 1);

    for (int trial = 0; trial < 12; ++trial) {
        const std::size_t kill_point = kill_at(generator);

        CheckpointCoordinator first_run(kParallelism + 1);
        Sink sink(kOutputKey);
        {
            auto pipeline = make_pipeline();
            pipeline.run(input, sink, &first_run, RunOptions{.fail_after_records = kill_point});
        }

        const auto checkpoint = first_run.latest_completed();
        const CompletedCheckpoint* restore = restore_point(checkpoint);
        const std::size_t recovered_from = restore != nullptr ? restore->source_offset : 0;

        CheckpointCoordinator second_run(kParallelism + 1);
        {
            auto pipeline = make_pipeline();
            pipeline.run(input, sink, &second_run, RunOptions{.restore_from = restore});
        }

        EXPECT_EQ(final_totals(sink), expected)
            << "killed after " << kill_point << " records, recovered from offset " << recovered_from
            << " -- final state diverged from an uninterrupted run";
    }
}

// Protects: repeated failures compound correctly.
//
// One crash is the easy case. A job that crashes, partially recovers, and crashes
// again must still converge -- and each recovery has to start from the newest
// completed checkpoint, not the first one.
TEST(FaultInjectionTest, SurvivesRepeatedFailures) {
    const std::vector<Record<Trip>> input = make_trips(kZones, 24);
    const auto expected = uninterrupted_totals(input);

    Sink sink(kOutputKey);
    std::optional<CompletedCheckpoint> checkpoint;

    for (const std::size_t kill_point : {20U, 55U, 90U}) {
        CheckpointCoordinator coordinator(kParallelism + 1);
        auto pipeline = make_pipeline();
        pipeline.run(
            input, sink, &coordinator,
            RunOptions{.restore_from = checkpoint.has_value() ? &checkpoint.value() : nullptr,
                       .fail_after_records = kill_point});
        if (const auto latest = coordinator.latest_completed()) {
            checkpoint = latest;
        }
    }

    // Final attempt, allowed to finish.
    {
        CheckpointCoordinator coordinator(kParallelism + 1);
        auto pipeline = make_pipeline();
        pipeline.run(
            input, sink, &coordinator,
            RunOptions{.restore_from = checkpoint.has_value() ? &checkpoint.value() : nullptr});
    }

    EXPECT_EQ(final_totals(sink), expected);
}

// Protects: an appending sink genuinely double-counts, so the idempotence
// requirement is real rather than defensive.
//
// The negative control for the whole stage. Without it, "the results are correct"
// could be true simply because no replay occurred, and the sink requirement would
// look like an unnecessary precaution.
TEST(FaultInjectionTest, AnAppendingSinkWouldDoubleCount) {
    const std::vector<Record<Trip>> input = make_trips(kZones, 20);

    CheckpointCoordinator first_run(kParallelism + 1);
    ripple::CollectingSink<Output> appending;
    {
        auto pipeline = make_pipeline();
        pipeline.run(input, appending, &first_run, RunOptions{.fail_after_records = 70});
    }
    const CompletedCheckpoint checkpoint = require_latest(first_run);

    CheckpointCoordinator second_run(kParallelism + 1);
    {
        auto pipeline = make_pipeline();
        pipeline.run(input, appending, &second_run, RunOptions{.restore_from = &checkpoint});
    }

    EXPECT_GT(appending.records().size(), input.size())
        << "a plain appending sink received more rows than there were records -- which is "
           "precisely why end-to-end exactly-once needs an idempotent or transactional sink, "
           "not just a correct engine";
}

} // namespace
