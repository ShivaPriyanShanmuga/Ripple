#include <ripple/aggregators.hpp>
#include <ripple/checkpoint/checkpoint_coordinator.hpp>
#include <ripple/operator.hpp>
#include <ripple/operators/chain.hpp>
#include <ripple/operators/keyed.hpp>
#include <ripple/operators/watermark_generator.hpp>
#include <ripple/operators/window.hpp>
#include <ripple/parallel/parallel_pipeline.hpp>
#include <ripple/record.hpp>
#include <ripple/sink.hpp>
#include <ripple/sinks/idempotent_sink.hpp>
#include <ripple/state/state_backend.hpp>
#include <ripple/timestamp.hpp>
#include <ripple/window.hpp>
#include <ripple/window_assigners.hpp>

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <utility>
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
const auto kOutputKey = [](const Output& out) { return out.key; };

using Sink = ripple::IdempotentSink<Output, decltype(kOutputKey)>;

auto make_operator_factory() {
    return [](std::size_t /*subtask*/, ripple::StateBackend& backend) {
        return std::unique_ptr<ripple::Operator<Trip, Output>>(ripple::make_keyed_aggregate<Trip>(
            backend, kZoneOf, kFareOf, SumAggregator<std::int64_t>{}));
    };
}

auto make_pipeline(std::size_t parallelism = kParallelism) {
    return ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = parallelism,
                       .queue_capacity = 8,
                       .checkpoint_interval_records = kCheckpointInterval},
        kZoneOf, make_operator_factory());
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

// Protects: a checkpoint taken at one parallelism restores at another.
//
// This is what key groups buy. A key's group is `hash(key) % kMaxKeyGroups` and
// never changes; only which subtask owns that group does. On restore each
// subtask reads *every* old snapshot and keeps the groups it now owns, so state
// follows its keys instead of being stranded on a subtask index that no longer
// means the same thing.
//
// Without key groups -- partitioning directly on `hash(key) % parallelism` --
// every key moves when the parallelism changes and none of them find their
// state. Scaling up or down would mean starting from zero.
//
// Both directions are exercised: scaling down concentrates several old subtasks'
// groups onto one, scaling up splits one old subtask's groups across several.
TEST(RecoveryTest, RescalesToADifferentParallelismAcrossACheckpoint) {
    const std::vector<Record<Trip>> input = make_trips(kZones, 20);
    const auto expected = uninterrupted_totals(input);

    for (const std::size_t new_parallelism : {1U, 2U, 3U, 7U, 8U}) {
        CheckpointCoordinator first_run(kParallelism + 1);
        Sink sink(kOutputKey);
        {
            auto pipeline = make_pipeline(kParallelism);
            pipeline.run(input, sink, &first_run, RunOptions{.fail_after_records = 60});
        }

        const CompletedCheckpoint checkpoint = require_latest(first_run);
        ASSERT_EQ(checkpoint.parallelism, kParallelism);

        CheckpointCoordinator second_run(new_parallelism + 1);
        {
            auto pipeline = make_pipeline(new_parallelism);
            pipeline.run(input, sink, &second_run, RunOptions{.restore_from = &checkpoint});
        }

        EXPECT_EQ(final_totals(sink), expected) << "rescaling from " << kParallelism << " to "
                                                << new_parallelism << " lost or duplicated state";
    }
}

// Protects: rescaling loses nothing even with no failure involved.
//
// A clean stop and restart at a different parallelism is the *planned* rescale --
// the operational case, as opposed to the crash case above.
TEST(RecoveryTest, RescalesAfterACleanCheckpoint) {
    const std::vector<Record<Trip>> input = make_trips(kZones, 20);
    const auto expected = uninterrupted_totals(input);

    CheckpointCoordinator first_run(kParallelism + 1);
    Sink sink(kOutputKey);
    {
        auto pipeline = make_pipeline(kParallelism);
        pipeline.run(input, sink, &first_run, RunOptions{.fail_after_records = 45});
    }
    const CompletedCheckpoint checkpoint = require_latest(first_run);

    CheckpointCoordinator second_run(8 + 1);
    {
        auto pipeline = make_pipeline(8);
        pipeline.run(input, sink, &second_run, RunOptions{.restore_from = &checkpoint});
    }

    EXPECT_EQ(final_totals(sink), expected);
}

// ---------------------------------------------------------------------------
// Windowed recovery
// ---------------------------------------------------------------------------

// Protects: a WINDOWED parallel pipeline recovers correctly.
//
// This test exists because of a specific blind spot. Every other recovery test
// here uses a keyed aggregate, whose state lives in the StateBackend and is
// therefore covered by the backend snapshot. A window operator keeps its state in
// its own map, because it is indexed by (key, window) which the backend's flat
// interface does not model -- so it is covered only by
// `Operator::snapshot_state`, which defaults to a no-op.
//
// That default meant windowed jobs checkpointed nothing at all, and no test in
// this file noticed. It was found by running the demo application. Recovery
// restarted every partially-filled window from empty and produced totals that
// were plausible and quietly short -- no crash, no error, just a report missing
// most of its rows.
//
// The lesson generalises past this bug: a test suite that only exercises one
// *kind* of state proves nothing about the other kinds.
using WindowedOutput = ripple::WindowResult<std::string, std::int64_t>;

auto make_windowed_factory() {
    return [](std::size_t, ripple::StateBackend&) {
        auto watermarks =
            ripple::make_bounded_out_of_orderness_watermarks<Trip>(ripple::Duration{50});
        auto window = ripple::make_keyed_window<Trip>(
            kZoneOf, kFareOf, ripple::TumblingWindows{ripple::Duration{500}},
            SumAggregator<std::int64_t>{});
        return std::unique_ptr<ripple::Operator<Trip, WindowedOutput>>(
            ripple::make_chain<Trip, Trip, WindowedOutput>(std::move(watermarks), std::move(window),
                                                           "windowed"));
    };
}

auto make_windowed_pipeline(std::size_t parallelism) {
    return ripple::make_parallel_pipeline<Trip, WindowedOutput>(
        ripple::ParallelConfig{.parallelism = parallelism,
                               .queue_capacity = 16,
                               .checkpoint_interval_records = kCheckpointInterval},
        kZoneOf, make_windowed_factory());
}

/// Windows are keyed by (zone, window start) and, because allowed lateness lets a
/// window fire more than once, must be upserted rather than accumulated.
std::map<std::pair<std::string, std::int64_t>, std::int64_t>
window_totals(const std::vector<Record<WindowedOutput>>& records) {
    std::map<std::pair<std::string, std::int64_t>, std::int64_t> totals;
    for (const Record<WindowedOutput>& record : records) {
        totals[{record.value.key, ripple::millis_since_epoch(record.value.window.start)}] =
            record.value.value;
    }
    return totals;
}

TEST(WindowedRecoveryTest, RecoversWindowStateAcrossACrash) {
    const std::vector<Record<Trip>> input = make_trips(kZones, 40);

    // Reference: no failure.
    std::map<std::pair<std::string, std::int64_t>, std::int64_t> expected;
    {
        ripple::CollectingSink<WindowedOutput> sink;
        CheckpointCoordinator coordinator(kParallelism + 1);
        auto pipeline = make_windowed_pipeline(kParallelism);
        pipeline.run(input, sink, &coordinator);
        expected = window_totals(sink.records());
    }
    ASSERT_FALSE(expected.empty()) << "the reference run produced no windows";

    // Crash, then recover into the same result set -- as an idempotent sink
    // downstream of a real job would.
    ripple::CollectingSink<WindowedOutput> crashed_sink;
    CheckpointCoordinator first_run(kParallelism + 1);
    {
        auto pipeline = make_windowed_pipeline(kParallelism);
        pipeline.run(input, crashed_sink, &first_run, RunOptions{.fail_after_records = 120});
    }
    const CompletedCheckpoint checkpoint = require_latest(first_run);

    ripple::CollectingSink<WindowedOutput> recovered_sink;
    CheckpointCoordinator second_run(kParallelism + 1);
    {
        auto pipeline = make_windowed_pipeline(kParallelism);
        pipeline.run(input, recovered_sink, &second_run, RunOptions{.restore_from = &checkpoint});
    }

    auto merged = window_totals(crashed_sink.records());
    for (const auto& [key, value] : window_totals(recovered_sink.records())) {
        merged[key] = value;
    }

    EXPECT_EQ(merged, expected)
        << "windowed state did not survive the checkpoint -- window contents are operator "
           "state and are only captured by Operator::snapshot_state";
}

// Protects: a windowed pipeline also survives a change in parallelism.
//
// Window state is *operator* state, which D-062 says is restored only at
// unchanged parallelism -- so on a rescale the windows deliberately start empty
// and the run replays from the checkpoint offset. This asserts that the outcome
// is still correct rather than silently short, which is the property that
// actually matters.
TEST(WindowedRecoveryTest, RescalingAWindowedPipelineStillConverges) {
    const std::vector<Record<Trip>> input = make_trips(kZones, 40);

    std::map<std::pair<std::string, std::int64_t>, std::int64_t> expected;
    {
        ripple::CollectingSink<WindowedOutput> sink;
        CheckpointCoordinator coordinator(kParallelism + 1);
        auto pipeline = make_windowed_pipeline(kParallelism);
        pipeline.run(input, sink, &coordinator);
        expected = window_totals(sink.records());
    }

    ripple::CollectingSink<WindowedOutput> sink;
    CheckpointCoordinator first_run(kParallelism + 1);
    {
        auto pipeline = make_windowed_pipeline(kParallelism);
        pipeline.run(input, sink, &first_run, RunOptions{.fail_after_records = 100});
    }
    const CompletedCheckpoint checkpoint = require_latest(first_run);

    ripple::CollectingSink<WindowedOutput> rescaled_sink;
    CheckpointCoordinator second_run(2 + 1);
    {
        auto pipeline = make_windowed_pipeline(2);
        pipeline.run(input, rescaled_sink, &second_run, RunOptions{.restore_from = &checkpoint});
    }

    auto merged = window_totals(sink.records());
    for (const auto& [key, value] : window_totals(rescaled_sink.records())) {
        merged[key] = value;
    }
    EXPECT_EQ(merged, expected);
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
