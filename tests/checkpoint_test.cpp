#include <ripple/aggregators.hpp>
#include <ripple/checkpoint/checkpoint_coordinator.hpp>
#include <ripple/operator.hpp>
#include <ripple/operators/keyed.hpp>
#include <ripple/parallel/parallel_pipeline.hpp>
#include <ripple/record.hpp>
#include <ripple/serialization.hpp>
#include <ripple/sink.hpp>
#include <ripple/state/memory_state_backend.hpp>
#include <ripple/state/state_backend.hpp>
#include <ripple/state/state_handles.hpp>
#include <ripple/timestamp.hpp>

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace {

using ripple::CheckpointCoordinator;
using ripple::CheckpointId;
using ripple::CompletedCheckpoint;
using ripple::ParallelConfig;
using ripple::Record;
using ripple::SumAggregator;

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

// ---------------------------------------------------------------------------
// Coordinator in isolation
// ---------------------------------------------------------------------------

TEST(CheckpointCoordinatorTest, IssuesIncreasingIds) {
    CheckpointCoordinator coordinator(2);
    EXPECT_EQ(coordinator.trigger(0), 1);
    EXPECT_EQ(coordinator.trigger(10), 2);
    EXPECT_EQ(coordinator.trigger(20), 3);
}

// Protects: a checkpoint is not usable until EVERY task has acknowledged.
//
// A partial checkpoint is not partially useful, it is corrupt. Restoring from a
// set of snapshots where one task never reported would reset the others to the
// cut while that task kept state from some later point -- the cut would not be a
// cut, and exactly-once would be silently untrue.
TEST(CheckpointCoordinatorTest, DoesNotCompleteUntilEveryTaskAcknowledges) {
    CheckpointCoordinator coordinator(3);
    const CheckpointId id = coordinator.trigger(100);

    coordinator.acknowledge(id, 0, ripple::serialize<std::int64_t>(1));
    EXPECT_FALSE(coordinator.is_complete(id));
    EXPECT_EQ(coordinator.pending_count(), 1U);

    coordinator.acknowledge(id, 1, ripple::serialize<std::int64_t>(2));
    EXPECT_FALSE(coordinator.is_complete(id));

    coordinator.acknowledge(id, 2, ripple::serialize<std::int64_t>(3));
    EXPECT_TRUE(coordinator.is_complete(id));
    EXPECT_EQ(coordinator.pending_count(), 0U);
    EXPECT_EQ(coordinator.completed_count(), 1U);
}

TEST(CheckpointCoordinatorTest, RetainsSourceOffsetAndTaskState) {
    CheckpointCoordinator coordinator(2);
    const CheckpointId id = coordinator.trigger(4'242);
    coordinator.acknowledge(id, 0, ripple::serialize<std::int64_t>(7));
    coordinator.acknowledge(id, 1, ripple::serialize<std::int64_t>(9));

    const CompletedCheckpoint checkpoint = require_latest(coordinator);
    EXPECT_EQ(checkpoint.id, id);
    EXPECT_EQ(checkpoint.source_offset, 4'242U);
    ASSERT_EQ(checkpoint.task_state.size(), 2U);
    EXPECT_EQ(ripple::deserialize<std::int64_t>(checkpoint.task_state.at(0)), 7);
    EXPECT_EQ(ripple::deserialize<std::int64_t>(checkpoint.task_state.at(1)), 9);
}

// Protects: a late acknowledgement cannot mutate an already-complete checkpoint.
//
// Not mere tolerance -- correctness. A completed checkpoint describes one
// specific cut; accepting a straggler afterwards would silently replace one
// task's snapshot with state from a later point, and the set would no longer
// describe any single cut at all.
TEST(CheckpointCoordinatorTest, IgnoresAcknowledgementsForCompletedCheckpoints) {
    CheckpointCoordinator coordinator(2);
    const CheckpointId id = coordinator.trigger(0);
    coordinator.acknowledge(id, 0, ripple::serialize<std::int64_t>(1));
    coordinator.acknowledge(id, 1, ripple::serialize<std::int64_t>(2));
    ASSERT_TRUE(coordinator.is_complete(id));

    coordinator.acknowledge(id, 0, ripple::serialize<std::int64_t>(999));

    const CompletedCheckpoint checkpoint = require_latest(coordinator);
    EXPECT_EQ(ripple::deserialize<std::int64_t>(checkpoint.task_state.at(0)), 1)
        << "a late acknowledgement overwrote a completed checkpoint";
}

TEST(CheckpointCoordinatorTest, TracksMultipleCheckpointsInFlight) {
    CheckpointCoordinator coordinator(2);
    const CheckpointId first = coordinator.trigger(10);
    const CheckpointId second = coordinator.trigger(20);
    EXPECT_EQ(coordinator.pending_count(), 2U);

    // Completed out of order, which the algorithm permits: each barrier defines
    // its own cut independently.
    coordinator.acknowledge(second, 0, {});
    coordinator.acknowledge(second, 1, {});
    EXPECT_TRUE(coordinator.is_complete(second));
    EXPECT_FALSE(coordinator.is_complete(first));

    coordinator.acknowledge(first, 0, {});
    coordinator.acknowledge(first, 1, {});
    EXPECT_TRUE(coordinator.is_complete(first));
    EXPECT_EQ(coordinator.completed_count(), 2U);
}

// ---------------------------------------------------------------------------
// End to end, in a running parallel pipeline
// ---------------------------------------------------------------------------

struct Trip {
    std::string zone;
    std::int64_t fare;
};

using Output = ripple::KeyedValue<std::string, std::int64_t>;

const auto kZoneOf = [](const Trip& trip) { return trip.zone; };
const auto kFareOf = [](const Trip& trip) { return trip.fare; };
const auto kZoneHash = [](const Trip& trip) { return std::hash<std::string>{}(trip.zone); };

auto make_operator_factory() {
    return [](std::size_t /*subtask*/, ripple::StateBackend& backend) {
        return std::unique_ptr<ripple::Operator<Trip, Output>>(ripple::make_keyed_aggregate<Trip>(
            backend, kZoneOf, kFareOf, SumAggregator<std::int64_t>{}));
    };
}

/// Named once rather than written as a braced list of string literals at each
/// use site: `for (const std::string& z : {"a", "b"})` binds the reference to a
/// temporary constructed on every iteration. Clang accepts it silently; GCC's
/// -Wrange-loop-construct catches it, which is why CI builds with both (D-006).
const std::vector<std::string> kTrackedZones{"a", "b", "c", "d"};
const std::vector<std::string> kRestoredZones{"midtown", "queens"};

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

// Protects: checkpoints actually complete while the pipeline is running, with a
// snapshot from every task.
//
// Nothing pauses to make this happen. Barriers ride the same queues as records,
// each task snapshots when its barrier arrives, and the coordinator counts
// acknowledgements. There is no stop-the-world anywhere in the design.
TEST(CheckpointPipelineTest, CompletesCheckpointsWithoutPausingThePipeline) {
    constexpr std::size_t kParallelism = 4;
    const std::vector<Record<Trip>> input = make_trips({"a", "b", "c", "d", "e"}, 40);

    CheckpointCoordinator coordinator(kParallelism + 1); // subtasks + sink
    ripple::CollectingSink<Output> sink;

    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{
            .parallelism = kParallelism, .queue_capacity = 16, .checkpoint_interval_records = 25},
        kZoneHash, make_operator_factory());
    pipeline.run(input, sink, &coordinator);

    EXPECT_EQ(sink.records().size(), input.size()) << "checkpointing must not drop records";
    EXPECT_GE(coordinator.completed_count(), 2U) << "no checkpoint completed";

    for (const CompletedCheckpoint& checkpoint : coordinator.completed()) {
        EXPECT_EQ(checkpoint.task_state.size(), kParallelism + 1)
            << "checkpoint " << checkpoint.id << " completed without every task";
    }
}

// Protects: THE property of the whole algorithm -- every checkpoint describes a
// single consistent cut through the stream.
//
// The keyed aggregate emits exactly one record per input, so the number of
// records the sink had written when it snapshotted must equal the source offset
// recorded when the barrier was injected. Equality here means the snapshot
// contains every record before the cut and none after it, across all four
// subtasks running independently.
//
// This is what barrier alignment buys. Without it the sink would process
// post-barrier records from whichever subtask ran ahead, its snapshot would
// count more records than the source had emitted at the cut, and those records
// would be both baked into the checkpoint *and* replayed on recovery -- counted
// twice. That difference is exactly aligned (exactly-once) versus unaligned
// (at-least-once) checkpointing.
TEST(CheckpointPipelineTest, EveryCheckpointDescribesAConsistentCut) {
    constexpr std::size_t kParallelism = 4;
    const std::vector<Record<Trip>> input = make_trips({"a", "b", "c", "d", "e", "f", "g"}, 30);

    CheckpointCoordinator coordinator(kParallelism + 1);
    ripple::CollectingSink<Output> sink;

    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = kParallelism,
                       .queue_capacity = 8, // small, so subtasks genuinely diverge
                       .checkpoint_interval_records = 20},
        kZoneHash, make_operator_factory());
    pipeline.run(input, sink, &coordinator);

    const auto checkpoints = coordinator.completed();
    ASSERT_GE(checkpoints.size(), 3U);

    for (const CompletedCheckpoint& checkpoint : checkpoints) {
        const auto sink_records =
            ripple::deserialize<std::size_t>(checkpoint.task_state.at(kParallelism));
        EXPECT_EQ(sink_records, checkpoint.source_offset)
            << "checkpoint " << checkpoint.id << " snapshotted " << sink_records
            << " records but the source was at offset " << checkpoint.source_offset
            << " -- the cut is not consistent";
    }
}

// Protects: a barrier never overtakes the records ahead of it.
//
// The ordering guarantee the whole algorithm rests on. Each subtask's snapshot
// is taken when the barrier reaches it, so the total state across subtasks at
// checkpoint N must account for exactly N records -- no more (a barrier jumped a
// queue) and no fewer (a record jumped a barrier).
TEST(CheckpointPipelineTest, BarriersDoNotOvertakeRecords) {
    constexpr std::size_t kParallelism = 3;
    const std::vector<Record<Trip>> input = make_trips({"a", "b", "c", "d"}, 25);

    CheckpointCoordinator coordinator(kParallelism + 1);
    ripple::CollectingSink<Output> sink;

    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{
            .parallelism = kParallelism, .queue_capacity = 4, .checkpoint_interval_records = 30},
        kZoneHash, make_operator_factory());
    pipeline.run(input, sink, &coordinator);

    for (const CompletedCheckpoint& checkpoint : coordinator.completed()) {
        // Rebuild each subtask's state backend from its snapshot and add up the
        // per-key running totals. Every fare is 1, so the sum is a record count.
        std::int64_t total = 0;
        for (std::size_t subtask = 0; subtask < kParallelism; ++subtask) {
            ripple::MemoryStateBackend restored;
            ripple::ByteReader reader(checkpoint.task_state.at(subtask));
            restored.restore_snapshot(reader);

            const ripple::AggregatingState<SumAggregator<std::int64_t>> state(restored,
                                                                              "keyed-aggregate");
            for (const std::string& zone : kTrackedZones) {
                restored.set_current_key(ripple::serialize(zone));
                if (!state.empty()) {
                    total += state.get();
                }
            }
        }
        EXPECT_EQ(total, static_cast<std::int64_t>(checkpoint.source_offset))
            << "checkpoint " << checkpoint.id
            << " accounts for a different number of records than the source had emitted";
    }
}

// Protects: state captured in a checkpoint round-trips back into a live backend.
//
// The bridge to Stage 8: a checkpoint is only worth taking if it can be restored.
TEST(CheckpointPipelineTest, SnapshottedStateRestoresIntoAWorkingBackend) {
    constexpr std::size_t kParallelism = 2;
    const std::vector<Record<Trip>> input = make_trips({"midtown", "queens"}, 20);

    CheckpointCoordinator coordinator(kParallelism + 1);
    ripple::CollectingSink<Output> sink;

    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{
            .parallelism = kParallelism, .queue_capacity = 8, .checkpoint_interval_records = 10},
        kZoneHash, make_operator_factory());
    pipeline.run(input, sink, &coordinator);

    const CompletedCheckpoint checkpoint = require_latest(coordinator);

    std::int64_t restored_total = 0;
    for (std::size_t subtask = 0; subtask < kParallelism; ++subtask) {
        ripple::MemoryStateBackend backend;
        ripple::ByteReader reader(checkpoint.task_state.at(subtask));
        backend.restore_snapshot(reader);
        EXPECT_TRUE(reader.exhausted()) << "snapshot and restore disagree about the format";

        const ripple::AggregatingState<SumAggregator<std::int64_t>> state(backend,
                                                                          "keyed-aggregate");
        for (const std::string& zone : kRestoredZones) {
            backend.set_current_key(ripple::serialize(zone));
            if (!state.empty()) {
                restored_total += state.get();
            }
        }
    }
    EXPECT_EQ(restored_total, static_cast<std::int64_t>(checkpoint.source_offset));
}

// Protects: with checkpointing disabled the pipeline behaves exactly as before.
TEST(CheckpointPipelineTest, RunsUnchangedWhenCheckpointingIsDisabled) {
    const std::vector<Record<Trip>> input = make_trips({"a", "b"}, 10);

    CheckpointCoordinator coordinator(5);
    ripple::CollectingSink<Output> sink;

    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 8, .checkpoint_interval_records = 0},
        kZoneHash, make_operator_factory());
    pipeline.run(input, sink, &coordinator);

    EXPECT_EQ(sink.records().size(), input.size());
    EXPECT_EQ(coordinator.completed_count(), 0U);
}

} // namespace
