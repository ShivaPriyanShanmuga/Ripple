#include <ripple/aggregators.hpp>
#include <ripple/operator.hpp>
#include <ripple/operators/keyed.hpp>
#include <ripple/parallel/parallel_pipeline.hpp>
#include <ripple/pipeline.hpp>
#include <ripple/record.hpp>
#include <ripple/sink.hpp>
#include <ripple/source.hpp>
#include <ripple/state/memory_state_backend.hpp>
#include <ripple/state/state_backend.hpp>
#include <ripple/timestamp.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

using ripple::MemoryStateBackend;
using ripple::ParallelConfig;
using ripple::Record;
using ripple::SumAggregator;

struct Trip {
    std::string zone;
    std::int64_t fare;
};

using Output = ripple::KeyedValue<std::string, std::int64_t>;

const auto kZoneOf = [](const Trip& trip) { return trip.zone; };
const auto kFareOf = [](const Trip& trip) { return trip.fare; };
const auto kZoneHash = [](const Trip& trip) { return std::hash<std::string>{}(trip.zone); };

std::vector<Record<Trip>> make_trips(const std::vector<std::string>& zones, int per_zone) {
    std::vector<Record<Trip>> trips;
    std::int64_t millis = 0;
    for (int i = 0; i < per_zone; ++i) {
        for (const std::string& zone : zones) {
            trips.push_back(
                ripple::make_record(Trip{zone, i + 1}, ripple::timestamp_from_millis(millis)));
            millis += 10;
        }
    }
    return trips;
}

/// Per-key sequence of emitted running totals. Order *across* keys is
/// meaningless once the job is parallel; order *within* a key is not, and is
/// exactly what must be preserved.
std::map<std::string, std::vector<std::int64_t>> by_key(const std::vector<Record<Output>>& out) {
    std::map<std::string, std::vector<std::int64_t>> grouped;
    for (const Record<Output>& record : out) {
        grouped[record.value.key].push_back(record.value.value);
    }
    return grouped;
}

auto make_operator_factory() {
    return [](std::size_t /*subtask*/, ripple::StateBackend& backend) {
        return std::unique_ptr<ripple::Operator<Trip, Output>>(ripple::make_keyed_aggregate<Trip>(
            backend, kZoneOf, kFareOf, SumAggregator<std::int64_t>{}));
    };
}

// ---------------------------------------------------------------------------
// The correctness oracle
// ---------------------------------------------------------------------------

// Protects: parallel output equals single-threaded output.
//
// This is the payoff of building Stages 1-4 single-threaded and deterministic.
// Event-time semantics make output a pure function of input, so there is one
// correct answer to compare against, and any divergence here is a runtime bug --
// known immediately, without having to wonder whether the aggregation logic is
// also wrong.
//
// Note what it does NOT prove. A data race can produce byte-identical output on
// ten thousand runs and still be undefined behaviour. This test and TSan find
// disjoint bug classes: the oracle catches thread-safe logic errors (a wrong
// partitioner, watermarks merged with max instead of min) that TSan cannot see,
// and TSan catches races regardless of whether they affected output. Neither
// substitutes for the other.
TEST(ParallelPipelineTest, ProducesIdenticalResultsToTheSingleThreadedPipeline) {
    const std::vector<std::string> zones{"midtown", "brooklyn", "queens", "bronx", "harlem"};
    const std::vector<Record<Trip>> input = make_trips(zones, 40);

    // --- sequential reference ---
    //
    // The results are copied out *inside* this scope on purpose: `Pipeline` owns
    // its sink, so the raw pointer is dangling the moment the pipeline is
    // destroyed. Reading it afterwards is a use-after-free -- which is exactly
    // what the first version of this test did, and what the segfault caught.
    MemoryStateBackend sequential_backend;
    std::vector<Record<Output>> sequential_results;
    {
        auto sequential_sink = std::make_unique<ripple::CollectingSink<Output>>();
        auto* sequential_sink_ptr = sequential_sink.get();
        auto pipeline =
            ripple::from(std::make_unique<ripple::VectorSource<Trip>>(input))
                .via(ripple::make_keyed_aggregate<Trip>(sequential_backend, kZoneOf, kFareOf,
                                                        SumAggregator<std::int64_t>{}))
                .to(std::move(sequential_sink));
        pipeline.run();
        sequential_results = sequential_sink_ptr->records();
    }

    // --- parallel ---
    ripple::CollectingSink<Output> parallel_sink;
    auto parallel = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 16}, kZoneHash, make_operator_factory());
    parallel.run(input, parallel_sink);

    EXPECT_EQ(by_key(parallel_sink.records()), by_key(sequential_results))
        << "parallel execution changed the answer";
}

// Protects: no record is lost or duplicated by the shuffle.
TEST(ParallelPipelineTest, ProcessesEveryRecordExactlyOnce) {
    const std::vector<Record<Trip>> input = make_trips({"a", "b", "c", "d", "e", "f", "g"}, 100);

    ripple::CollectingSink<Output> sink;
    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 8}, kZoneHash, make_operator_factory());
    pipeline.run(input, sink);

    EXPECT_EQ(sink.records().size(), input.size());

    std::size_t total_handled = 0;
    for (const std::size_t count : pipeline.metrics().records_per_subtask) {
        total_handled += count;
    }
    EXPECT_EQ(total_handled, input.size());
}

// ---------------------------------------------------------------------------
// Partitioning
// ---------------------------------------------------------------------------

// Protects: every record for a given key reaches the same subtask.
//
// The guarantee the whole design rests on. If a key were split across subtasks,
// each would keep a partial running total under the same name -- and because
// each subtask has its own state backend, the two halves would never be
// reconciled. The output would be wrong, plausible, and non-deterministic.
//
// Verified by the totals themselves: the final running total for each zone can
// only be correct if every one of its records was accumulated in one place.
TEST(ParallelPipelineTest, RoutesEveryRecordForAKeyToTheSameSubtask) {
    const std::vector<std::string> zones{"midtown", "brooklyn", "queens", "bronx"};
    const int per_zone = 25;
    const std::vector<Record<Trip>> input = make_trips(zones, per_zone);

    ripple::CollectingSink<Output> sink;
    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 16}, kZoneHash, make_operator_factory());
    pipeline.run(input, sink);

    // fares are 1..per_zone, so a correct per-zone total is the triangular number.
    const std::int64_t expected = static_cast<std::int64_t>(per_zone) * (per_zone + 1) / 2;
    const auto grouped = by_key(sink.records());

    ASSERT_EQ(grouped.size(), zones.size());
    for (const auto& [zone, totals] : grouped) {
        ASSERT_FALSE(totals.empty());
        EXPECT_EQ(totals.back(), expected)
            << "zone " << zone << " was split across subtasks and its state diverged";
    }
}

// Protects: key skew is visible in the metrics.
//
// Hash partitioning sends every record for one key to one subtask. A single hot
// key therefore caps the entire job at one core's throughput no matter how much
// parallelism is configured -- and adding threads cannot fix it, only changing
// the key can. Asserting it here makes the failure mode legible rather than
// leaving it as "the job is mysteriously slow".
TEST(ParallelPipelineTest, ConcentratesAHotKeyOnASingleSubtask) {
    const std::vector<Record<Trip>> input = make_trips({"one-hot-key"}, 400);

    ripple::CollectingSink<Output> sink;
    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 32}, kZoneHash, make_operator_factory());
    pipeline.run(input, sink);

    const auto& per_subtask = pipeline.metrics().records_per_subtask;
    const auto busiest = std::max_element(per_subtask.begin(), per_subtask.end());
    EXPECT_EQ(*busiest, input.size()) << "a single key was somehow spread across subtasks";

    const std::size_t idle =
        static_cast<std::size_t>(std::count(per_subtask.begin(), per_subtask.end(), 0U));
    EXPECT_EQ(idle, per_subtask.size() - 1) << "three of four subtasks should be idle";
}

// ---------------------------------------------------------------------------
// Backpressure
// ---------------------------------------------------------------------------

/// Deliberately slow, to force the queues to fill.
class SlowSink final : public ripple::Sink<Output> {
public:
    explicit SlowSink(std::chrono::microseconds delay) : delay_(delay) {}

    // Discards the record deliberately; only the delay matters here.
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    void write(Record<Output>&& /*record*/) override {
        std::this_thread::sleep_for(delay_);
        ++written_;
    }

    [[nodiscard]] std::string_view name() const noexcept override { return "slow-sink"; }

    [[nodiscard]] std::size_t written() const noexcept { return written_; }

private:
    std::chrono::microseconds delay_;
    std::size_t written_ = 0;
};

// Protects: backpressure propagates from a slow sink all the way back to the
// source, and is observable while it does.
//
// The chain: the sink cannot keep up, so its queue fills; the subtasks block in
// `collect`, so they stop draining their own input queues; those fill, and the
// source blocks pushing into them. Nothing coordinates this -- no signalling, no
// rate limiter, no measurement. It falls out of the queues being bounded.
//
// The two counters distinguish *where* the pressure originated: blocks on the
// sink queue mean the sink is the bottleneck, and blocks on the input queues
// mean that pressure reached all the way upstream.
TEST(ParallelPipelineTest, PropagatesBackpressureFromASlowSinkToTheSource) {
    const std::vector<Record<Trip>> input = make_trips({"a", "b", "c", "d"}, 60);

    SlowSink sink(std::chrono::microseconds{200});
    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 4}, // tiny: pressure builds fast
        kZoneHash, make_operator_factory());
    pipeline.run(input, sink);

    EXPECT_EQ(sink.written(), input.size()) << "backpressure must throttle, never drop";

    const auto& metrics = pipeline.metrics();
    EXPECT_GT(metrics.sink_queue_push_blocks, 0U)
        << "subtasks never blocked on the sink queue -- the sink was not the bottleneck";
    EXPECT_GT(metrics.total_input_push_blocks(), 0U)
        << "pressure never reached the source; it was absorbed by buffering instead";
}

// Protects: a fast sink does not manufacture backpressure that is not there.
//
// The negative control. Without it, the test above could pass on a pipeline that
// blocks unconditionally, which would look like working backpressure and be a
// permanent throughput ceiling.
TEST(ParallelPipelineTest, DoesNotBlockOnTheSinkQueueWhenTheSinkKeepsUp) {
    const std::vector<Record<Trip>> input = make_trips({"a", "b", "c", "d"}, 5);

    ripple::CollectingSink<Output> sink;
    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 256}, kZoneHash,
        make_operator_factory());
    pipeline.run(input, sink);

    EXPECT_EQ(sink.records().size(), input.size());
    EXPECT_EQ(pipeline.metrics().sink_queue_push_blocks, 0U);
}

// ---------------------------------------------------------------------------
// Watermarks across the fan-in
// ---------------------------------------------------------------------------

// Protects: the sink's watermark is the minimum across its input channels, and
// arrives only once every subtask has reported.
//
// This is where WatermarkTracker (D-026, built in Stage 2 with nothing to
// consume it) finally does its job. Each subtask advances in event time
// independently; the sink may claim only as much progress as its slowest input
// has made. Taking the maximum instead would have the sink fire windows on the
// fastest subtask's clock and then receive perfectly on-time records from slower
// ones -- silent data loss proportional to how far the subtasks diverge.
TEST(ParallelPipelineTest, MergesWatermarksAcrossSubtasksByMinimum) {
    const std::vector<Record<Trip>> input = make_trips({"a", "b", "c", "d"}, 10);

    ripple::CollectingSink<Output> sink;
    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 16}, kZoneHash, make_operator_factory());
    pipeline.run(input, sink);

    // The source broadcasts one end-of-stream watermark to every subtask, so the
    // combined minimum advances exactly once -- when the last channel reports.
    ASSERT_EQ(sink.watermarks().size(), 1U)
        << "the combined watermark advanced before every channel had reported";
    EXPECT_EQ(sink.watermarks()[0].timestamp, ripple::kMaxTimestamp);
}

// Protects: shutdown completes with parallelism 1, which is the degenerate case
// where fan-in and fan-out collapse and off-by-one channel accounting hides.
TEST(ParallelPipelineTest, WorksWithParallelismOfOne) {
    const std::vector<Record<Trip>> input = make_trips({"a", "b"}, 10);

    ripple::CollectingSink<Output> sink;
    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 1, .queue_capacity = 4}, kZoneHash, make_operator_factory());
    pipeline.run(input, sink);

    EXPECT_EQ(sink.records().size(), input.size());
    EXPECT_EQ(sink.watermarks().size(), 1U);
}

// Protects: an empty input still shuts down rather than hanging on a channel
// count that never completes.
TEST(ParallelPipelineTest, ShutsDownCleanlyOnEmptyInput) {
    ripple::CollectingSink<Output> sink;
    auto pipeline = ripple::make_parallel_pipeline<Trip, Output>(
        ParallelConfig{.parallelism = 4, .queue_capacity = 4}, kZoneHash, make_operator_factory());
    pipeline.run({}, sink);

    EXPECT_TRUE(sink.records().empty());
    EXPECT_EQ(sink.watermarks().size(), 1U) << "end of stream must still be announced";
}

} // namespace
