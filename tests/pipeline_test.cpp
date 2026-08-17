#include <ripple/operators/filter.hpp>
#include <ripple/operators/map.hpp>
#include <ripple/pipeline.hpp>
#include <ripple/record.hpp>
#include <ripple/sink.hpp>
#include <ripple/source.hpp>
#include <ripple/timestamp.hpp>

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace {

using ripple::CollectingSink;
using ripple::Record;
using ripple::VectorSource;

std::vector<Record<int>> make_input(const std::vector<int>& values) {
    std::vector<Record<int>> records;
    records.reserve(values.size());
    std::int64_t millis = 0;
    for (const int value : values) {
        records.push_back(ripple::make_record(value, ripple::timestamp_from_millis(millis)));
        millis += 100;
    }
    return records;
}

// Protects: the end-to-end contract. Records traverse source -> map -> filter
// -> sink, transformations apply in order, and the filter's drops do not
// disturb the records around them.
TEST(PipelineTest, RunsSourceThroughOperatorsToSink) {
    auto source = std::make_unique<VectorSource<int>>(make_input({1, 2, 3, 4, 5}));
    auto sink = std::make_unique<CollectingSink<int>>();
    auto* sink_ptr = sink.get();

    auto pipeline = ripple::from(std::move(source))
                        .via(ripple::make_map<int>([](int value) { return value * 10; }))
                        .via(ripple::make_filter<int>([](int value) { return value > 20; }))
                        .to(std::move(sink));

    pipeline.run();

    ASSERT_EQ(sink_ptr->records().size(), 3U);
    EXPECT_EQ(sink_ptr->records()[0].value, 30);
    EXPECT_EQ(sink_ptr->records()[1].value, 40);
    EXPECT_EQ(sink_ptr->records()[2].value, 50);
}

// Protects: ordering within a single-threaded pipeline.
//
// This is the property Stage 6 will have to preserve *per key* once operators
// run on separate threads, and it is the baseline the parallel implementation
// gets compared against.
TEST(PipelineTest, PreservesRecordOrder) {
    auto source = std::make_unique<VectorSource<int>>(make_input({5, 3, 9, 1}));
    auto sink = std::make_unique<CollectingSink<int>>();
    auto* sink_ptr = sink.get();

    auto pipeline = ripple::from(std::move(source)).to(std::move(sink));
    pipeline.run();

    ASSERT_EQ(sink_ptr->records().size(), 4U);
    EXPECT_EQ(sink_ptr->records()[0].value, 5);
    EXPECT_EQ(sink_ptr->records()[1].value, 3);
    EXPECT_EQ(sink_ptr->records()[2].value, 9);
    EXPECT_EQ(sink_ptr->records()[3].value, 1);
}

// Protects: event time survives the full traversal untouched.
TEST(PipelineTest, PropagatesEventTimeEndToEnd) {
    auto source = std::make_unique<VectorSource<int>>(make_input({1, 2}));
    auto sink = std::make_unique<CollectingSink<std::string>>();
    auto* sink_ptr = sink.get();

    auto pipeline = ripple::from(std::move(source))
                        .via(ripple::make_map<int>([](int value) { return std::to_string(value); }))
                        .to(std::move(sink));
    pipeline.run();

    ASSERT_EQ(sink_ptr->records().size(), 2U);
    EXPECT_EQ(ripple::millis_since_epoch(sink_ptr->records()[0].event_time), 0);
    EXPECT_EQ(ripple::millis_since_epoch(sink_ptr->records()[1].event_time), 100);
}

// Protects: an empty stream is a valid stream and must not crash or emit.
TEST(PipelineTest, HandlesEmptySource) {
    auto source = std::make_unique<VectorSource<int>>(std::vector<Record<int>>{});
    auto sink = std::make_unique<CollectingSink<int>>();
    auto* sink_ptr = sink.get();

    auto pipeline = ripple::from(std::move(source))
                        .via(ripple::make_map<int>([](int value) { return value; }))
                        .to(std::move(sink));
    pipeline.run();

    EXPECT_TRUE(sink_ptr->records().empty());
}

// Protects: a filter that rejects everything yields an empty result rather than
// leaving the pipeline in a broken state.
TEST(PipelineTest, HandlesFilterRejectingEverything) {
    auto source = std::make_unique<VectorSource<int>>(make_input({1, 2, 3}));
    auto sink = std::make_unique<CollectingSink<int>>();
    auto* sink_ptr = sink.get();

    auto pipeline = ripple::from(std::move(source))
                        .via(ripple::make_filter<int>([](int) { return false; }))
                        .to(std::move(sink));
    pipeline.run();

    EXPECT_TRUE(sink_ptr->records().empty());
}

// Protects: the pipeline owns every node, including the source and the sink.
//
// Stage 7 walks this collection to snapshot each operator. If a source or sink
// were held outside it, that stage would silently skip them.
TEST(PipelineTest, OwnsSourceOperatorsAndSink) {
    auto pipeline = ripple::from(std::make_unique<VectorSource<int>>(make_input({1})))
                        .via(ripple::make_map<int>([](int value) { return value; }))
                        .via(ripple::make_filter<int>([](int) { return true; }))
                        .to(std::make_unique<CollectingSink<int>>());

    EXPECT_EQ(pipeline.operator_count(), 4U); // source + map + filter + sink
}

// ---------------------------------------------------------------------------
// Ownership: the payload must never be copied on the single-consumer path.
// ---------------------------------------------------------------------------

struct CopyCounter {
    int copies = 0;
};

/// A payload that reports whenever it is copied.
class Tracked {
public:
    Tracked() = default;

    explicit Tracked(CopyCounter* counter, int value) noexcept : counter_(counter), value_(value) {}

    Tracked(const Tracked& other) : counter_(other.counter_), value_(other.value_) {
        if (counter_ != nullptr) {
            ++counter_->copies;
        }
    }

    Tracked& operator=(const Tracked& other) {
        if (this != &other) {
            counter_ = other.counter_;
            value_ = other.value_;
            if (counter_ != nullptr) {
                ++counter_->copies;
            }
        }
        return *this;
    }

    // Marked noexcept deliberately, and this is load-bearing rather than
    // decorative: std::vector must provide the strong exception guarantee
    // during reallocation, so if the move constructor can throw, vector
    // *copies* every element instead of moving it. An unmarked move
    // constructor would silently turn every sink push_back into a full copy
    // and this test would fail -- which is exactly what we want it to catch.
    Tracked(Tracked&& other) noexcept : counter_(other.counter_), value_(other.value_) {
        other.counter_ = nullptr;
    }

    Tracked& operator=(Tracked&& other) noexcept {
        if (this != &other) {
            counter_ = other.counter_;
            value_ = other.value_;
            other.counter_ = nullptr;
        }
        return *this;
    }

    ~Tracked() = default;

    [[nodiscard]] int value() const noexcept { return value_; }

private:
    CopyCounter* counter_ = nullptr;
    int value_ = 0;
};

// Protects: the core ownership decision of Stage 1.
//
// We chose value semantics with moves over shared_ptr and over pooling. That
// choice is only worth anything if records genuinely move rather than copy
// through the DAG. Nothing about the source reads differently when it is
// copying -- the results are identical and only throughput suffers -- so this
// is asserted explicitly.
TEST(PipelineOwnershipTest, DoesNotCopyPayloadsOnTheSingleConsumerPath) {
    CopyCounter counter;

    std::vector<Record<Tracked>> input;
    input.reserve(3);
    for (int i = 0; i < 3; ++i) {
        input.push_back(Record<Tracked>{Tracked{&counter, i}, ripple::timestamp_from_millis(i)});
    }
    ASSERT_EQ(counter.copies, 0) << "setup itself copied";

    auto sink = std::make_unique<CollectingSink<Tracked>>();
    auto* sink_ptr = sink.get();

    auto pipeline =
        ripple::from(std::make_unique<VectorSource<Tracked>>(std::move(input)))
            .via(ripple::make_filter<Tracked>([](const Tracked& t) { return t.value() >= 0; }))
            .to(std::move(sink));
    pipeline.run();

    EXPECT_EQ(sink_ptr->records().size(), 3U);
    EXPECT_EQ(counter.copies, 0);
}

} // namespace
