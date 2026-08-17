#include <ripple/aggregators.hpp>
#include <ripple/collector.hpp>
#include <ripple/operators/watermark_generator.hpp>
#include <ripple/operators/window.hpp>
#include <ripple/pipeline.hpp>
#include <ripple/record.hpp>
#include <ripple/sink.hpp>
#include <ripple/source.hpp>
#include <ripple/timestamp.hpp>
#include <ripple/watermark.hpp>
#include <ripple/window.hpp>
#include <ripple/window_assigners.hpp>

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace {

using ripple::CountAggregator;
using ripple::Duration;
using ripple::Record;
using ripple::SessionWindows;
using ripple::SlidingWindows;
using ripple::SumAggregator;
using ripple::Timestamp;
using ripple::TimeWindow;
using ripple::TumblingWindows;
using ripple::Watermark;
using ripple::WindowResult;

Timestamp ms(std::int64_t millis) {
    return ripple::timestamp_from_millis(millis);
}

std::vector<TimeWindow> assign(const auto& assigner, std::int64_t millis) {
    std::vector<TimeWindow> windows;
    assigner.assign(ms(millis), windows);
    return windows;
}

// ---------------------------------------------------------------------------
// TimeWindow
// ---------------------------------------------------------------------------

// Protects: windows are half-open.
//
// With inclusive ends a record landing exactly on a boundary belongs to two
// adjacent tumbling windows and is counted twice. The error appears only at
// boundaries, so it survives any test that does not deliberately probe them.
TEST(TimeWindowTest, IsHalfOpen) {
    const TimeWindow window{ms(1'000), ms(2'000)};

    EXPECT_TRUE(window.contains(ms(1'000)));
    EXPECT_TRUE(window.contains(ms(1'999)));
    EXPECT_FALSE(window.contains(ms(2'000)));
    EXPECT_FALSE(window.contains(ms(999)));
}

// Protects: adjacency is not overlap.
//
// Load-bearing for sessions specifically: [1000,2000) meeting [2000,3000) is
// exactly the case where the inactivity gap was met precisely and the session
// must end rather than merge.
TEST(TimeWindowTest, AdjacentWindowsDoNotOverlap) {
    const TimeWindow first{ms(1'000), ms(2'000)};
    const TimeWindow adjacent{ms(2'000), ms(3'000)};
    const TimeWindow overlapping{ms(1'999), ms(3'000)};

    EXPECT_FALSE(first.overlaps(adjacent));
    EXPECT_TRUE(first.overlaps(overlapping));
}

TEST(TimeWindowTest, MaxTimestampIsTheLastInstantCovered) {
    const TimeWindow window{ms(1'000), ms(2'000)};
    EXPECT_EQ(window.max_timestamp(), ms(1'999));
    EXPECT_TRUE(window.contains(window.max_timestamp()));
}

// ---------------------------------------------------------------------------
// Assigners
// ---------------------------------------------------------------------------

TEST(TumblingWindowsTest, AssignsExactlyOneWindow) {
    const TumblingWindows assigner{Duration{5'000}};

    const auto windows = assign(assigner, 7'300);
    ASSERT_EQ(windows.size(), 1U);
    EXPECT_EQ(windows[0].start, ms(5'000));
    EXPECT_EQ(windows[0].end, ms(10'000));
}

TEST(TumblingWindowsTest, AlignsWindowsToTheEpoch) {
    const TumblingWindows assigner{Duration{5'000}};
    EXPECT_EQ(assign(assigner, 0)[0].start, ms(0));
    EXPECT_EQ(assign(assigner, 4'999)[0].start, ms(0));
    EXPECT_EQ(assign(assigner, 5'000)[0].start, ms(5'000));
}

// Protects: window assignment uses floor division, not C++'s truncating
// division.
//
// `-1 / 5000` is 0 in C++, not -1, so a truncating implementation puts every
// pre-epoch timestamp into the window starting at zero. Not hypothetical: the
// demo dataset contains meter timestamps stamped in 1900.
TEST(TumblingWindowsTest, HandlesPreEpochTimestamps) {
    const TumblingWindows assigner{Duration{5'000}};

    const auto windows = assign(assigner, -1);
    ASSERT_EQ(windows.size(), 1U);
    EXPECT_EQ(windows[0].start, ms(-5'000));
    EXPECT_EQ(windows[0].end, ms(0));

    EXPECT_EQ(assign(assigner, -5'000)[0].start, ms(-5'000));
    EXPECT_EQ(assign(assigner, -5'001)[0].start, ms(-10'000));
}

// Protects: one record lands in size/slide windows simultaneously.
//
// This multiple *is* the memory cost of sliding windows, and it is the number
// that surprises people: a one-hour window sliding every second holds every
// record in 3,600 accumulators at once.
TEST(SlidingWindowsTest, AssignsOneRecordToMultipleWindows) {
    const SlidingWindows assigner{Duration{5'000}, Duration{1'000}};

    const auto windows = assign(assigner, 7'000);
    ASSERT_EQ(windows.size(), 5U);

    for (const TimeWindow& window : windows) {
        EXPECT_TRUE(window.contains(ms(7'000)));
        EXPECT_EQ(window.end - window.start, Duration{5'000});
    }
}

TEST(SlidingWindowsTest, ProducesContiguousStartsSpacedBySlide) {
    const SlidingWindows assigner{Duration{4'000}, Duration{2'000}};

    const auto windows = assign(assigner, 5'000);
    ASSERT_EQ(windows.size(), 2U);
    // Walked backwards from the latest containing window.
    EXPECT_EQ(windows[0].start, ms(4'000));
    EXPECT_EQ(windows[1].start, ms(2'000));
}

TEST(SessionWindowsTest, AssignsAProvisionalWindowOfOneGap) {
    const SessionWindows assigner{Duration{30'000}};

    const auto windows = assign(assigner, 1'000);
    ASSERT_EQ(windows.size(), 1U);
    EXPECT_EQ(windows[0].start, ms(1'000));
    EXPECT_EQ(windows[0].end, ms(31'000));
}

// ---------------------------------------------------------------------------
// Window operator
// ---------------------------------------------------------------------------

template<typename T>
class ResultCollector final : public ripple::Collector<WindowResult<T>> {
public:
    void collect(Record<WindowResult<T>>&& record) override {
        results.push_back(std::move(record));
    }

    void emit_watermark(Watermark watermark) override { watermarks.push_back(watermark); }

    std::vector<Record<WindowResult<T>>> results;
    std::vector<Watermark> watermarks;
};

// Protects: a window does not fire until the watermark proves it complete.
//
// Firing early is the whole failure mode event time exists to prevent: the
// result would depend on how much of the window happened to have arrived, which
// is a function of network timing rather than of the data.
TEST(WindowOperatorTest, DoesNotFireBeforeTheWatermarkPassesTheWindowEnd) {
    ripple::WindowOperator<int, TumblingWindows, SumAggregator<int>> window{
        TumblingWindows{Duration{1'000}}, SumAggregator<int>{}};
    ResultCollector<int> out;

    window.process(ripple::make_record(5, ms(100)), out);
    window.process(ripple::make_record(7, ms(200)), out);

    // Watermark inside the window: not yet complete.
    window.on_watermark(Watermark{ms(500)}, out);
    EXPECT_TRUE(out.results.empty());

    // Watermark at the window's exclusive end: now complete.
    window.on_watermark(Watermark{ms(1'000)}, out);
    ASSERT_EQ(out.results.size(), 1U);
    EXPECT_EQ(out.results[0].value.value, 12);
    EXPECT_EQ(out.results[0].value.window.start, ms(0));
    EXPECT_EQ(out.results[0].value.window.end, ms(1'000));
}

// Protects: the result record is stamped inside its own window.
//
// Stamping it at `end` instead would place the result in the *next* window if
// it flowed into a second windowing operator -- an off-by-one that shifts every
// downstream aggregate by exactly one window.
TEST(WindowOperatorTest, StampsResultAtTheWindowsLastInstant) {
    ripple::WindowOperator<int, TumblingWindows, SumAggregator<int>> window{
        TumblingWindows{Duration{1'000}}, SumAggregator<int>{}};
    ResultCollector<int> out;

    window.process(ripple::make_record(1, ms(100)), out);
    window.on_watermark(Watermark{ms(1'000)}, out);

    ASSERT_EQ(out.results.size(), 1U);
    EXPECT_EQ(out.results[0].event_time, ms(999));
}

// Protects: firing releases memory.
//
// This is the mechanism that lets a windowed job run indefinitely. If state
// survived firing, every window ever opened would accumulate and the process
// would grow without bound -- correct output, right up until the OOM kill.
TEST(WindowOperatorTest, PurgesWindowStateAfterFiring) {
    ripple::WindowOperator<int, TumblingWindows, SumAggregator<int>> window{
        TumblingWindows{Duration{1'000}}, SumAggregator<int>{}};
    ResultCollector<int> out;

    for (std::int64_t i = 0; i < 5; ++i) {
        window.process(ripple::make_record(1, ms(i * 1'000 + 100)), out);
    }
    EXPECT_EQ(window.open_window_count(), 5U);

    window.on_watermark(Watermark{ms(5'000)}, out);
    EXPECT_EQ(window.open_window_count(), 0U);
    EXPECT_EQ(out.results.size(), 5U);
}

// Protects: a fired window is not re-emitted on every subsequent watermark.
TEST(WindowOperatorTest, EmitsEachWindowOnceWhenNothingChanges) {
    ripple::WindowOperator<int, TumblingWindows, SumAggregator<int>> window{
        TumblingWindows{Duration{1'000}}, SumAggregator<int>{}, Duration{10'000}};
    ResultCollector<int> out;

    window.process(ripple::make_record(3, ms(100)), out);
    window.on_watermark(Watermark{ms(1'000)}, out);
    window.on_watermark(Watermark{ms(2'000)}, out);
    window.on_watermark(Watermark{ms(3'000)}, out);

    EXPECT_EQ(out.results.size(), 1U);
}

// Protects: the window operator forwards watermarks.
//
// An override of on_watermark that fires windows but forgets to forward is a
// plausible bug with a vicious signature: event time freezes for everything
// downstream, so downstream windows never fire and never free state, and the
// job produces no output while appearing entirely healthy.
TEST(WindowOperatorTest, ForwardsWatermarksDownstream) {
    ripple::WindowOperator<int, TumblingWindows, SumAggregator<int>> window{
        TumblingWindows{Duration{1'000}}, SumAggregator<int>{}};
    ResultCollector<int> out;

    window.on_watermark(Watermark{ms(1'000)}, out);

    ASSERT_EQ(out.watermarks.size(), 1U);
    EXPECT_EQ(out.watermarks[0].timestamp, ms(1'000));
}

TEST(WindowOperatorTest, SlidingWindowsCountARecordInEveryWindowItBelongsTo) {
    ripple::WindowOperator<int, SlidingWindows, CountAggregator<int>> window{
        SlidingWindows{Duration{4'000}, Duration{2'000}}, CountAggregator<int>{}};
    ResultCollector<std::size_t> out; // CountAggregator's output type

    window.process(ripple::make_record(1, ms(5'000)), out);
    EXPECT_EQ(window.open_window_count(), 2U);

    window.on_watermark(Watermark{ms(100'000)}, out);
    ASSERT_EQ(out.results.size(), 2U);
    EXPECT_EQ(out.results[0].value.value, 1U);
    EXPECT_EQ(out.results[1].value.value, 1U);
}

// ---------------------------------------------------------------------------
// Session merging
// ---------------------------------------------------------------------------

// Protects: a record bridging two sessions merges them into one.
//
// This is the behaviour that makes sessions structurally different from the
// other assigners. Tumbling and sliding boundaries are fixed by arithmetic on
// the timestamp alone and no record can move them. A session's boundaries are
// determined by the data, so a record arriving between two sessions proves
// there was never a gap between them -- and the two were never really two.
TEST(SessionWindowOperatorTest, MergesSessionsBridgedByALaterRecord) {
    ripple::WindowOperator<int, SessionWindows, SumAggregator<int>> window{
        SessionWindows{Duration{1'000}}, SumAggregator<int>{}};
    ResultCollector<int> out;

    // Two clearly separate bursts of activity.
    window.process(ripple::make_record(1, ms(0)), out);     // -> [0, 1000)
    window.process(ripple::make_record(2, ms(5'000)), out); // -> [5000, 6000)
    ASSERT_EQ(window.open_window_count(), 2U);

    // A record in the middle bridges the gap: 2500 extends to 3500, still not
    // touching either. Two records are needed to close the whole span.
    window.process(ripple::make_record(4, ms(900)), out);   // [900,1900) merges with [0,1000)
    window.process(ripple::make_record(8, ms(1'800)), out); // extends the span further
    window.process(ripple::make_record(16, ms(2'700)), out);
    window.process(ripple::make_record(32, ms(3'600)), out);
    window.process(ripple::make_record(64, ms(4'500)), out);

    EXPECT_EQ(window.open_window_count(), 1U) << "sessions were not merged into one";

    window.on_watermark(Watermark{ms(100'000)}, out);
    ASSERT_EQ(out.results.size(), 1U);
    EXPECT_EQ(out.results[0].value.value, 127);
    EXPECT_EQ(out.results[0].value.window.start, ms(0));
    EXPECT_EQ(out.results[0].value.window.end, ms(6'000));
}

// Protects: a gap of exactly the session gap does NOT merge.
//
// The boundary case. [0,1000) and [1000,2000) are adjacent but not overlapping,
// which is the definition of "the inactivity gap was met" -- the session ends.
// Treating adjacency as overlap would silently glue together every session in
// the stream.
TEST(SessionWindowOperatorTest, DoesNotMergeSessionsSeparatedByExactlyTheGap) {
    ripple::WindowOperator<int, SessionWindows, SumAggregator<int>> window{
        SessionWindows{Duration{1'000}}, SumAggregator<int>{}};
    ResultCollector<int> out;

    window.process(ripple::make_record(1, ms(0)), out);
    window.process(ripple::make_record(2, ms(1'000)), out);

    EXPECT_EQ(window.open_window_count(), 2U);
}

// ---------------------------------------------------------------------------
// Late data
// ---------------------------------------------------------------------------

// Protects: records that missed every window go to the side output rather than
// disappearing.
TEST(LateDataTest, RoutesTooLateRecordsToTheSideOutput) {
    ripple::WindowOperator<int, TumblingWindows, SumAggregator<int>> window{
        TumblingWindows{Duration{1'000}}, SumAggregator<int>{}};
    ripple::CollectingLateRecordHandler<int> late;
    window.set_late_record_handler(&late);
    ResultCollector<int> out;

    window.process(ripple::make_record(1, ms(100)), out);
    window.on_watermark(Watermark{ms(5'000)}, out); // [0,1000) fires and is purged

    window.process(ripple::make_record(99, ms(200)), out); // belongs to a purged window

    ASSERT_EQ(late.records().size(), 1U);
    EXPECT_EQ(late.records()[0].value, 99);
    EXPECT_EQ(window.late_record_count(), 1U);
}

// Protects: late records are counted even with no handler attached.
//
// Observability first: the count alone is usually enough to reveal that the
// watermark bound is set too tight.
TEST(LateDataTest, CountsLateRecordsWithoutAHandler) {
    ripple::WindowOperator<int, TumblingWindows, SumAggregator<int>> window{
        TumblingWindows{Duration{1'000}}, SumAggregator<int>{}};
    ResultCollector<int> out;

    window.on_watermark(Watermark{ms(5'000)}, out);
    window.process(ripple::make_record(1, ms(100)), out);

    EXPECT_EQ(window.late_record_count(), 1U);
    EXPECT_TRUE(out.results.empty());
}

// Protects: allowed lateness keeps a window alive past its fire, and a late
// arrival produces an updated result.
//
// The consequence for downstream, worth stating explicitly: with allowed
// lateness the same window can be emitted more than once. A sink must therefore
// treat window results as upserts keyed by the window, not as appends -- which
// is the same idempotence requirement Stage 8 will need for exactly-once.
TEST(LateDataTest, AllowedLatenessAdmitsLateRecordsAndReFiresTheWindow) {
    ripple::WindowOperator<int, TumblingWindows, SumAggregator<int>> window{
        TumblingWindows{Duration{1'000}}, SumAggregator<int>{}, Duration{2'000}};
    ripple::CollectingLateRecordHandler<int> late;
    window.set_late_record_handler(&late);
    ResultCollector<int> out;

    window.process(ripple::make_record(10, ms(100)), out);
    window.on_watermark(Watermark{ms(1'000)}, out);
    ASSERT_EQ(out.results.size(), 1U);
    EXPECT_EQ(out.results[0].value.value, 10);

    // Late, but inside the allowed lateness: admitted, and the window re-fires
    // with a corrected total.
    window.process(ripple::make_record(5, ms(200)), out);
    window.on_watermark(Watermark{ms(1'500)}, out);
    ASSERT_EQ(out.results.size(), 2U);
    EXPECT_EQ(out.results[1].value.value, 15);
    EXPECT_TRUE(late.records().empty());

    // Past the allowed lateness: the window is gone and the record is late.
    window.on_watermark(Watermark{ms(4'000)}, out);
    window.process(ripple::make_record(7, ms(300)), out);
    EXPECT_EQ(late.records().size(), 1U);
    EXPECT_EQ(out.results.size(), 2U);
}

// ---------------------------------------------------------------------------
// End to end
// ---------------------------------------------------------------------------

// Protects: windows work inside a real pipeline, driven by generated
// watermarks, including the end-of-stream flush.
//
// In particular the final window only fires because of the maximal watermark
// emitted when the source is exhausted (D-025). Without it this test would see
// three results instead of four, with no error raised.
TEST(WindowPipelineTest, AggregatesTumblingWindowsEndToEnd) {
    std::vector<Record<int>> input;
    for (std::int64_t i = 0; i < 10; ++i) {
        input.push_back(ripple::make_record(1, ms(i * 500))); // one every 500ms
    }

    auto sink = std::make_unique<ripple::CollectingSink<WindowResult<std::size_t>>>();
    auto* sink_ptr = sink.get();

    auto pipeline =
        ripple::from(std::make_unique<ripple::VectorSource<int>>(std::move(input)))
            .via(ripple::make_bounded_out_of_orderness_watermarks<int>(Duration{100}))
            .via(ripple::make_window<int>(TumblingWindows{Duration{1'000}}, CountAggregator<int>{}))
            .to(std::move(sink));
    pipeline.run();

    // 10 records spanning [0, 4500] at 500ms intervals -> 5 windows of 2 each.
    ASSERT_EQ(sink_ptr->records().size(), 5U);
    for (const auto& result : sink_ptr->records()) {
        EXPECT_EQ(result.value.value, 2U);
    }
    EXPECT_EQ(sink_ptr->records()[0].value.window.start, ms(0));
    EXPECT_EQ(sink_ptr->records()[4].value.window.start, ms(4'000));
}

} // namespace
