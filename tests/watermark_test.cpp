#include <ripple/collector.hpp>
#include <ripple/operators/filter.hpp>
#include <ripple/operators/map.hpp>
#include <ripple/operators/watermark_generator.hpp>
#include <ripple/pipeline.hpp>
#include <ripple/record.hpp>
#include <ripple/sink.hpp>
#include <ripple/source.hpp>
#include <ripple/timestamp.hpp>
#include <ripple/watermark.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using ripple::Duration;
using ripple::Record;
using ripple::Timestamp;
using ripple::VectorSource;
using ripple::Watermark;
using ripple::WatermarkTracker;

Timestamp ms(std::int64_t millis) {
    return ripple::timestamp_from_millis(millis);
}

// ---------------------------------------------------------------------------
// WatermarkTracker -- the minimum-across-inputs rule
// ---------------------------------------------------------------------------

// Protects: the combined watermark is the MINIMUM across channels, not the
// maximum and not the most recent.
//
// Taking the maximum would be the natural-looking mistake, and it is
// catastrophic rather than merely wrong: the operator would claim to have seen
// everything up to the *fastest* channel's time, fire windows on that basis,
// and then receive perfectly on-time records from the slower channel that now
// look late. Silent data loss proportional to how far the channels diverge.
TEST(WatermarkTrackerTest, CombinesChannelsByMinimum) {
    WatermarkTracker tracker(2);

    // Channel 0 races ahead. The combined watermark cannot move, because
    // channel 1 has still promised nothing.
    EXPECT_FALSE(tracker.update(0, Watermark{ms(5'000)}).has_value());
    EXPECT_EQ(tracker.current().timestamp, ripple::kMinTimestamp);

    // Channel 1 reports a lower value. That value is now the constraint.
    const auto advanced = tracker.update(1, Watermark{ms(1'000)});
    ASSERT_TRUE(advanced.has_value());
    // Guarded by the ASSERT above, which clang-tidy's flow analysis does not model.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_EQ(advanced->timestamp, ms(1'000));
    EXPECT_EQ(tracker.current().timestamp, ms(1'000));
}

// Protects: progress is gated by the slowest channel, every time.
TEST(WatermarkTrackerTest, AdvancesOnlyWhenTheSlowestChannelAdvances) {
    WatermarkTracker tracker(2);
    (void)tracker.update(0, Watermark{ms(1'000)});
    (void)tracker.update(1, Watermark{ms(1'000)});
    ASSERT_EQ(tracker.current().timestamp, ms(1'000));

    // Pushing the fast channel far ahead changes nothing.
    EXPECT_FALSE(tracker.update(0, Watermark{ms(9'000)}).has_value());
    EXPECT_EQ(tracker.current().timestamp, ms(1'000));

    // Only movement on the laggard releases the combined watermark, and only
    // as far as the laggard itself has reached.
    const auto advanced = tracker.update(1, Watermark{ms(4'000)});
    ASSERT_TRUE(advanced.has_value());
    // Guarded by the ASSERT above, which clang-tidy's flow analysis does not model.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_EQ(advanced->timestamp, ms(4'000));
}

// Protects: a silent channel stalls the whole operator.
//
// Asserting the pathology deliberately, because it is the failure mode that
// looks healthiest in production: records keep flowing, nothing errors, and
// output simply stops. Knowing this is the expected behaviour of the minimum
// rule -- not a bug -- is what makes it diagnosable.
TEST(WatermarkTrackerTest, IdleChannelStallsProgress) {
    WatermarkTracker tracker(3);
    for (int i = 1; i <= 10; ++i) {
        (void)tracker.update(0, Watermark{ms(std::int64_t{i} * 1'000)});
        (void)tracker.update(1, Watermark{ms(std::int64_t{i} * 1'000)});
        // Channel 2 never reports.
    }
    EXPECT_EQ(tracker.current().timestamp, ripple::kMinTimestamp);
}

// Protects: per-channel monotonicity is enforced, not assumed.
//
// A regressing watermark would drag the minimum backwards, re-opening a window
// that had already fired and producing a second, contradictory result for a
// period that was supposed to be complete.
TEST(WatermarkTrackerTest, IgnoresRegressingChannelWatermarks) {
    WatermarkTracker tracker(1);
    (void)tracker.update(0, Watermark{ms(5'000)});

    EXPECT_FALSE(tracker.update(0, Watermark{ms(2'000)}).has_value());
    EXPECT_EQ(tracker.current().timestamp, ms(5'000));
}

// ---------------------------------------------------------------------------
// Bounded out-of-orderness generation
// ---------------------------------------------------------------------------

template<typename T>
class RecordingCollector final : public ripple::Collector<T> {
public:
    void collect(Record<T>&& record) override { records.push_back(std::move(record)); }

    void emit_watermark(Watermark watermark) override { watermarks.push_back(watermark); }

    std::vector<Record<T>> records;
    std::vector<Watermark> watermarks;
};

// Protects: the watermark lags the highest observed event time by exactly the
// configured bound.
TEST(BoundedOutOfOrdernessTest, LagsHighestEventTimeByTheConfiguredBound) {
    ripple::BoundedOutOfOrdernessWatermarks<int> generator{Duration{500}};
    RecordingCollector<int> out;

    generator.process(ripple::make_record(1, ms(10'000)), out);

    ASSERT_EQ(out.watermarks.size(), 1U);
    EXPECT_EQ(out.watermarks[0].timestamp, ms(9'500));
}

// Protects: out-of-order arrivals do not drag the watermark backwards.
//
// The generator tracks the *maximum* event time seen, not the most recent one.
// Tracking the most recent would make the watermark oscillate with every
// straggler, and a downstream window could fire, then un-fire, then fire again.
TEST(BoundedOutOfOrdernessTest, IsMonotonicUnderOutOfOrderInput) {
    ripple::BoundedOutOfOrdernessWatermarks<int> generator{Duration{1'000}};
    RecordingCollector<int> out;

    generator.process(ripple::make_record(1, ms(10'000)), out); // -> 9'000
    generator.process(ripple::make_record(2, ms(9'500)), out);  // straggler, no advance
    generator.process(ripple::make_record(3, ms(9'800)), out);  // straggler, no advance
    generator.process(ripple::make_record(4, ms(12'000)), out); // -> 11'000

    ASSERT_EQ(out.records.size(), 4U);
    ASSERT_EQ(out.watermarks.size(), 2U) << "a watermark was emitted without advancing";
    EXPECT_EQ(out.watermarks[0].timestamp, ms(9'000));
    EXPECT_EQ(out.watermarks[1].timestamp, ms(11'000));
}

// Protects: THE ordering invariant -- the record is emitted before any
// watermark derived from it.
//
// If the order were reversed, a record's own arrival would produce a watermark
// that renders that same record late. Downstream, a window could close and free
// its state one instant before receiving a record that belonged in it. The
// failure is silent: counts stay plausible and only boundary records land
// wrongly.
TEST(BoundedOutOfOrdernessTest, EmitsTheRecordBeforeTheWatermarkItProduces) {
    // Zero lag is the sharpest possible test: the generated watermark equals
    // the record's own event time exactly, so any ordering slip is observable.
    ripple::BoundedOutOfOrdernessWatermarks<int> generator{Duration{0}};

    class OrderingCollector final : public ripple::Collector<int> {
    public:
        // This collector inspects the timestamp and drops the payload, which
        // is a legitimate thing for a consumer to do.
        // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
        void collect(Record<int>&& record) override {
            log.emplace_back(true, ripple::millis_since_epoch(record.event_time));
        }

        void emit_watermark(Watermark watermark) override {
            log.emplace_back(false, ripple::millis_since_epoch(watermark.timestamp));
        }

        std::vector<std::pair<bool, std::int64_t>> log; // (is_record, millis)
    };

    OrderingCollector out;
    generator.process(ripple::make_record(1, ms(1'000)), out);

    ASSERT_EQ(out.log.size(), 2U);
    EXPECT_TRUE(out.log[0].first) << "watermark was emitted before its own record";
    EXPECT_FALSE(out.log[1].first);
    EXPECT_EQ(out.log[0].second, 1'000);
    EXPECT_EQ(out.log[1].second, 1'000);
}

// ---------------------------------------------------------------------------
// Propagation through a pipeline
// ---------------------------------------------------------------------------

/// Records the interleaving of records and watermarks, which is the property
/// that matters. Keeping them in separate lists would hide exactly the bug
/// these tests exist to catch.
template<typename T>
class OrderRecordingSink final : public ripple::Sink<T> {
public:
    struct Event {
        bool is_record;
        std::int64_t millis;
    };

    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    void write(Record<T>&& record) override {
        events.push_back({true, ripple::millis_since_epoch(record.event_time)});
    }

    void on_watermark(Watermark watermark) override {
        events.push_back({false, ripple::millis_since_epoch(watermark.timestamp)});
    }

    [[nodiscard]] std::string_view name() const noexcept override { return "order-recording-sink"; }

    std::vector<Event> events;
};

std::vector<Record<int>> records_at(const std::vector<std::int64_t>& millis) {
    std::vector<Record<int>> records;
    records.reserve(millis.size());
    int value = 0;
    for (const std::int64_t at : millis) {
        records.push_back(ripple::make_record(value++, ms(at)));
    }
    return records;
}

// Protects: operators with no notion of time still forward watermarks.
//
// Map and filter never mention watermarks -- they inherit the default
// `on_watermark`. If that default were missing or dropped the watermark, event
// time would stop advancing at the first map in any pipeline and every
// downstream window would hang forever holding state.
TEST(WatermarkPropagationTest, PassesThroughTimeAgnosticOperators) {
    auto sink = std::make_unique<OrderRecordingSink<int>>();
    auto* sink_ptr = sink.get();

    auto pipeline =
        ripple::from(std::make_unique<VectorSource<int>>(records_at({1'000, 2'000, 3'000})))
            .via(ripple::make_bounded_out_of_orderness_watermarks<int>(Duration{500}))
            .via(ripple::make_map<int>([](int value) { return value; }))
            .via(ripple::make_filter<int>([](int) { return true; }))
            .to(std::move(sink));
    pipeline.run();

    const bool saw_watermark = std::any_of(sink_ptr->events.begin(), sink_ptr->events.end(),
                                           [](const auto& event) { return !event.is_record; });
    EXPECT_TRUE(saw_watermark) << "watermarks did not survive map and filter";
}

// Protects: the invariant that gives watermarks their meaning.
//
// After a watermark of T is delivered, no record with event time <= T may
// follow it on the same channel. This is what lets a downstream operator treat
// "watermark T arrived" as "everything up to T is already processed" without
// any coordination. It is checked over the whole event log rather than at one
// point, because a single misordered pair anywhere breaks the guarantee.
TEST(WatermarkPropagationTest, NoRecordArrivesAfterAWatermarkCoveringIt) {
    auto sink = std::make_unique<OrderRecordingSink<int>>();
    auto* sink_ptr = sink.get();

    // Deliberately jumbled input, within the 1s bound.
    auto pipeline = ripple::from(std::make_unique<VectorSource<int>>(
                                     records_at({5'000, 4'500, 6'000, 5'200, 7'000, 6'800})))
                        .via(ripple::make_bounded_out_of_orderness_watermarks<int>(Duration{1'000}))
                        .to(std::move(sink));
    pipeline.run();

    std::int64_t highest_watermark = ripple::millis_since_epoch(ripple::kMinTimestamp);
    for (const auto& event : sink_ptr->events) {
        if (event.is_record) {
            EXPECT_GT(event.millis, highest_watermark)
                << "record at " << event.millis << "ms arrived after watermark "
                << highest_watermark << "ms, which claimed to cover it";
        } else {
            EXPECT_GE(event.millis, highest_watermark) << "watermark went backwards";
            highest_watermark = event.millis;
        }
    }
}

// Protects: the end of the input produces a maximal watermark.
//
// Without it, a finite job silently drops whatever windows were still open when
// the input ran out -- the last few minutes of every run simply missing, with
// no error and plausible-looking totals.
TEST(WatermarkPropagationTest, EmitsMaximalWatermarkAtEndOfStream) {
    auto sink = std::make_unique<OrderRecordingSink<int>>();
    auto* sink_ptr = sink.get();

    auto pipeline = ripple::from(std::make_unique<VectorSource<int>>(records_at({1'000})))
                        .via(ripple::make_bounded_out_of_orderness_watermarks<int>(Duration{100}))
                        .to(std::move(sink));
    pipeline.run();

    ASSERT_FALSE(sink_ptr->events.empty());
    const auto& last = sink_ptr->events.back();
    EXPECT_FALSE(last.is_record);
    EXPECT_EQ(last.millis, ripple::millis_since_epoch(ripple::kMaxTimestamp));
}

// Protects: an empty stream still terminates event time.
//
// A source with no records must not leave downstream operators waiting forever;
// the end-of-stream watermark has to be emitted regardless.
TEST(WatermarkPropagationTest, EmitsEndOfStreamWatermarkForEmptyInput) {
    auto sink = std::make_unique<OrderRecordingSink<int>>();
    auto* sink_ptr = sink.get();

    auto pipeline = ripple::from(std::make_unique<VectorSource<int>>(std::vector<Record<int>>{}))
                        .via(ripple::make_bounded_out_of_orderness_watermarks<int>(Duration{100}))
                        .to(std::move(sink));
    pipeline.run();

    ASSERT_EQ(sink_ptr->events.size(), 1U);
    EXPECT_FALSE(sink_ptr->events[0].is_record);
    EXPECT_EQ(sink_ptr->events[0].millis, ripple::millis_since_epoch(ripple::kMaxTimestamp));
}

} // namespace
