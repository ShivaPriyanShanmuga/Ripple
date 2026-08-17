#include <ripple/collector.hpp>
#include <ripple/operators/filter.hpp>
#include <ripple/operators/map.hpp>
#include <ripple/record.hpp>
#include <ripple/timestamp.hpp>
#include <ripple/watermark.hpp>

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

namespace {

using ripple::Record;

/// Captures whatever an operator emits, so operators can be exercised in
/// isolation without building a pipeline around them.
template<typename T>
class TestCollector final : public ripple::Collector<T> {
public:
    void collect(Record<T>&& record) override { received.push_back(std::move(record)); }

    void emit_watermark(ripple::Watermark watermark) override { watermarks.push_back(watermark); }

    std::vector<Record<T>> received;
    std::vector<ripple::Watermark> watermarks;
};

// Protects: map applies the function to the payload.
TEST(MapOperatorTest, TransformsPayload) {
    auto op = ripple::make_map<int>([](int value) { return value * 2; });
    TestCollector<int> out;

    op->process(ripple::make_record(21, ripple::timestamp_from_millis(100)), out);

    ASSERT_EQ(out.received.size(), 1U);
    EXPECT_EQ(out.received[0].value, 42);
}

// Protects: map does NOT touch the event timestamp.
//
// This matters far more than it looks. From Stage 2 onward, the event timestamp
// determines which window a record lands in. An operator that reset it to "now"
// -- an easy accident when constructing the output record -- would convert the
// whole engine from event-time to processing-time semantics silently, and every
// windowed result would become dependent on execution speed.
TEST(MapOperatorTest, PreservesEventTime) {
    auto op = ripple::make_map<int>([](int value) { return std::to_string(value); });
    TestCollector<std::string> out;

    op->process(ripple::make_record(7, ripple::timestamp_from_millis(1'234)), out);

    ASSERT_EQ(out.received.size(), 1U);
    EXPECT_EQ(out.received[0].value, "7");
    EXPECT_EQ(ripple::millis_since_epoch(out.received[0].event_time), 1'234);
}

// Protects: map can change the payload type, which is the whole reason the
// operator interface is templated on two types rather than one.
TEST(MapOperatorTest, ChangesPayloadType) {
    auto op = ripple::make_map<std::string>(
        [](const std::string& text) { return static_cast<int>(text.size()); });
    TestCollector<int> out;

    op->process(ripple::make_record(std::string{"brooklyn"}, ripple::timestamp_from_millis(1)),
                out);

    ASSERT_EQ(out.received.size(), 1U);
    EXPECT_EQ(out.received[0].value, 8);
}

// Protects: a passing record is forwarded intact, timestamp included.
TEST(FilterOperatorTest, ForwardsMatchingRecords) {
    auto op = ripple::make_filter<int>([](int value) { return value > 10; });
    TestCollector<int> out;

    op->process(ripple::make_record(15, ripple::timestamp_from_millis(500)), out);

    ASSERT_EQ(out.received.size(), 1U);
    EXPECT_EQ(out.received[0].value, 15);
    EXPECT_EQ(ripple::millis_since_epoch(out.received[0].event_time), 500);
}

// Protects: a failing record produces no output at all.
//
// Specifically that it emits *nothing* rather than a default-constructed or
// moved-from record. An operator is allowed to call the collector zero times;
// this asserts that filter actually uses that freedom.
TEST(FilterOperatorTest, DropsNonMatchingRecords) {
    auto op = ripple::make_filter<int>([](int value) { return value > 10; });
    TestCollector<int> out;

    op->process(ripple::make_record(3, ripple::timestamp_from_millis(500)), out);

    EXPECT_TRUE(out.received.empty());
}

// Protects: the predicate sees the payload before it is moved.
//
// The failure this guards against: if `process` moved the payload into the
// predicate, a record that passes would be forwarded with an emptied payload.
// That is silent data loss -- the record count stays correct and the contents
// are gone -- which is the worst kind of bug to find in production.
TEST(FilterOperatorTest, PredicateDoesNotConsumePayload) {
    auto op =
        ripple::make_filter<std::string>([](const std::string& text) { return !text.empty(); });
    TestCollector<std::string> out;

    op->process(ripple::make_record(std::string{"a genuinely long payload string"},
                                    ripple::timestamp_from_millis(1)),
                out);

    ASSERT_EQ(out.received.size(), 1U);
    EXPECT_EQ(out.received[0].value, "a genuinely long payload string");
}

TEST(OperatorTest, ReportsItsName) {
    auto mapper = ripple::make_map<int>([](int value) { return value; }, "double-fare");
    auto filter = ripple::make_filter<int>([](int value) { return value > 0; }, "drop-refunds");

    EXPECT_EQ(mapper->name(), "double-fare");
    EXPECT_EQ(filter->name(), "drop-refunds");
}

} // namespace
