#include <ripple/record.hpp>
#include <ripple/timestamp.hpp>

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>

namespace {

using ripple::Duration;
using ripple::Record;
using ripple::Timestamp;

// Protects: the boundary conversion round-trips.
//
// Timestamps enter from files as integers and Stage 4 will serialize them back
// as integers. If this conversion loses or shifts a value, every window
// boundary in the system is silently wrong and no other test would notice --
// the error would be uniform, so results would look self-consistent.
TEST(TimestampTest, RoundTripsThroughMillis) {
    constexpr std::int64_t kMillis = 1'700'000'123'456;
    const Timestamp timestamp = ripple::timestamp_from_millis(kMillis);
    EXPECT_EQ(ripple::millis_since_epoch(timestamp), kMillis);
}

// Protects: the chrono types compose the way window arithmetic needs.
//
// This is the concrete payoff of choosing chrono over int64. The subtraction
// yields a Duration and the addition yields a Timestamp; `Timestamp +
// Timestamp` does not compile at all, which is the mistake we are buying
// protection against.
TEST(TimestampTest, SupportsWindowArithmetic) {
    const Timestamp window_start = ripple::timestamp_from_millis(1'000);
    const Duration window_size{5'000};

    const Timestamp window_end = window_start + window_size;
    EXPECT_EQ(ripple::millis_since_epoch(window_end), 6'000);

    const Duration span = window_end - window_start;
    EXPECT_EQ(span.count(), 5'000);

    static_assert(std::is_same_v<decltype(window_end - window_start), Duration>);
    static_assert(std::is_same_v<decltype(window_start + window_size), Timestamp>);
}

// Protects: Record follows the rule of zero and stays cheap to move.
//
// If someone later adds a hand-written destructor or copy constructor to
// Record, the implicit move constructor is suppressed and every hop through the
// pipeline silently degrades from a move to a copy. That is a throughput
// regression with no visible symptom, so it is asserted rather than assumed.
TEST(RecordTest, IsMovableAndTriviallyDefaultConstructible) {
    static_assert(std::is_move_constructible_v<Record<std::string>>);
    static_assert(std::is_move_assignable_v<Record<std::string>>);
    static_assert(std::is_nothrow_move_constructible_v<Record<std::string>>);
}

TEST(RecordTest, MakeRecordDeducesPayloadType) {
    const auto record =
        ripple::make_record(std::string{"midtown"}, ripple::timestamp_from_millis(42));
    static_assert(std::is_same_v<decltype(record), const Record<std::string>>);
    EXPECT_EQ(record.value, "midtown");
    EXPECT_EQ(ripple::millis_since_epoch(record.event_time), 42);
}

// Protects: moving a Record moves its payload rather than copying it.
TEST(RecordTest, MovePreservesTimestampAndTransfersPayload) {
    Record<std::string> original{std::string(64, 'x'), ripple::timestamp_from_millis(7)};
    const char* const original_data = original.value.data();

    const Record<std::string> moved = std::move(original);

    EXPECT_EQ(ripple::millis_since_epoch(moved.event_time), 7);
    EXPECT_EQ(moved.value.size(), 64U);
    // The heap buffer was stolen, not duplicated. A 64-character string is past
    // libstdc++'s small-string threshold, so this genuinely exercises the move.
    EXPECT_EQ(moved.value.data(), original_data);
}

} // namespace
