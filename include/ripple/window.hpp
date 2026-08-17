#pragma once

#include <ripple/record.hpp>
#include <ripple/timestamp.hpp>

#include <compare>
#include <cstdint>
#include <utility>
#include <vector>

namespace ripple {

/// A half-open interval in event time: `[start, end)`.
///
/// Half-open matters. With inclusive ends, a record landing exactly on a
/// boundary belongs to two adjacent tumbling windows and gets counted twice --
/// a doubling error that appears only at boundaries and so survives casual
/// testing.
struct TimeWindow {
    Timestamp start;
    Timestamp end;

    /// The largest event time this window can contain. Used as the timestamp of
    /// the window's own output record, following the convention that a window's
    /// result is stamped at the last instant it covers rather than at its
    /// exclusive end -- stamping it at `end` would place the result in the
    /// *next* window if it flowed into another windowing operator.
    [[nodiscard]] constexpr Timestamp max_timestamp() const noexcept { return end - Duration{1}; }

    [[nodiscard]] constexpr bool contains(Timestamp timestamp) const noexcept {
        return timestamp >= start && timestamp < end;
    }

    /// Strict overlap, not adjacency. `[1000,2000)` and `[2000,3000)` do not
    /// overlap: for session windows that is precisely the case where the
    /// inactivity gap was met exactly and the session should end.
    [[nodiscard]] constexpr bool overlaps(const TimeWindow& other) const noexcept {
        return start < other.end && other.start < end;
    }

    friend bool operator==(const TimeWindow&, const TimeWindow&) = default;

    /// Orders by start, then end -- which is what lets the window operator hold
    /// windows in an ordered map and find merge candidates by looking only at
    /// immediate neighbours.
    friend std::strong_ordering operator<=>(const TimeWindow&, const TimeWindow&) = default;
};

/// A window's computed result, tagged with the window it came from.
///
/// The window must travel with the value: downstream has no other way to know
/// which interval a number refers to, and with allowed lateness the same window
/// can be emitted more than once.
template<typename T>
struct WindowResult {
    TimeWindow window;
    T value;
};

/// Receives records that arrived too late to be placed in any window.
///
/// A side output rather than a silent drop. Late data is a symptom -- of a
/// watermark bound set too tight, or of a genuinely misbehaving upstream -- and
/// data that disappears without a trace is indistinguishable from data that was
/// never sent.
template<typename T>
class LateRecordHandler {
public:
    LateRecordHandler() = default;
    virtual ~LateRecordHandler() = default;

    LateRecordHandler(const LateRecordHandler&) = delete;
    LateRecordHandler& operator=(const LateRecordHandler&) = delete;
    LateRecordHandler(LateRecordHandler&&) = delete;
    LateRecordHandler& operator=(LateRecordHandler&&) = delete;

    virtual void on_late_record(Record<T>&& record) = 0;
};

/// Retains late records so they can be inspected rather than lost.
///
/// In a real deployment this is where you would count, log, or route to a
/// dead-letter stream; the count alone is often enough to tell you the
/// watermark bound is set too tight.
template<typename T>
class CollectingLateRecordHandler final : public LateRecordHandler<T> {
public:
    void on_late_record(Record<T>&& record) override { records_.push_back(std::move(record)); }

    [[nodiscard]] const std::vector<Record<T>>& records() const noexcept { return records_; }

private:
    std::vector<Record<T>> records_;
};

namespace detail {

/// Floor division, as distinct from C++'s truncating division.
///
/// `-1 / 1000` is `0` in C++, not `-1`. Window boundaries computed with
/// truncating division are therefore wrong for every timestamp before the Unix
/// epoch: records from 1969 would be assigned to the window starting at 1970.
/// This is not hypothetical for the demo dataset, which contains timestamps
/// stamped in 1900 and 2098 by broken meters.
[[nodiscard]] constexpr std::int64_t floor_div(std::int64_t numerator,
                                               std::int64_t denominator) noexcept {
    const std::int64_t quotient = numerator / denominator;
    const std::int64_t remainder = numerator % denominator;
    const bool rounds_toward_zero = remainder != 0 && ((remainder < 0) != (denominator < 0));
    return rounds_toward_zero ? quotient - 1 : quotient;
}

} // namespace detail

} // namespace ripple
