#pragma once

#include <ripple/timestamp.hpp>
#include <ripple/window.hpp>

#include <cstdint>
#include <vector>

namespace ripple {

/// Window assigners answer one question: given a record's event time, which
/// windows does it belong to?
///
/// They are template parameters of the window operator rather than a virtual
/// interface. Assignment happens once per record per window and involves only
/// arithmetic, so there is nothing to gain from indirection here -- and unlike
/// the operator graph, the set of assigners does not need to be extensible at
/// runtime from configuration.
///
/// Each assigner exposes `static constexpr bool is_merging`, which the window
/// operator uses to compile out the merge machinery entirely for the
/// non-merging cases.

/// Fixed-size, back-to-back, non-overlapping. Every record lands in exactly one.
///
/// The cheapest option, and the default choice unless overlapping or
/// activity-defined windows are actually needed: memory is proportional to the
/// number of live windows, which is bounded by the watermark lag.
class TumblingWindows {
public:
    explicit constexpr TumblingWindows(Duration size) noexcept : size_(size) {}

    static constexpr bool is_merging = false;

    void assign(Timestamp timestamp, std::vector<TimeWindow>& out) const {
        const std::int64_t millis = millis_since_epoch(timestamp);
        const std::int64_t size = size_.count();
        const std::int64_t start = detail::floor_div(millis, size) * size;
        out.push_back(
            TimeWindow{timestamp_from_millis(start), timestamp_from_millis(start + size)});
    }

    [[nodiscard]] constexpr Duration size() const noexcept { return size_; }

private:
    Duration size_;
};

/// Fixed-size windows starting more often than they end, so they overlap.
///
/// Every record lands in `ceil(size / slide)` windows simultaneously. That
/// multiple is the memory cost, and it is the number to watch: a one-hour
/// window sliding every second puts each record in 3,600 windows at once. The
/// aggregator's accumulator is duplicated that many times.
class SlidingWindows {
public:
    constexpr SlidingWindows(Duration size, Duration slide) noexcept : size_(size), slide_(slide) {}

    static constexpr bool is_merging = false;

    void assign(Timestamp timestamp, std::vector<TimeWindow>& out) const {
        const std::int64_t millis = millis_since_epoch(timestamp);
        const std::int64_t size = size_.count();
        const std::int64_t slide = slide_.count();

        // The latest window that can contain this record starts at the largest
        // multiple of `slide` not exceeding the timestamp. Walk backwards from
        // there while the window still reaches the record.
        const std::int64_t last_start = detail::floor_div(millis, slide) * slide;
        for (std::int64_t start = last_start; start + size > millis; start -= slide) {
            out.push_back(
                TimeWindow{timestamp_from_millis(start), timestamp_from_millis(start + size)});
        }
    }

    [[nodiscard]] constexpr Duration size() const noexcept { return size_; }

    [[nodiscard]] constexpr Duration slide() const noexcept { return slide_; }

private:
    Duration size_;
    Duration slide_;
};

/// Windows defined by gaps in activity rather than by the clock.
///
/// A record provisionally opens the window `[t, t + gap)`. Windows that overlap
/// are then **merged** -- and merging is the thing that makes sessions
/// structurally different from the other two assigners.
///
/// Why only sessions merge: tumbling and sliding window boundaries are fixed in
/// advance by arithmetic on the timestamp alone, so no record can ever change
/// where a boundary lies. A session's boundaries are determined by the data. A
/// record arriving between two existing sessions proves there was no gap
/// between them after all, and the two sessions were never really two.
///
/// The consequence for the aggregator: it must supply a `merge` operation that
/// combines two accumulators. That is a genuine constraint -- it forces the
/// aggregation to be associative -- and it is why the window operator requires
/// `merge` only when the assigner is a merging one.
class SessionWindows {
public:
    explicit constexpr SessionWindows(Duration gap) noexcept : gap_(gap) {}

    static constexpr bool is_merging = true;

    void assign(Timestamp timestamp, std::vector<TimeWindow>& out) const {
        out.push_back(TimeWindow{timestamp, timestamp + gap_});
    }

    [[nodiscard]] constexpr Duration gap() const noexcept { return gap_; }

private:
    Duration gap_;
};

} // namespace ripple
