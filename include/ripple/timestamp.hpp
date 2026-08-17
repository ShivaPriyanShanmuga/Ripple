#pragma once

#include <chrono>
#include <cstdint>

namespace ripple {

/// Event-time durations. Milliseconds is the resolution the engine reasons in:
/// fine enough for windowing, coarse enough that an int64 of them covers any
/// realistic event-time range without overflow.
using Duration = std::chrono::milliseconds;

/// A point in event time, measured from the Unix epoch.
///
/// Deliberately a chrono type rather than a bare std::int64_t. The type system
/// then enforces the arithmetic that window logic depends on:
///
///     Timestamp - Timestamp -> Duration     (how far apart are two events)
///     Timestamp + Duration  -> Timestamp    (window start plus window size)
///     Timestamp + Timestamp -> compile error
///
/// That last line is the point. Adding two timestamps is meaningless, and it is
/// exactly the mistake that window-boundary arithmetic invites. With int64
/// milliseconds it compiles silently and produces a window in the year 55000.
using Timestamp = std::chrono::sys_time<Duration>;

/// Sentinels. kMaxTimestamp is what Stage 2 will use to signal "this stream is
/// finished, nothing later can ever arrive".
inline constexpr Timestamp kMinTimestamp = Timestamp::min();
inline constexpr Timestamp kMaxTimestamp = Timestamp::max();

/// Conversion at the system boundary. Timestamps arrive from files and wire
/// formats as integers, and Stage 4 will serialize them back the same way, so
/// the conversion is explicit and lives in exactly one place.
[[nodiscard]] constexpr Timestamp timestamp_from_millis(std::int64_t millis) noexcept {
    return Timestamp{Duration{millis}};
}

[[nodiscard]] constexpr std::int64_t millis_since_epoch(Timestamp timestamp) noexcept {
    return timestamp.time_since_epoch().count();
}

} // namespace ripple
