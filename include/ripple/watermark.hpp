#pragma once

#include <ripple/timestamp.hpp>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <optional>
#include <vector>

namespace ripple {

/// An assertion about progress in event time.
///
/// A watermark with timestamp T claims: "no record with event time <= T will
/// arrive on this channel from now on."
///
/// What it does NOT claim, and this is the distinction that matters: it is not a
/// guarantee. It is a heuristic produced by a strategy the user chose. Records
/// older than T can and do still arrive; they are *late*, and Stage 3 decides
/// what to do with them. A watermark's job is to authorize downstream operators
/// to act -- to close a window and emit a result -- not to promise they will
/// never be contradicted.
struct Watermark {
    Timestamp timestamp{};

    friend bool operator==(const Watermark&, const Watermark&) = default;
    friend std::strong_ordering operator<=>(const Watermark&, const Watermark&) = default;
};

/// Emitted once the input is exhausted. Every window still open must fire,
/// because nothing further can ever arrive to change its contents.
inline constexpr Watermark kEndOfStreamWatermark{kMaxTimestamp};

/// Combines the watermarks of several input channels into one.
///
/// The combined watermark is the **minimum** across channels, never the maximum
/// or the most recent. If channel A has reached 09:05 and channel B has only
/// reached 09:01, this operator may not claim 09:05: channel B can still deliver
/// a record with event time 09:03. You are only as caught up as your slowest
/// input.
///
/// The operational consequence is worth knowing before Stage 6 makes it real:
/// an input channel that goes *silent* stops advancing its own watermark, so the
/// minimum stops advancing, so every downstream window stops firing. The job
/// looks healthy -- records flowing, no errors -- and simply produces no output.
///
/// Not yet wired into a pipeline: Stage 2's topology is a linear chain, so no
/// operator has more than one input. Built and tested here because it is the
/// conceptual core of watermark propagation, and consumed in Stage 6 when
/// fan-in appears.
class WatermarkTracker {
public:
    explicit WatermarkTracker(std::size_t channel_count)
        : channel_watermarks_(channel_count, kMinTimestamp) {}

    /// Records a watermark on one channel.
    ///
    /// Returns the new combined watermark if it advanced, and nothing if it did
    /// not. Callers forward downstream only on advance, which is what keeps
    /// watermarks monotonic and suppresses redundant emissions.
    [[nodiscard]] std::optional<Watermark> update(std::size_t channel, Watermark watermark) {
        // Per-channel monotonicity is enforced here rather than trusted. A
        // channel that regressed would drag the minimum backwards, and a
        // downstream window that had already fired on the higher watermark
        // would then be re-opened -- producing a duplicate, contradictory
        // result for a window that was supposed to be complete.
        Timestamp& channel_watermark = channel_watermarks_[channel];
        channel_watermark = std::max(channel_watermark, watermark.timestamp);

        const Timestamp combined =
            *std::min_element(channel_watermarks_.begin(), channel_watermarks_.end());

        if (combined > combined_.timestamp) {
            combined_ = Watermark{combined};
            return combined_;
        }
        return std::nullopt;
    }

    [[nodiscard]] Watermark current() const noexcept { return combined_; }

    [[nodiscard]] std::size_t channel_count() const noexcept { return channel_watermarks_.size(); }

private:
    std::vector<Timestamp> channel_watermarks_;
    Watermark combined_{kMinTimestamp};
};

} // namespace ripple
