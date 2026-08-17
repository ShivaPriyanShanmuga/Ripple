#pragma once

#include <ripple/collector.hpp>
#include <ripple/operator.hpp>
#include <ripple/record.hpp>
#include <ripple/timestamp.hpp>
#include <ripple/watermark.hpp>
#include <ripple/window.hpp>

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ripple {

/// Buffers records into windows and emits each window's aggregate when event
/// time proves the window is complete.
///
/// ## What "the window fires" means mechanically
///
/// There is nothing mystical about it:
///   1. a record arrives, the assigner says which windows it belongs to, and its
///      value is folded into each of those windows' accumulators;
///   2. a watermark arrives;
///   3. every window whose `end` is at or before that watermark is now provably
///      complete, so its accumulator is turned into a result and emitted;
///   4. once the window can no longer receive even late records, its state is
///      **deleted**.
///
/// Step 4 is the one worth dwelling on: firing is how a windowed job reclaims
/// memory. A pipeline whose watermark stops advancing does not merely stop
/// producing output -- it stops freeing window state, and grows until it dies.
/// That is the practical reason the idle-channel stall from Stage 2 matters.
///
/// Windows here are global to the stream. Stage 4 adds `keyBy`, after which each
/// key maintains its own independent set of windows.
template<typename In, typename Assigner, typename Aggregator>
class WindowOperator final : public Operator<In, WindowResult<typename Aggregator::OutputType>> {
public:
    using AccumulatorType = typename Aggregator::AccumulatorType;
    using AggregateOutput = typename Aggregator::OutputType;
    using ResultType = WindowResult<AggregateOutput>;

    WindowOperator(Assigner assigner, Aggregator aggregator,
                   Duration allowed_lateness = Duration{0}, std::string name = "window")
        : assigner_(std::move(assigner)), aggregator_(std::move(aggregator)),
          allowed_lateness_(allowed_lateness), name_(std::move(name)) {}

    /// Records produce no output directly. A window's contents are not known to
    /// be complete until a watermark says so, which is why the collector is
    /// unused here and everything is emitted from `on_watermark`.
    void process(Record<In>&& record, Collector<ResultType>& /*out*/) override {
        scratch_windows_.clear();
        assigner_.assign(record.event_time, scratch_windows_);

        bool placed_anywhere = false;
        for (const TimeWindow& window : scratch_windows_) {
            if (is_expired(window)) {
                continue;
            }

            auto [entry, inserted] =
                windows_.try_emplace(window, WindowState{aggregator_.create(), true});

            // `record.value` is read once per window rather than moved. This is
            // the fan-out copy anticipated in D-015: a sliding-window record
            // genuinely belongs to several windows at once, and no ownership
            // scheme avoids that -- the data has to exist in several places.
            // Passing it as `const In&` at least leaves the aggregator to copy
            // only what it actually retains.
            aggregator_.add(entry->second.accumulator, record.value);
            entry->second.dirty = true;
            placed_anywhere = true;
        }

        if (!placed_anywhere) {
            // Every window this record belonged to has already been purged, so
            // there is nowhere left to put it. Side-output rather than drop:
            // late data is a symptom -- of a watermark bound set too tight, or a
            // misbehaving upstream -- and silently vanishing data is
            // indistinguishable from data that was never sent.
            ++late_record_count_;
            if (late_handler_ != nullptr) {
                late_handler_->on_late_record(std::move(record));
            }
            return;
        }

        // Compiled out entirely for tumbling and sliding windows, whose
        // boundaries are fixed by arithmetic and can never move.
        if constexpr (Assigner::is_merging) {
            merge_overlapping_windows();
        }
    }

    void on_watermark(Watermark watermark, Collector<ResultType>& out) override {
        current_watermark_ = watermark.timestamp;

        for (auto& [window, state] : windows_) {
            if (window.end <= current_watermark_ && state.dirty) {
                out.collect(
                    Record<ResultType>{ResultType{window, aggregator_.result(state.accumulator)},
                                       window.max_timestamp()});
                // Cleared so a window that has fired and received nothing new
                // is not re-emitted on every subsequent watermark. It is set
                // again by any late record that lands in this window, which is
                // what produces the updated result.
                state.dirty = false;
            }
        }

        // Purge windows that can no longer receive anything, late or otherwise.
        // This is the only place window memory is released.
        std::erase_if(windows_, [this](const auto& entry) {
            return entry.first.end + allowed_lateness_ <= current_watermark_;
        });

        // Forward the watermark AFTER emitting results, and never forget to
        // forward it at all. Results carry timestamps inside the window they
        // came from, so they must reach downstream before the watermark that
        // would render them late. And an override that swallows the watermark
        // freezes event time for the entire rest of the pipeline: downstream
        // windows never fire, never free state, and the job produces nothing
        // while looking perfectly healthy.
        out.emit_watermark(watermark);
    }

    [[nodiscard]] std::string_view name() const noexcept override { return name_; }

    /// Attach a destination for records that arrive too late for any window.
    /// Optional: with no handler they are counted and dropped.
    void set_late_record_handler(LateRecordHandler<In>* handler) noexcept {
        late_handler_ = handler;
    }

    [[nodiscard]] std::size_t late_record_count() const noexcept { return late_record_count_; }

    /// Number of windows currently holding state. The quantity to watch when
    /// reasoning about a windowed job's memory profile.
    [[nodiscard]] std::size_t open_window_count() const noexcept { return windows_.size(); }

private:
    struct WindowState {
        AccumulatorType accumulator;
        /// Whether anything has been added since this window last emitted.
        bool dirty;
    };

    [[nodiscard]] bool is_expired(const TimeWindow& window) const noexcept {
        return window.end + allowed_lateness_ <= current_watermark_;
    }

    /// Collapses overlapping session windows into one.
    ///
    /// Only adjacent entries need comparing: the map is ordered by start, so if
    /// window A overlapped a non-adjacent window C, it would necessarily also
    /// overlap everything between them.
    void merge_overlapping_windows() {
        auto current = windows_.begin();
        while (current != windows_.end()) {
            auto next = std::next(current);
            if (next == windows_.end()) {
                break;
            }
            if (!current->first.overlaps(next->first)) {
                ++current;
                continue;
            }

            const TimeWindow merged{current->first.start,
                                    std::max(current->first.end, next->first.end)};
            // clang-tidy suggests `const` here. Do not take that advice: the
            // accumulator is moved into the merged window below, and `std::move`
            // on a const object silently binds to const&& and copies instead --
            // the exact trap noted in D-015. A const-correctness check that does
            // not model moves gets this backwards.
            // NOLINTNEXTLINE(misc-const-correctness)
            AccumulatorType accumulator = aggregator_.merge(std::move(current->second.accumulator),
                                                            std::move(next->second.accumulator));

            windows_.erase(next);
            windows_.erase(current);

            // Resume from the merged window rather than moving on: it may now
            // reach far enough to overlap the window after it as well.
            current =
                windows_.insert_or_assign(merged, WindowState{std::move(accumulator), true}).first;
        }
    }

    Assigner assigner_;
    Aggregator aggregator_;
    Duration allowed_lateness_;
    std::string name_;

    /// Ordered by window start, which is what makes session merging a scan of
    /// neighbours rather than a search.
    std::map<TimeWindow, WindowState> windows_;

    Timestamp current_watermark_{kMinTimestamp};
    LateRecordHandler<In>* late_handler_ = nullptr;
    std::size_t late_record_count_ = 0;

    /// Reused across calls so that assigning a record to N sliding windows does
    /// not allocate on every record.
    std::vector<TimeWindow> scratch_windows_;
};

template<typename In, typename Assigner, typename Aggregator>
[[nodiscard]] auto make_window(Assigner assigner, Aggregator aggregator,
                               Duration allowed_lateness = Duration{0},
                               std::string name = "window") {
    return std::make_unique<WindowOperator<In, Assigner, Aggregator>>(
        std::move(assigner), std::move(aggregator), allowed_lateness, std::move(name));
}

} // namespace ripple
