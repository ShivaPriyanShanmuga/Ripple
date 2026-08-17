#pragma once

#include <ripple/collector.hpp>
#include <ripple/operator.hpp>
#include <ripple/record.hpp>
#include <ripple/serialization.hpp>
#include <ripple/state/key_group.hpp>
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
#include <type_traits>
#include <utility>
#include <vector>

namespace ripple {

/// Buffers records into windows, **per key**, and emits each window's aggregate
/// when event time proves the window is complete.
///
/// ## What "the window fires" means mechanically
///
/// There is nothing mystical about it:
///   1. a record arrives, its key is selected, the assigner says which windows it
///      belongs to, and its value is folded into that key's accumulator for each
///      of those windows;
///   2. a watermark arrives;
///   3. every window whose `end` is at or before that watermark is now provably
///      complete, so its accumulator becomes a result and is emitted;
///   4. once a window can no longer receive even late records, its state is
///      **deleted**.
///
/// Step 4 is the one worth dwelling on: firing is how a windowed job reclaims
/// memory. A pipeline whose watermark stops advancing does not merely stop
/// producing output -- it stops freeing window state and grows until it dies.
/// That is the practical reason Stage 2's idle-channel stall matters.
///
/// ## Keyed vs global
///
/// State is `key -> window -> accumulator`. With the default `GlobalKeySelector`
/// there is exactly one key and the operator behaves as an unkeyed windower, so
/// the two cases share one implementation rather than one being a copy of the
/// other with an extra map.
///
/// Windows are nested *under* the key rather than keyed by a flat `(key, window)`
/// pair, because session merging has to scan a single key's windows in time
/// order. A flat map ordered by `(key, window)` would happen to work, but only by
/// accident of that ordering; nesting makes the requirement explicit.
/// `KeySelector` answers "whose window is this"; `ValueSelector` answers "what
/// part of the record is being aggregated". Two projections rather than one for
/// the same reason the keyed aggregate takes two: collapsing them would force a
/// bespoke aggregator per record type instead of letting
/// `SumAggregator<int64_t>` be reused everywhere.
template<typename In, typename Assigner, typename Aggregator,
         typename KeySelector = GlobalKeySelector, typename ValueSelector = IdentitySelector>
class WindowOperator final
    : public Operator<In, WindowResult<std::decay_t<std::invoke_result_t<KeySelector&, const In&>>,
                                       typename Aggregator::OutputType>> {
public:
    using KeyType = std::decay_t<std::invoke_result_t<KeySelector&, const In&>>;
    using AccumulatorType = typename Aggregator::AccumulatorType;
    using AggregateOutput = typename Aggregator::OutputType;
    using ResultType = WindowResult<KeyType, AggregateOutput>;

    WindowOperator(Assigner assigner, Aggregator aggregator,
                   Duration allowed_lateness = Duration{0}, std::string name = "window",
                   KeySelector key_selector = {}, ValueSelector value_selector = {})
        : assigner_(std::move(assigner)), aggregator_(std::move(aggregator)),
          key_selector_(std::move(key_selector)), value_selector_(std::move(value_selector)),
          allowed_lateness_(allowed_lateness), name_(std::move(name)) {}

    /// Records produce no output directly. A window's contents are not known to
    /// be complete until a watermark says so, which is why the collector is
    /// unused here and everything is emitted from `on_watermark`.
    void process(Record<In>&& record, Collector<ResultType>& /*out*/) override {
        scratch_windows_.clear();
        assigner_.assign(record.event_time, scratch_windows_);

        const KeyType key = key_selector_(record.value);
        WindowMap& windows = keyed_windows_[key];

        bool placed_anywhere = false;
        for (const TimeWindow& window : scratch_windows_) {
            if (is_expired(window)) {
                continue;
            }

            auto [entry, inserted] =
                windows.try_emplace(window, WindowState{aggregator_.create(), true});

            // `record.value` is read once per window rather than moved. This is
            // the fan-out copy anticipated in D-015: a sliding-window record
            // genuinely belongs to several windows at once, and no ownership
            // scheme avoids that -- the data has to exist in several places.
            // Passing it as `const In&` leaves the aggregator to copy only what
            // it actually retains.
            aggregator_.add(entry->second.accumulator, value_selector_(record.value));
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
            if (windows.empty()) {
                keyed_windows_.erase(key);
            }
            if (late_handler_ != nullptr) {
                late_handler_->on_late_record(std::move(record));
            }
            return;
        }

        // Compiled out entirely for tumbling and sliding windows, whose
        // boundaries are fixed by arithmetic and can never move.
        if constexpr (Assigner::is_merging) {
            merge_overlapping_windows(windows);
        }
    }

    void on_watermark(Watermark watermark, Collector<ResultType>& out) override {
        current_watermark_ = watermark.timestamp;

        for (auto& [key, windows] : keyed_windows_) {
            for (auto& [window, state] : windows) {
                if (window.end <= current_watermark_ && state.dirty) {
                    out.collect(Record<ResultType>{
                        ResultType{key, window, aggregator_.result(state.accumulator)},
                        window.max_timestamp()});
                    // Cleared so a window that has fired and received nothing
                    // new is not re-emitted on every subsequent watermark. A
                    // late record landing in it sets the flag again, which is
                    // what produces the corrected result.
                    state.dirty = false;
                }
            }

            // Purge windows that can no longer receive anything. This is the
            // only place window memory is released.
            std::erase_if(windows, [this](const auto& entry) {
                return entry.first.end + allowed_lateness_ <= current_watermark_;
            });
        }

        // A key with no live windows is itself dead weight. Without this the
        // window maps shrink but the *key* map grows forever -- a leak
        // proportional to key cardinality, which for something like a user id is
        // unbounded. The same failure the state backend guards against (D-037).
        std::erase_if(keyed_windows_, [](const auto& entry) { return entry.second.empty(); });

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

    /// ## Window contents are *operator* state, and must be checkpointed here
    ///
    /// Unlike a keyed aggregate, this operator does not keep its state in the
    /// `StateBackend` -- it holds `key -> window -> accumulator` in its own map,
    /// because window state is indexed by a pair the backend's flat key-value
    /// interface does not model.
    ///
    /// The consequence is that the backend snapshot does **not** cover it, so
    /// without these two functions a windowed job checkpoints nothing and
    /// recovery silently restarts every partially-filled window from empty. That
    /// is not a hypothetical: it is exactly what the demo application did before
    /// this existed, and the symptom was a recovered run whose totals were
    /// plausible and quietly short.
    ///
    /// `current_watermark_` is part of the state too. Restoring windows without
    /// it would leave the operator believing no time has passed, so it would
    /// re-admit records it had already declared late.
    void snapshot_state(ByteWriter& writer) const override {
        detail::write_length(writer, keyed_windows_.size());
        for (const auto& [key, windows] : keyed_windows_) {
            Serializer<KeyType>::write(writer, key);
            detail::write_length(writer, windows.size());
            for (const auto& [window, state] : windows) {
                Serializer<TimeWindow>::write(writer, window);
                Serializer<AccumulatorType>::write(writer, state.accumulator);
                Serializer<bool>::write(writer, state.dirty);
            }
        }
        Serializer<Timestamp>::write(writer, current_watermark_);
    }

    /// Additive and filtered by key group, which is what makes a windowed job
    /// **rescalable**.
    ///
    /// Window state is keyed by the same key the shuffle partitions on, so it
    /// redistributes exactly like backend state (D-062): each task reads every
    /// old snapshot and keeps the groups it now owns. Without this, a rescaled
    /// windowed job silently restarts every window from empty -- it does not
    /// fail, it just under-reports, which is the same quiet failure D-066 was.
    void restore_state(ByteReader& reader, KeyGroupRange range) override {
        const auto key_count = static_cast<std::size_t>(reader.read_fixed<std::uint32_t>());
        for (std::size_t i = 0; i < key_count; ++i) {
            KeyType key = Serializer<KeyType>::read(reader);
            const auto window_count = static_cast<std::size_t>(reader.read_fixed<std::uint32_t>());
            WindowMap windows;
            for (std::size_t j = 0; j < window_count; ++j) {
                const TimeWindow window = Serializer<TimeWindow>::read(reader);
                // Not const: it is moved into the window below, and `std::move`
                // on a const object silently copies (D-015).
                // NOLINTNEXTLINE(misc-const-correctness)
                AccumulatorType accumulator = Serializer<AccumulatorType>::read(reader);
                const bool dirty = Serializer<bool>::read(reader);
                windows.emplace(window, WindowState{std::move(accumulator), dirty});
            }
            // Entries outside the range are still parsed -- the reader must
            // advance past them -- and then dropped.
            if (range.contains(key_group_of(serialize(key)))) {
                keyed_windows_.emplace(std::move(key), std::move(windows));
            }
        }
        // Event-time progress belongs to the whole operator, not to any key, so
        // it is taken from whichever snapshot advanced furthest. Taking the last
        // one read would depend on blob ordering.
        const Timestamp restored = Serializer<Timestamp>::read(reader);
        current_watermark_ = std::max(current_watermark_, restored);
    }

    /// Attach a destination for records that arrive too late for any window.
    /// Optional: with no handler they are counted and dropped.
    void set_late_record_handler(LateRecordHandler<In>* handler) noexcept {
        late_handler_ = handler;
    }

    [[nodiscard]] std::size_t late_record_count() const noexcept { return late_record_count_; }

    /// Live windows across every key. The quantity to watch when reasoning about
    /// a windowed job's memory profile.
    [[nodiscard]] std::size_t open_window_count() const noexcept {
        std::size_t total = 0;
        for (const auto& [key, windows] : keyed_windows_) {
            total += windows.size();
        }
        return total;
    }

    /// Distinct keys currently holding window state.
    [[nodiscard]] std::size_t keyed_state_size() const noexcept { return keyed_windows_.size(); }

private:
    struct WindowState {
        AccumulatorType accumulator;
        /// Whether anything has been added since this window last emitted.
        bool dirty;
    };

    using WindowMap = std::map<TimeWindow, WindowState>;

    [[nodiscard]] bool is_expired(const TimeWindow& window) const noexcept {
        return window.end + allowed_lateness_ <= current_watermark_;
    }

    /// Collapses overlapping session windows within one key.
    ///
    /// Only adjacent entries need comparing: the map is ordered by start, so if
    /// window A overlapped a non-adjacent window C, it would necessarily also
    /// overlap everything between them.
    void merge_overlapping_windows(WindowMap& windows) {
        auto current = windows.begin();
        while (current != windows.end()) {
            auto next = std::next(current);
            if (next == windows.end()) {
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

            windows.erase(next);
            windows.erase(current);

            // Resume from the merged window rather than moving on: it may now
            // reach far enough to overlap the window after it as well.
            current =
                windows.insert_or_assign(merged, WindowState{std::move(accumulator), true}).first;
        }
    }

    Assigner assigner_;
    Aggregator aggregator_;
    KeySelector key_selector_;
    ValueSelector value_selector_;
    Duration allowed_lateness_;
    std::string name_;

    /// `key -> window -> accumulator`. Both levels ordered: the inner one so
    /// session merging can scan neighbours in time order, the outer one so
    /// iteration is deterministic.
    std::map<KeyType, WindowMap> keyed_windows_;

    Timestamp current_watermark_{kMinTimestamp};
    LateRecordHandler<In>* late_handler_ = nullptr;
    std::size_t late_record_count_ = 0;

    /// Reused across calls so assigning a record to N sliding windows does not
    /// allocate on every record.
    std::vector<TimeWindow> scratch_windows_;
};

/// Windows an unkeyed stream.
template<typename In, typename Assigner, typename Aggregator>
[[nodiscard]] auto make_window(Assigner assigner, Aggregator aggregator,
                               Duration allowed_lateness = Duration{0},
                               std::string name = "window") {
    return std::make_unique<WindowOperator<In, Assigner, Aggregator>>(
        std::move(assigner), std::move(aggregator), allowed_lateness, std::move(name));
}

/// Windows a stream per key -- each key keeps its own independent set of windows,
/// which is what "per-region revenue in 5-minute windows" actually means.
template<typename In, typename Assigner, typename Aggregator, typename KeySelector,
         typename ValueSelector>
[[nodiscard]] auto make_keyed_window(KeySelector key_selector, ValueSelector value_selector,
                                     Assigner assigner, Aggregator aggregator,
                                     Duration allowed_lateness = Duration{0},
                                     std::string name = "keyed-window") {
    return std::make_unique<WindowOperator<In, Assigner, Aggregator, KeySelector, ValueSelector>>(
        std::move(assigner), std::move(aggregator), allowed_lateness, std::move(name),
        std::move(key_selector), std::move(value_selector));
}

} // namespace ripple
