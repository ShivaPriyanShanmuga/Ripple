#pragma once

#include <ripple/collector.hpp>
#include <ripple/operator.hpp>
#include <ripple/record.hpp>
#include <ripple/serialization.hpp>
#include <ripple/state/state_backend.hpp>
#include <ripple/state/state_handles.hpp>
#include <ripple/timestamp.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace ripple {

/// A value paired with the key it belongs to.
template<typename Key, typename Value>
struct KeyedValue {
    Key key;
    Value value;
};

/// Base for operators that maintain per-key state.
///
/// ## What `keyBy` actually does
///
/// It routes. Every record carrying the same key is guaranteed to be handled in
/// the same place, so that key's state is only ever touched by one processor.
/// In Stage 4 "one place" means one operator instance; in Stage 6 it will mean
/// one thread, and nothing here changes.
///
/// That guarantee is the reason keyed state needs **no locking at all**. The
/// alternative design -- one shared map of counters behind a mutex, contended by
/// every thread on every record -- is not merely slower, it does not scale: the
/// contention grows with the thread count. Partitioning removes the shared
/// resource instead of protecting it, which is why real engines partition rather
/// than lock.
///
/// ## Why `process` is `final`
///
/// Subclasses override `process_keyed`, not `process`. The base sets the
/// backend's current key first, so a subclass cannot forget to. If subclasses
/// could override `process`, the failure mode would be an operator reading state
/// under whichever key happened to be current from the *previous* record --
/// producing plausible, wrong, non-deterministic results that no type would
/// catch.
template<typename In, typename Out, typename KeySelector>
class KeyedOperator : public Operator<In, Out> {
public:
    using KeyType = std::decay_t<std::invoke_result_t<KeySelector&, const In&>>;
    static_assert(Serializable<KeyType>,
                  "the key type must be serializable: keys are stored as bytes so that one "
                  "state backend can serve operators keyed on any type");

    KeyedOperator(StateBackend& backend, KeySelector selector, std::string name)
        : backend_(&backend), selector_(std::move(selector)), name_(std::move(name)) {}

    void process(Record<In>&& record, Collector<Out>& out) final {
        current_key_ = selector_(record.value);
        backend_->set_current_key(serialize(current_key_));
        process_keyed(std::move(record), out);
    }

    [[nodiscard]] std::string_view name() const noexcept override { return name_; }

protected:
    virtual void process_keyed(Record<In>&& record, Collector<Out>& out) = 0;

    /// Valid only for the duration of a `process_keyed` call.
    [[nodiscard]] const KeyType& current_key() const noexcept { return current_key_; }

    [[nodiscard]] StateBackend& state() const noexcept { return *backend_; }

private:
    StateBackend* backend_;
    KeySelector selector_;
    std::string name_;
    KeyType current_key_{};
};

/// Maintains a running aggregate per key and emits the updated value on every
/// record.
///
/// Takes **two** projections, because keyed aggregation is inherently two
/// questions: a key selector answering "which bucket does this belong to" and a
/// value selector answering "what part of it is being aggregated". `keyBy(zone)
/// .sum(fare)` needs both, and collapsing them -- requiring the aggregator to
/// consume the whole input type -- would force a bespoke aggregator per record
/// type rather than letting `SumAggregator<int64_t>` be reused everywhere.
///
/// Emits per record rather than per window, deliberately: this demonstrates
/// keyed state on its own, with no windowing mixed in.
template<typename In, typename KeySelector, typename ValueSelector, typename Aggregator>
class KeyedAggregateOperator final
    : public KeyedOperator<In,
                           KeyedValue<std::decay_t<std::invoke_result_t<KeySelector&, const In&>>,
                                      typename Aggregator::OutputType>,
                           KeySelector> {
public:
    using Key = std::decay_t<std::invoke_result_t<KeySelector&, const In&>>;
    using OutputValue = KeyedValue<Key, typename Aggregator::OutputType>;
    using Base = KeyedOperator<In, OutputValue, KeySelector>;

    KeyedAggregateOperator(StateBackend& backend, KeySelector key_selector,
                           ValueSelector value_selector, Aggregator aggregator,
                           std::string state_name = "keyed-aggregate",
                           std::string name = "keyed-aggregate")
        : Base(backend, std::move(key_selector), std::move(name)),
          value_selector_(std::move(value_selector)),
          state_(backend, std::move(state_name), std::move(aggregator)) {}

    /// Nothing to snapshot here: everything this operator holds lives in the
    /// backend, so Stage 7 snapshots the backend once and covers every keyed
    /// operator sharing it. Spelled out rather than inherited so the reason is
    /// visible at the point someone would otherwise wonder.
    void snapshot_state(ByteWriter& /*writer*/) const override {}

    void restore_state(ByteReader& /*reader*/) override {}

protected:
    // Reads the payload and lets the record go; there is nothing downstream to
    // forward it to, since this operator emits an aggregate instead.
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    void process_keyed(Record<In>&& record, Collector<OutputValue>& out) override {
        const Timestamp event_time = record.event_time;
        state_.add(value_selector_(record.value));
        out.collect(
            Record<OutputValue>{OutputValue{this->current_key(), state_.get()}, event_time});
    }

private:
    ValueSelector value_selector_;
    AggregatingState<Aggregator> state_;
};

template<typename In, typename KeySelector, typename ValueSelector, typename Aggregator>
[[nodiscard]] auto make_keyed_aggregate(StateBackend& backend, KeySelector key_selector,
                                        ValueSelector value_selector, Aggregator aggregator,
                                        std::string state_name = "keyed-aggregate",
                                        std::string name = "keyed-aggregate") {
    return std::make_unique<KeyedAggregateOperator<In, KeySelector, ValueSelector, Aggregator>>(
        backend, std::move(key_selector), std::move(value_selector), std::move(aggregator),
        std::move(state_name), std::move(name));
}

} // namespace ripple
