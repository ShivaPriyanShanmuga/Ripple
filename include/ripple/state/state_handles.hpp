#pragma once

#include <ripple/serialization.hpp>
#include <ripple/state/state_backend.hpp>

#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace ripple {

/// State handles are **views**, not containers.
///
/// A handle owns nothing. It holds a reference to the backend and a state name,
/// and every operation reads or writes under whatever key the backend currently
/// has. That is what makes it impossible to accidentally touch another key's
/// state: the handle has no way to name a key at all.
///
/// The cost of that design is a serialize/deserialize round trip on every
/// access. For an in-memory backend that is pure overhead over just holding a
/// `std::map<Key, T>` -- but it is what makes the backend swappable for a
/// file- or disk-backed one, and what lets Stage 7 snapshot state it knows
/// nothing about the type of.

/// A single value per key.
template<Serializable T>
class ValueState {
public:
    ValueState(StateBackend& backend, std::string name)
        : backend_(&backend), name_(std::move(name)) {}

    /// Empty means "this key has never been written", which callers must
    /// distinguish from a written zero -- hence `optional` rather than a
    /// default-constructed `T`. Collapsing the two is how "no purchases yet"
    /// silently becomes "purchased zero".
    [[nodiscard]] std::optional<T> value() const {
        const auto bytes = backend_->get(name_);
        if (!bytes.has_value()) {
            return std::nullopt;
        }
        return deserialize<T>(*bytes);
    }

    [[nodiscard]] T value_or(T fallback) const {
        auto current = value();
        return current.has_value() ? std::move(*current) : std::move(fallback);
    }

    void update(const T& value) { backend_->put(name_, serialize(value)); }

    void clear() { backend_->remove(name_); }

private:
    StateBackend* backend_;
    std::string name_;
};

/// An append-only list per key.
template<Serializable T>
class ListState {
public:
    ListState(StateBackend& backend, std::string name)
        : backend_(&backend), name_(std::move(name)) {}

    [[nodiscard]] std::vector<T> get() const {
        const auto bytes = backend_->get(name_);
        if (!bytes.has_value()) {
            return {};
        }
        return deserialize<std::vector<T>>(*bytes);
    }

    /// Deserializes the whole list, appends, and reserializes -- O(n) per
    /// append, so O(n^2) to build a list of n elements.
    ///
    /// Accepted here because the backend interface is deliberately a plain
    /// key-value store, and adding an `append` primitive would push list
    /// semantics down into every future backend. A production backend solves
    /// this with a native append or a merge operator (RocksDB's is exactly
    /// this); ours would too if a benchmark ever showed list state mattering.
    void add(const T& value) {
        std::vector<T> values = get();
        values.push_back(value);
        backend_->put(name_, serialize(values));
    }

    void clear() { backend_->remove(name_); }

private:
    StateBackend* backend_;
    std::string name_;
};

/// A running aggregate per key.
///
/// The memory-efficient option and the one to reach for by default: it keeps one
/// accumulator rather than every contributing record, exactly as window state
/// does (D-029).
template<typename Aggregator>
    requires Serializable<typename Aggregator::AccumulatorType>
class AggregatingState {
public:
    using InputType = typename Aggregator::InputType;
    using AccumulatorType = typename Aggregator::AccumulatorType;
    using OutputType = typename Aggregator::OutputType;

    AggregatingState(StateBackend& backend, std::string name, Aggregator aggregator = {})
        : backend_(&backend), name_(std::move(name)), aggregator_(std::move(aggregator)) {}

    void add(const InputType& value) {
        AccumulatorType accumulator = load_or_create();
        aggregator_.add(accumulator, value);
        backend_->put(name_, serialize(accumulator));
    }

    [[nodiscard]] OutputType get() const { return aggregator_.result(load_or_create()); }

    /// Empty when the key has never been written, so callers can tell "no data"
    /// from "aggregated to the identity value".
    [[nodiscard]] bool empty() const { return !backend_->get(name_).has_value(); }

    void clear() { backend_->remove(name_); }

private:
    [[nodiscard]] AccumulatorType load_or_create() const {
        const auto bytes = backend_->get(name_);
        if (!bytes.has_value()) {
            return aggregator_.create();
        }
        return deserialize<AccumulatorType>(*bytes);
    }

    StateBackend* backend_;
    std::string name_;
    Aggregator aggregator_;
};

} // namespace ripple
