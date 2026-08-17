#pragma once

#include <cstddef>
#include <utility>
#include <vector>

namespace ripple {

/// An aggregator collapses the records in a window into a single result,
/// incrementally.
///
/// The shape is deliberate: `add` folds one input into an accumulator, so a
/// window holds **one accumulator** rather than every record it has seen. For a
/// sum that is 8 bytes instead of the whole window's worth of data, which is the
/// difference between a windowed job that runs indefinitely and one that grows
/// until it is killed.
///
/// `merge` is required only by merging assigners (sessions). Requiring it forces
/// the aggregation to be associative, which is exactly the property that makes
/// merging well-defined.
///
/// An aggregator must provide:
///     using InputType / AccumulatorType / OutputType
///     AccumulatorType create() const
///     void add(AccumulatorType&, const InputType&) const
///     OutputType result(const AccumulatorType&) const
///     AccumulatorType merge(AccumulatorType, AccumulatorType) const
///
/// Expressed as a duck-typed template parameter rather than a virtual interface:
/// `add` is called once per record per window, it is the hottest thing in a
/// windowed pipeline, and unlike the operator graph there is no requirement to
/// swap aggregators at runtime from configuration.

template<typename T>
struct SumAggregator {
    using InputType = T;
    using AccumulatorType = T;
    using OutputType = T;

    [[nodiscard]] AccumulatorType create() const { return AccumulatorType{}; }

    void add(AccumulatorType& accumulator, const InputType& value) const { accumulator += value; }

    [[nodiscard]] OutputType result(const AccumulatorType& accumulator) const {
        return accumulator;
    }

    [[nodiscard]] AccumulatorType merge(AccumulatorType left, AccumulatorType right) const {
        return left + right;
    }
};

template<typename T>
struct CountAggregator {
    using InputType = T;
    using AccumulatorType = std::size_t;
    using OutputType = std::size_t;

    [[nodiscard]] AccumulatorType create() const { return 0; }

    void add(AccumulatorType& accumulator, const InputType& /*value*/) const { ++accumulator; }

    [[nodiscard]] OutputType result(const AccumulatorType& accumulator) const {
        return accumulator;
    }

    [[nodiscard]] AccumulatorType merge(AccumulatorType left, AccumulatorType right) const {
        return left + right;
    }
};

/// Retains every record in the window.
///
/// Useful for tests and for windows that genuinely need the raw contents, but
/// note that it forfeits the memory advantage described above: state grows with
/// the number of records rather than staying constant. Reach for it knowingly.
template<typename T>
struct CollectAggregator {
    using InputType = T;
    using AccumulatorType = std::vector<T>;
    using OutputType = std::vector<T>;

    [[nodiscard]] AccumulatorType create() const { return {}; }

    void add(AccumulatorType& accumulator, const InputType& value) const {
        accumulator.push_back(value);
    }

    [[nodiscard]] OutputType result(const AccumulatorType& accumulator) const {
        return accumulator;
    }

    [[nodiscard]] AccumulatorType merge(AccumulatorType left, AccumulatorType right) const {
        left.insert(left.end(), std::make_move_iterator(right.begin()),
                    std::make_move_iterator(right.end()));
        return left;
    }
};

} // namespace ripple
