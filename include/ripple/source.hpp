#pragma once

#include <ripple/collector.hpp>
#include <ripple/operator.hpp>
#include <ripple/record.hpp>

#include <string_view>
#include <utility>
#include <vector>

namespace ripple {

/// Produces records into the pipeline.
///
/// `run` drives the entire pipeline in Stage 1: it is called once, pushes every
/// record it has, and returns when the input is exhausted. Because execution is
/// single-threaded, the call stack at the deepest point runs
/// source -> operator -> operator -> sink, and unwinds once per record.
///
/// Stage 8 will add offset tracking here so that a source can be rewound during
/// recovery. Stage 1 has no notion of an offset.
template<typename Out>
class Source : public OperatorBase {
public:
    /// Lets the pipeline builder recover `Out` from a concrete source type.
    /// Template argument deduction will not look through a
    /// `unique_ptr<Derived>` to find `unique_ptr<Source<Out>>`, so the builder
    /// deduces the concrete type and reads this alias instead.
    using OutputType = Out;

    virtual void run(Collector<Out>& out) = 0;
};

/// A source backed by an in-memory vector. The workhorse for tests, and in
/// Stage 9 the shape a replayable file source will take.
template<typename T>
class VectorSource final : public Source<T> {
public:
    explicit VectorSource(std::vector<Record<T>> records) : records_(std::move(records)) {}

    void run(Collector<T>& out) override {
        for (auto& record : records_) {
            // `records_` owns its elements and we are draining it exactly once,
            // so moving out of each element is safe. The vector is left holding
            // moved-from values, which is why `run` is documented as
            // single-shot: calling it twice would emit empty payloads rather
            // than failing loudly.
            out.collect(std::move(record));
        }
        records_.clear();
    }

    [[nodiscard]] std::string_view name() const noexcept override { return "vector-source"; }

private:
    std::vector<Record<T>> records_;
};

} // namespace ripple
