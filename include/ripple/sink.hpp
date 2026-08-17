#pragma once

#include <ripple/operator.hpp>
#include <ripple/record.hpp>

#include <string_view>
#include <utility>
#include <vector>

namespace ripple {

/// The terminal node of a pipeline. Consumes records and produces nothing.
///
/// Stage 8 will require sinks to be idempotent or transactional for end-to-end
/// exactly-once. Stage 1 asks nothing of them.
template<typename In>
class Sink : public OperatorBase {
public:
    /// See the note on Source::OutputType: the builder deduces the concrete
    /// sink type and reads this alias to recover the payload type.
    using InputType = In;

    virtual void write(Record<In>&& record) = 0;
};

/// Accumulates everything it receives. The primary assertion target for tests:
/// pipeline correctness is checked by comparing this against an expected
/// sequence.
template<typename T>
class CollectingSink final : public Sink<T> {
public:
    void write(Record<T>&& record) override { records_.push_back(std::move(record)); }

    [[nodiscard]] const std::vector<Record<T>>& records() const noexcept { return records_; }

    [[nodiscard]] std::string_view name() const noexcept override { return "collecting-sink"; }

private:
    std::vector<Record<T>> records_;
};

} // namespace ripple
