#pragma once

#include <ripple/collector.hpp>
#include <ripple/operator.hpp>
#include <ripple/record.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace ripple {

/// Forwards records whose payload satisfies a predicate; drops the rest.
///
/// Input and output types are identical, which is what makes the zero-copy path
/// work: a record that passes is *moved* straight through untouched, and a
/// record that fails is simply destroyed when `process` returns. Neither path
/// copies the payload.
template<typename T, typename Predicate>
class FilterOperator final : public Operator<T, T> {
public:
    explicit FilterOperator(Predicate predicate, std::string name = "filter")
        : predicate_(std::move(predicate)), name_(std::move(name)) {}

    void process(Record<T>&& record, Collector<T>& out) override {
        // The predicate takes a const reference. It must not consume the
        // payload, because a record that passes still has to be forwarded
        // intact -- and a moved-from payload forwarded downstream is a silent
        // data-loss bug rather than a crash.
        if (predicate_(std::as_const(record.value))) {
            out.collect(std::move(record));
        }
    }

    [[nodiscard]] std::string_view name() const noexcept override { return name_; }

private:
    Predicate predicate_;
    std::string name_;
};

template<typename T, typename Predicate>
[[nodiscard]] auto make_filter(Predicate predicate, std::string name = "filter") {
    return std::make_unique<FilterOperator<T, Predicate>>(std::move(predicate), std::move(name));
}

} // namespace ripple
