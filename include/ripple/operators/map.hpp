#pragma once

#include <ripple/collector.hpp>
#include <ripple/operator.hpp>
#include <ripple/record.hpp>
#include <ripple/timestamp.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace ripple {

/// Applies a function to every payload, one record in, one record out.
///
/// `Fn` is a template parameter rather than a `std::function` so that the
/// user's callable is stored by value with no indirection and no allocation.
/// The one virtual call per record is at the operator boundary, where we
/// accepted it; there is no reason to add a second one inside.
template<typename In, typename Out, typename Fn>
class MapOperator final : public Operator<In, Out> {
public:
    explicit MapOperator(Fn function, std::string name = "map")
        : function_(std::move(function)), name_(std::move(name)) {}

    // clang-tidy wants to see `std::move(record)`. We move `record.value`
    // instead, which consumes the parameter just as thoroughly -- the check
    // simply does not track moves of subobjects. Moving the whole Record into a
    // local first would satisfy the check and add a pointless move per record.
    //
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    void process(Record<In>&& record, Collector<Out>& out) override {
        // Read the timestamp BEFORE moving the payload.
        //
        // Moving `record.value` leaves that member valid-but-unspecified while
        // `record.event_time` is untouched, so reading it afterwards would in
        // fact be safe here. We copy it out first anyway because the safe
        // version and the unsafe version look identical, and the habit of
        // "extract what you need, then move" is what prevents the genuine
        // use-after-move that appears the moment someone reorders these lines.
        const Timestamp event_time = record.event_time;
        out.collect(Record<Out>{function_(std::move(record.value)), event_time});
    }

    [[nodiscard]] std::string_view name() const noexcept override { return name_; }

private:
    Fn function_;
    std::string name_;
};

/// Deduces `Out` from what `Fn` returns when applied to `In`, so callers write
/// `make_map<Trip>(to_fare)` rather than spelling out all three types.
template<typename In, typename Fn>
[[nodiscard]] auto make_map(Fn function, std::string name = "map") {
    using Out = std::decay_t<std::invoke_result_t<Fn&, In&&>>;
    return std::make_unique<MapOperator<In, Out, Fn>>(std::move(function), std::move(name));
}

} // namespace ripple
