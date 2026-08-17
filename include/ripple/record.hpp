#pragma once

#include <ripple/timestamp.hpp>

#include <type_traits>
#include <utility>

namespace ripple {

/// A unit of data flowing through the pipeline: a payload plus the event time
/// at which it happened.
///
/// Deliberately an aggregate with no user-declared special member functions --
/// the rule of zero. The compiler generates copy, move, and destruction, and
/// each is correct by construction. Declaring any one of them by hand would
/// suppress or complicate the others for no gain.
///
/// `T` is unconstrained on purpose. There is no central registry of payload
/// types, no variant listing them all, and no base class they must inherit.
/// A payload is whatever the user's operator produces, sized exactly as itself.
template<typename T>
struct Record {
    T value;
    Timestamp event_time{};
};

/// Convenience constructor that deduces `T`. Takes the value by value and moves
/// it in: callers passing an rvalue get a move, callers passing an lvalue get
/// one copy, and there is no overload set to maintain.
template<typename T>
[[nodiscard]] Record<std::decay_t<T>> make_record(T&& value, Timestamp event_time) {
    return Record<std::decay_t<T>>{std::forward<T>(value), event_time};
}

} // namespace ripple
