#pragma once

#include <ripple/record.hpp>
#include <ripple/watermark.hpp>

namespace ripple {

/// Non-template base so that collectors of different payload types can be owned
/// together in one container. It has no behaviour: its entire job is to give
/// `std::unique_ptr` a virtual destructor to call, which is what makes
/// polymorphic ownership safe.
class CollectorBase {
public:
    CollectorBase() = default;
    virtual ~CollectorBase() = default;

    // Rule of five, resolved by deleting rather than defining.
    //
    // A polymorphic base with a public copy constructor invites slicing: copying
    // a `Collector<T>&` that actually refers to a `SinkCollector<T>` would
    // silently construct a base-only object and discard the behaviour. Nothing
    // in the design ever needs to copy or move a collector -- they are created
    // once by the pipeline builder and live at a stable heap address -- so the
    // safe answer is to delete all four.
    CollectorBase(const CollectorBase&) = delete;
    CollectorBase& operator=(const CollectorBase&) = delete;
    CollectorBase(CollectorBase&&) = delete;
    CollectorBase& operator=(CollectorBase&&) = delete;
};

/// Where an operator sends its output.
///
/// This interface is the seam that makes Stage 6 possible without touching
/// operator code. In Stage 1 the implementation calls the next operator
/// directly on the same thread. In Stage 6 it will push into a bounded queue
/// drained by another thread. An operator only ever sees `Collector<T>&` and
/// cannot tell the difference.
template<typename T>
class Collector : public CollectorBase {
public:
    /// Takes ownership of the record. The rvalue reference is the contract:
    /// after calling this, the caller must not read the record again.
    virtual void collect(Record<T>&& record) = 0;

    /// Sends a watermark down the same path, in the same order, as records.
    ///
    /// This ordering is the entire mechanism. Because a watermark travels
    /// through the identical channel as the records ahead of it and cannot
    /// overtake them, an operator receiving watermark T knows every record with
    /// event time <= T has already passed through *it specifically*. No global
    /// clock, no coordination, no central authority -- each operator infers
    /// event-time progress from its own input alone.
    ///
    /// A separate side-channel for watermarks would destroy this. The watermark
    /// could then arrive before records it claims to cover, and a window would
    /// fire while its own data was still in flight.
    virtual void emit_watermark(Watermark watermark) = 0;
};

} // namespace ripple
