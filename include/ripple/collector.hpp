#pragma once

#include <ripple/record.hpp>

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
};

} // namespace ripple
