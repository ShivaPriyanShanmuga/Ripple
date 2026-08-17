#pragma once

#include <ripple/collector.hpp>
#include <ripple/record.hpp>
#include <ripple/serialization.hpp>
#include <ripple/watermark.hpp>

#include <string_view>

namespace ripple {

/// The type-erased half of the operator design.
///
/// A pipeline holds `std::vector<std::unique_ptr<OperatorBase>>`. That is what
/// gives us two things a compile-time operator chain cannot provide:
///   - runtime topology: the DAG can be built from configuration, not baked
///     into a type;
///   - traversal: Stage 7 must walk every operator and tell it to snapshot.
///
/// Everything type-dependent stays in the derived `Operator<In, Out>`, so the
/// erasure costs us no type safety on the data path.
class OperatorBase {
public:
    OperatorBase() = default;
    virtual ~OperatorBase() = default;

    OperatorBase(const OperatorBase&) = delete;
    OperatorBase& operator=(const OperatorBase&) = delete;
    OperatorBase(OperatorBase&&) = delete;
    OperatorBase& operator=(OperatorBase&&) = delete;

    /// Used in diagnostics and, from Stage 6, in instrumentation output.
    [[nodiscard]] virtual std::string_view name() const noexcept = 0;

    /// ## Operator state, designed here for Stage 7
    ///
    /// Whatever this operator holds that must survive a failure. Distinct from
    /// the *keyed* state in `StateBackend`: this is the per-operator kind -- a
    /// source's read offset, a window operator's open windows, a watermark
    /// generator's high-water mark.
    ///
    /// Defaulted to no-ops because most operators are stateless. Map and filter
    /// hold nothing between records and so need to say nothing about
    /// checkpointing; only operators with something to lose implement these.
    ///
    /// Stage 7 adds barriers, alignment, and a coordinator that *call* these.
    /// It does not change them, which is the whole point of defining them now:
    /// retrofitting a state interface across every operator once checkpointing
    /// exists is the expensive version of this work.
    virtual void snapshot_state(ByteWriter& /*writer*/) const {}

    /// Must restore exactly what `snapshot_state` wrote. The two are a matched
    /// pair, and `deserialize`'s full-consumption check is what catches them
    /// drifting apart.
    virtual void restore_state(ByteReader& /*reader*/) {}
};

/// A transformation from `Record<In>` to zero or more `Record<Out>`.
///
/// Note the shape: `process` does not *return* an output. It is handed a
/// collector and may call it any number of times -- zero for a filter that
/// drops, once for a map, many for a future flat-map or window firing. A
/// return-value interface could not express those cases uniformly.
///
/// This is also why the engine pushes rather than pulls: an operator is a plain
/// function call that runs to completion. A pull interface would have to be
/// resumable, which means every operator becomes a coroutine or a hand-written
/// state machine.
template<typename In, typename Out>
class Operator : public OperatorBase {
public:
    using InputType = In;
    using OutputType = Out;

    /// Consumes `record`. The `&&` states that ownership transfers in: the
    /// caller has given up the record and will not read it again.
    virtual void process(Record<In>&& record, Collector<Out>& out) = 0;

    /// Reacts to event-time progress.
    ///
    /// The default -- forward it unchanged -- is correct for the majority of
    /// operators. Map and filter have no notion of time and should not have to
    /// mention watermarks at all; that is why this is a separate virtual with a
    /// default body rather than a variant every operator must dispatch on.
    ///
    /// Stage 3's window operator overrides this: a watermark passing a window's
    /// end is exactly the signal to fire that window, emit its result, and free
    /// its state. An override MUST still forward the watermark afterwards, or
    /// event time stops advancing for everything downstream.
    virtual void on_watermark(Watermark watermark, Collector<Out>& out) {
        out.emit_watermark(watermark);
    }
};

} // namespace ripple
