#pragma once

#include <ripple/collector.hpp>
#include <ripple/operator.hpp>
#include <ripple/record.hpp>
#include <ripple/sink.hpp>
#include <ripple/source.hpp>
#include <ripple/watermark.hpp>

#include <cassert>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace ripple {

namespace detail {

/// The wiring node between two operators: receives a record, hands it to its
/// operator, and tells that operator where to send its output.
///
/// `downstream_` starts null and is filled in later by the builder. That is
/// what allows the pipeline to be described front-to-back in source order while
/// the connections are inherently back-to-front -- an operator cannot be given
/// its successor until the successor exists.
template<typename In, typename Out>
class OperatorCollector final : public Collector<In> {
public:
    explicit OperatorCollector(Operator<In, Out>& op) noexcept : operator_(&op) {}

    void collect(Record<In>&& record) override {
        assert(downstream_ != nullptr && "pipeline was not fully wired");
        operator_->process(std::move(record), *downstream_);
    }

    void emit_watermark(Watermark watermark) override {
        assert(downstream_ != nullptr && "pipeline was not fully wired");
        operator_->on_watermark(watermark, *downstream_);
    }

    [[nodiscard]] Collector<Out>** downstream_slot() noexcept { return &downstream_; }

private:
    Operator<In, Out>* operator_;
    Collector<Out>* downstream_ = nullptr;
};

/// Terminal wiring node. Has no downstream slot, which is what makes a sink a
/// sink.
template<typename In>
class SinkCollector final : public Collector<In> {
public:
    explicit SinkCollector(Sink<In>& sink) noexcept : sink_(&sink) {}

    void collect(Record<In>&& record) override { sink_->write(std::move(record)); }

    void emit_watermark(Watermark watermark) override { sink_->on_watermark(watermark); }

private:
    Sink<In>* sink_;
};

/// Erases the source's payload type so `Pipeline` can be a plain non-template
/// class. Without this, `Pipeline` would have to be templated on the source
/// type, which would make it impossible to hold pipelines of different shapes
/// in one container -- the exact problem we set out to avoid.
class SourceRunner {
public:
    SourceRunner() = default;
    virtual ~SourceRunner() = default;

    SourceRunner(const SourceRunner&) = delete;
    SourceRunner& operator=(const SourceRunner&) = delete;
    SourceRunner(SourceRunner&&) = delete;
    SourceRunner& operator=(SourceRunner&&) = delete;

    virtual void run() = 0;
};

template<typename Out>
class TypedSourceRunner final : public SourceRunner {
public:
    explicit TypedSourceRunner(Source<Out>& source) noexcept : source_(&source) {}

    void run() override {
        assert(head_ != nullptr && "pipeline has no sink");
        source_->run(*head_);

        // The input is exhausted, so nothing further can ever arrive. Emitting
        // a watermark at the maximum timestamp forces every window still open
        // downstream to fire.
        //
        // Without this, a finite job would silently drop its final windows:
        // records would have been consumed, results would look plausible, and
        // the last few minutes of every run would simply be missing. The
        // failure is quiet and only visible if you know the expected totals.
        head_->emit_watermark(kEndOfStreamWatermark);
    }

    [[nodiscard]] Collector<Out>** head_slot() noexcept { return &head_; }

private:
    Source<Out>* source_;
    Collector<Out>* head_ = nullptr;
};

} // namespace detail

/// A wired, runnable dataflow graph.
///
/// Owns every operator and every wiring node. Note that `operators_` holds the
/// source and the sink too: both are `OperatorBase`, and Stage 7 will need to
/// snapshot all three kinds uniformly.
class Pipeline {
public:
    /// Runs to completion. Single-threaded in Stage 1: the source pushes each
    /// record all the way to the sink before producing the next one.
    void run();

    [[nodiscard]] std::size_t operator_count() const noexcept { return operators_.size(); }

private:
    template<typename Out>
    friend class PipelineBuilder;

    Pipeline(std::vector<std::unique_ptr<OperatorBase>> operators,
             std::vector<std::unique_ptr<CollectorBase>> collectors,
             std::unique_ptr<detail::SourceRunner> runner) noexcept
        : operators_(std::move(operators)), collectors_(std::move(collectors)),
          runner_(std::move(runner)) {}

    // All three members are move-only, so Pipeline is move-only by the rule of
    // zero: no special members declared, all four generated correctly.
    std::vector<std::unique_ptr<OperatorBase>> operators_;
    std::vector<std::unique_ptr<CollectorBase>> collectors_;
    std::unique_ptr<detail::SourceRunner> runner_;
};

template<typename Out>
class PipelineBuilder;

template<typename SourceT>
[[nodiscard]] auto from(std::unique_ptr<SourceT> source);

/// Assembles a pipeline, carrying the current stage's output type in `Out` so
/// that mismatched connections fail at compile time.
///
/// Every method is `&&`-qualified: a builder is consumed by use. That prevents
/// reusing a builder after `via`, which would produce two operators both
/// claiming the same predecessor slot and silently drop a branch of the graph.
template<typename Out>
class PipelineBuilder {
public:
    /// Appends an operator.
    ///
    /// Deduced on the *concrete* operator type rather than on
    /// `Operator<Out, NewOut>`, because template argument deduction does not
    /// look through `unique_ptr<Derived>` to find `unique_ptr<Base>` -- a
    /// derived-to-base conversion is available only after deduction succeeds,
    /// and deduction needs an exact match. So we deduce `OperatorT` exactly and
    /// recover the payload types from its member aliases.
    ///
    /// The static_assert turns a mis-wired pipeline into one readable line
    /// instead of a page of overload-resolution failures.
    template<typename OperatorT>
    [[nodiscard]] auto via(std::unique_ptr<OperatorT> op) && {
        using NewOut = typename OperatorT::OutputType;
        static_assert(std::is_same_v<typename OperatorT::InputType, Out>,
                      "operator input type does not match the previous stage's output type");

        Operator<Out, NewOut>& op_ref = *op;
        operators_.push_back(std::move(op));

        auto collector = std::make_unique<detail::OperatorCollector<Out, NewOut>>(op_ref);
        auto* collector_ptr = collector.get();
        collectors_.push_back(std::move(collector));

        // Connect the previous stage to this one.
        *slot_ = collector_ptr;

        // The next stage will fill this operator's downstream slot.
        //
        // Taking a pointer into a heap object owned by `collectors_` is safe
        // across the vector growing, because the vector stores unique_ptrs, not
        // the objects themselves: reallocation moves pointers and leaves the
        // pointed-to objects at fixed addresses. Storing collectors by value
        // would make every one of these slot pointers dangle on the next
        // push_back.
        return PipelineBuilder<NewOut>(std::move(operators_), std::move(collectors_),
                                       std::move(runner_), collector_ptr->downstream_slot());
    }

    /// Terminates the pipeline. Deduced on the concrete sink type for the same
    /// reason as `via`.
    template<typename SinkT>
    [[nodiscard]] Pipeline to(std::unique_ptr<SinkT> sink) && {
        static_assert(std::is_same_v<typename SinkT::InputType, Out>,
                      "sink input type does not match the final stage's output type");

        Sink<Out>& sink_ref = *sink;
        operators_.push_back(std::move(sink));

        auto collector = std::make_unique<detail::SinkCollector<Out>>(sink_ref);
        *slot_ = collector.get();
        collectors_.push_back(std::move(collector));

        return Pipeline(std::move(operators_), std::move(collectors_), std::move(runner_));
    }

private:
    template<typename>
    friend class PipelineBuilder;

    template<typename SourceT>
    friend auto from(std::unique_ptr<SourceT> source);

    PipelineBuilder(std::vector<std::unique_ptr<OperatorBase>> operators,
                    std::vector<std::unique_ptr<CollectorBase>> collectors,
                    std::unique_ptr<detail::SourceRunner> runner, Collector<Out>** slot) noexcept
        : operators_(std::move(operators)), collectors_(std::move(collectors)),
          runner_(std::move(runner)), slot_(slot) {}

    std::vector<std::unique_ptr<OperatorBase>> operators_;
    std::vector<std::unique_ptr<CollectorBase>> collectors_;
    std::unique_ptr<detail::SourceRunner> runner_;

    /// Address of the not-yet-filled `Collector<Out>*` in the previous stage.
    Collector<Out>** slot_;
};

/// Entry point:
///     ripple::from(std::move(source)).via(std::move(op)).to(std::move(sink))
template<typename SourceT>
[[nodiscard]] auto from(std::unique_ptr<SourceT> source) {
    using Out = typename SourceT::OutputType;
    Source<Out>& source_ref = *source;

    std::vector<std::unique_ptr<OperatorBase>> operators;
    operators.push_back(std::move(source));

    auto runner = std::make_unique<detail::TypedSourceRunner<Out>>(source_ref);

    // Read the slot address before moving the unique_ptr. Moving a unique_ptr
    // transfers the pointer, not the pointee, so the address stays valid -- but
    // evaluation order inside the call expression below is unspecified, so the
    // read must happen in its own statement.
    Collector<Out>** slot = runner->head_slot();

    return PipelineBuilder<Out>(std::move(operators), {}, std::move(runner), slot);
}

} // namespace ripple
