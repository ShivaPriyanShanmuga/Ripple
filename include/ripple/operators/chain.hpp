#pragma once

#include <ripple/collector.hpp>
#include <ripple/operator.hpp>
#include <ripple/record.hpp>
#include <ripple/serialization.hpp>
#include <ripple/state/key_group.hpp>
#include <ripple/watermark.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace ripple {

/// Runs two operators back to back as one, so a subtask can host a chain rather
/// than a single stage.
///
/// ## Why chaining exists at all
///
/// The obvious alternative is to give each operator its own thread and a queue
/// between them. That is strictly worse for adjacent stages: every record pays a
/// mutex, a condition-variable notify, and a thread handoff to travel a few
/// nanoseconds of actual work. Real engines chain operators into a single task
/// for exactly this reason and only break the chain where the topology forces a
/// shuffle -- a `keyBy`, or a change in parallelism.
///
/// Here it also removes a structural limitation: the parallel runtime hosts one
/// operator per subtask, so without chaining a subtask could not both generate
/// watermarks and window the results.
///
/// ## How it works
///
/// The first operator writes into an adapter that is a `Collector<Mid>`, and the
/// adapter feeds the second operator. Because `Collector` was defined as an
/// interface in Stage 1 (D-014), this needs no cooperation from either operator
/// -- neither knows it is chained. The same seam that let a queue be inserted in
/// Stage 6 lets a whole operator be inserted here.
///
/// Watermarks propagate the same way, which matters: `first` may *react* to a
/// watermark by emitting records (a window firing) and those records must reach
/// `second` before the watermark does. Routing the watermark through the adapter
/// rather than around it is what preserves that ordering.
template<typename In, typename Mid, typename Out>
class ChainedOperator final : public Operator<In, Out> {
public:
    ChainedOperator(std::unique_ptr<Operator<In, Mid>> first,
                    std::unique_ptr<Operator<Mid, Out>> second, std::string name = "chain")
        : first_(std::move(first)), second_(std::move(second)), name_(std::move(name)) {}

    void process(Record<In>&& record, Collector<Out>& out) override {
        Adapter adapter(*second_, out);
        first_->process(std::move(record), adapter);
    }

    void on_watermark(Watermark watermark, Collector<Out>& out) override {
        Adapter adapter(*second_, out);
        // Handed to `first` rather than forwarded directly downstream. If it
        // went straight out, a window in `first` that fires on this watermark
        // would emit records *after* the watermark that already claimed to cover
        // them -- and `second` would treat its own output as late.
        first_->on_watermark(watermark, adapter);
    }

    [[nodiscard]] std::string_view name() const noexcept override { return name_; }

    /// Written and read in the same order, so the pair round-trips. The
    /// full-consumption check in `deserialize` (D-034) catches them drifting.
    void snapshot_state(ByteWriter& writer) const override {
        first_->snapshot_state(writer);
        second_->snapshot_state(writer);
    }

    void restore_state(ByteReader& reader, KeyGroupRange range) override {
        first_->restore_state(reader, range);
        second_->restore_state(reader, range);
    }

    [[nodiscard]] Operator<In, Mid>& first() noexcept { return *first_; }

    [[nodiscard]] Operator<Mid, Out>& second() noexcept { return *second_; }

private:
    /// Bridges the two: everything the first operator emits becomes input to the
    /// second. Constructed per call rather than stored, because it holds a
    /// reference to the *current* downstream collector, which differs between
    /// calls once the runtime is parallel.
    class Adapter final : public Collector<Mid> {
    public:
        Adapter(Operator<Mid, Out>& second, Collector<Out>& out) noexcept
            : second_(&second), out_(&out) {}

        void collect(Record<Mid>&& record) override { second_->process(std::move(record), *out_); }

        void emit_watermark(Watermark watermark) override {
            second_->on_watermark(watermark, *out_);
        }

    private:
        Operator<Mid, Out>* second_;
        Collector<Out>* out_;
    };

    std::unique_ptr<Operator<In, Mid>> first_;
    std::unique_ptr<Operator<Mid, Out>> second_;
    std::string name_;
};

template<typename In, typename Mid, typename Out>
[[nodiscard]] auto make_chain(std::unique_ptr<Operator<In, Mid>> first,
                              std::unique_ptr<Operator<Mid, Out>> second,
                              std::string name = "chain") {
    return std::make_unique<ChainedOperator<In, Mid, Out>>(std::move(first), std::move(second),
                                                           std::move(name));
}

} // namespace ripple
