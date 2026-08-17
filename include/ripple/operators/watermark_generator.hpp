#pragma once

#include <ripple/collector.hpp>
#include <ripple/operator.hpp>
#include <ripple/record.hpp>
#include <ripple/timestamp.hpp>
#include <ripple/watermark.hpp>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace ripple {

/// Generates watermarks using the bounded-out-of-orderness strategy.
///
/// The strategy in one line: assume no record's event time is ever more than
/// `max_out_of_orderness` behind the highest event time seen so far. Given that
/// assumption, once the highest event time observed is T, it is safe to assert
/// a watermark of `T - max_out_of_orderness`.
///
/// Note what "out of orderness" actually measures, because the name misleads.
/// It is not a claim that any timestamp is wrong or late -- an event time is
/// stamped when the thing happened and is never wrong. It measures how *jumbled
/// the arrival stream is*: how far behind the newest event time an arriving
/// record's event time may be. Records arriving 09:34:50, 09:35:00, 09:34:40 are
/// all correctly stamped; the third is merely out of order by 20 seconds.
///
/// Choosing the bound is a business decision, not an engineering one. A large
/// bound means few late records and complete results, at the cost of every
/// answer arriving that much later. A small bound means fast answers and more
/// records missing their window. There is no correct value -- it is a dial
/// between latency and completeness.
///
/// Implemented as an operator rather than as logic inside `Source` so the
/// strategy is pluggable and independently testable, and so sources stay
/// concerned only with producing records. Logically it still belongs "at the
/// source": it is placed immediately after one.
template<typename T>
class BoundedOutOfOrdernessWatermarks final : public Operator<T, T> {
public:
    explicit BoundedOutOfOrdernessWatermarks(Duration max_out_of_orderness,
                                             std::string name = "watermarks")
        : max_out_of_orderness_(max_out_of_orderness), name_(std::move(name)) {}

    void process(Record<T>&& record, Collector<T>& out) override {
        const Timestamp event_time = record.event_time;
        const Timestamp max_seen =
            max_event_time_.has_value() ? std::max(*max_event_time_, event_time) : event_time;
        max_event_time_ = max_seen;

        // Forward the record BEFORE any watermark derived from it.
        //
        // This ordering is load-bearing. Suppose we emitted the watermark first:
        // this record's own event time can equal the new watermark, so a
        // downstream window would see "everything up to T has arrived", fire,
        // emit its result and free its state -- and only then receive a record
        // belonging to the window it just closed. The record is now late
        // against a watermark that its own arrival produced.
        //
        // The bug is invisible in aggregate: counts stay plausible, no error is
        // raised, and only the boundary records of each window quietly land in
        // the wrong place.
        out.collect(std::move(record));

        const Timestamp candidate = max_seen - max_out_of_orderness_;
        if (candidate > last_emitted_.timestamp) {
            last_emitted_ = Watermark{candidate};
            out.emit_watermark(last_emitted_);
        }
        // Emitting only on advance is what keeps watermarks monotonic. A
        // regressing watermark would re-open a window that had already fired
        // and produce a second, contradictory result for a period that was
        // supposed to be complete.
    }

    [[nodiscard]] std::string_view name() const noexcept override { return name_; }

    /// The watermark most recently emitted. Exposed for tests and, from
    /// Stage 6, for instrumentation.
    [[nodiscard]] Watermark current_watermark() const noexcept { return last_emitted_; }

private:
    Duration max_out_of_orderness_;
    std::string name_;

    /// Optional rather than initialised to kMinTimestamp: subtracting the delay
    /// from the minimum representable timestamp would overflow.
    std::optional<Timestamp> max_event_time_;
    Watermark last_emitted_{kMinTimestamp};
};

template<typename T>
[[nodiscard]] auto make_bounded_out_of_orderness_watermarks(Duration max_out_of_orderness,
                                                            std::string name = "watermarks") {
    return std::make_unique<BoundedOutOfOrdernessWatermarks<T>>(max_out_of_orderness,
                                                                std::move(name));
}

} // namespace ripple
