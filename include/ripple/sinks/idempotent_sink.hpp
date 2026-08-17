#pragma once

#include <ripple/record.hpp>
#include <ripple/sink.hpp>

#include <cstddef>
#include <map>
#include <string_view>
#include <type_traits>
#include <utility>

namespace ripple {

/// A sink whose writes are **upserts keyed by a selector**, so writing the same
/// logical result twice has the same effect as writing it once.
///
/// ## Why this exists, and what "exactly-once" actually means
///
/// Exactly-once does **not** mean each record is delivered once. After a failure
/// records are absolutely re-sent -- replay is how recovery works. It means the
/// *effect on state* is as if each record were processed exactly once.
///
/// Recovery restores operator state to a checkpoint and rewinds the source to
/// the offset recorded in that same checkpoint, so replayed records are applied
/// to state that no longer contains them. State ends up identical to a run with
/// no failure. But the sink already received the pre-crash writes, and will
/// receive them again.
///
/// So the end-to-end property needs three things, and the engine only supplies
/// one of them:
///   1. a **replayable source** -- one that can be rewound to a position. A file
///      or a Kafka topic can; a UDP socket cannot, and no amount of engine
///      correctness recovers data that is simply gone;
///   2. **consistent snapshots** -- Stage 7;
///   3. an **idempotent or transactional sink** -- this class, or a two-phase
///      commit.
///
/// Miss the third and a perfect engine still double-counts in the outside world:
/// writing "$500 revenue for Midtown" twice into a database that *adds* is a real
/// error no checkpoint can undo.
///
/// **Why real sinks use two-phase commit.** Upserting works when results are
/// keyed and replacing is meaningful. When they are not -- appending rows,
/// sending emails, charging cards -- the sink instead writes into an uncommitted
/// transaction and commits only once the checkpoint containing those writes is
/// confirmed complete. Pre-commit on barrier, commit on checkpoint-complete
/// notification: the same two phases, driven by the checkpoint protocol.
template<typename T, typename KeySelector>
class IdempotentSink final : public Sink<T> {
public:
    using KeyType = std::decay_t<std::invoke_result_t<KeySelector&, const T&>>;

    explicit IdempotentSink(KeySelector selector) : selector_(std::move(selector)) {}

    // The payload is moved out of `record`; the check does not track moves of
    // subobjects.
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    void write(Record<T>&& record) override {
        ++write_count_;
        KeyType key = selector_(record.value);
        values_.insert_or_assign(std::move(key), std::move(record.value));
    }

    /// The deduplicated effect: one value per key, whatever was written last.
    [[nodiscard]] const std::map<KeyType, T>& values() const noexcept { return values_; }

    /// Total writes received, replays included.
    ///
    /// The gap between this and `values().size()` is the point: delivery is
    /// at-least-once and visibly so, while the effect is exactly-once.
    [[nodiscard]] std::size_t write_count() const noexcept { return write_count_; }

    [[nodiscard]] std::string_view name() const noexcept override { return "idempotent-sink"; }

private:
    KeySelector selector_;
    std::map<KeyType, T> values_;
    std::size_t write_count_ = 0;
};

template<typename T, typename KeySelector>
[[nodiscard]] auto make_idempotent_sink(KeySelector selector) {
    return IdempotentSink<T, KeySelector>(std::move(selector));
}

} // namespace ripple
