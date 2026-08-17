#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <vector>

namespace ripple {

using CheckpointId = std::int64_t;
using TaskId = std::size_t;

/// A checkpoint every task has acknowledged. Only these are safe to recover
/// from.
struct CompletedCheckpoint {
    CheckpointId id = 0;

    /// How far the source had read when it injected the barrier.
    ///
    /// Without this a checkpoint is useless: restoring operator state to an
    /// earlier point while the source carries on from wherever it happens to be
    /// would skip every record in between. State and offset are one atomic fact
    /// about the same cut through the stream, which is why they live in the same
    /// record.
    std::size_t source_offset = 0;

    /// Parallelism of the run that produced this checkpoint.
    ///
    /// Needed on restore for two reasons: task ids `[0, parallelism)` are
    /// subtasks and `parallelism` itself is the sink, and a restore at a
    /// different parallelism is a **rescale**, which redistributes key groups
    /// rather than restoring blob-for-blob.
    std::size_t parallelism = 0;

    /// Serialized state per task. Ordered so a checkpoint's contents are
    /// deterministic and comparable, for the same reason the state backend uses
    /// ordered maps (D-037).
    std::map<TaskId, std::vector<std::byte>> task_state;
};

/// Triggers checkpoints, collects acknowledgements, and declares a checkpoint
/// complete once every task has reported.
///
/// ## The only out-of-band component in the system
///
/// Everything else -- records, watermarks, barriers -- travels through the data
/// path. The coordinator is the single exception, and it is deliberately tiny:
/// it starts checkpoints and counts acknowledgements, and does nothing else. The
/// default answer to "should the coordinator also do X?" is no, put X in the
/// stream.
///
/// ## Why a checkpoint is not usable until *every* task acknowledges
///
/// A partial checkpoint is not a partially useful checkpoint, it is a corrupt
/// one. Restoring from a set of snapshots where task 3 never reported would
/// reset tasks 0-2 to the cut while task 3 kept state from some later point --
/// the cut would not be a cut at all, and the "exactly once" property would be
/// silently untrue. So `latest_completed()` only ever exposes a fully
/// acknowledged checkpoint, and incomplete ones are simply discarded.
class CheckpointCoordinator {
public:
    explicit CheckpointCoordinator(std::size_t task_count) : task_count_(task_count) {}

    CheckpointCoordinator(const CheckpointCoordinator&) = delete;
    CheckpointCoordinator& operator=(const CheckpointCoordinator&) = delete;
    CheckpointCoordinator(CheckpointCoordinator&&) = delete;
    CheckpointCoordinator& operator=(CheckpointCoordinator&&) = delete;
    ~CheckpointCoordinator() = default;

    /// Starts a new checkpoint and returns its id. Called by the source, which
    /// then injects a barrier carrying that id.
    [[nodiscard]] CheckpointId trigger(std::size_t source_offset, std::size_t parallelism);

    /// Records one task's snapshot. Called from that task's own thread, so this
    /// is the one place in the checkpointing path that needs a lock.
    void acknowledge(CheckpointId id, TaskId task, std::vector<std::byte> state);

    [[nodiscard]] bool is_complete(CheckpointId id) const;

    /// The most recent fully acknowledged checkpoint, if any. This is what
    /// Stage 8 recovers from.
    [[nodiscard]] std::optional<CompletedCheckpoint> latest_completed() const;

    [[nodiscard]] std::size_t completed_count() const;

    /// Every completed checkpoint, oldest first.
    ///
    /// Retained without bound, which is fine at this project's scale and is not
    /// what a production system does: real engines keep the last N and delete
    /// older ones, since a checkpoint's whole value is being the *most recent*
    /// consistent cut. Recovery only ever uses the newest.
    [[nodiscard]] std::vector<CompletedCheckpoint> completed() const;

    /// Checkpoints started but not yet fully acknowledged. A number that only
    /// grows means tasks are failing to acknowledge -- the checkpointing
    /// equivalent of a stalled watermark.
    [[nodiscard]] std::size_t pending_count() const;

    [[nodiscard]] std::size_t task_count() const noexcept { return task_count_; }

private:
    mutable std::mutex mutex_;
    std::size_t task_count_;
    CheckpointId next_id_ = 1;
    std::map<CheckpointId, CompletedCheckpoint> pending_;
    std::vector<CompletedCheckpoint> completed_;
};

} // namespace ripple
