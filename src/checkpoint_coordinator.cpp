#include <ripple/checkpoint/checkpoint_coordinator.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace ripple {

CheckpointId CheckpointCoordinator::trigger(std::size_t source_offset, std::size_t parallelism) {
    const std::lock_guard<std::mutex> lock(mutex_);
    const CheckpointId id = next_id_++;
    pending_[id] = CompletedCheckpoint{id, source_offset, parallelism, {}, {}};
    started_[id] = std::chrono::steady_clock::now();
    return id;
}

void CheckpointCoordinator::acknowledge(CheckpointId id, TaskId task,
                                        std::vector<std::byte> state) {
    const std::lock_guard<std::mutex> lock(mutex_);

    const auto entry = pending_.find(id);
    if (entry == pending_.end()) {
        // Either already complete or abandoned. Ignoring a late acknowledgement
        // is correct rather than merely tolerant: a checkpoint that has already
        // been declared complete must not be mutated afterwards, or its contents
        // would no longer match the cut it was taken at.
        return;
    }

    entry->second.task_state.insert_or_assign(task, std::move(state));

    if (entry->second.task_state.size() < task_count_) {
        return;
    }

    // Every task has reported. Only now does this checkpoint become usable --
    // a partial checkpoint is not partially useful, it is corrupt, because the
    // set of snapshots would not describe a single consistent cut.
    entry->second.duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - started_[id]);
    started_.erase(id);
    completed_.push_back(std::move(entry->second));
    pending_.erase(entry);
}

bool CheckpointCoordinator::is_complete(CheckpointId id) const {
    const std::lock_guard<std::mutex> lock(mutex_);
    return std::any_of(completed_.begin(), completed_.end(),
                       [id](const CompletedCheckpoint& checkpoint) { return checkpoint.id == id; });
}

std::optional<CompletedCheckpoint> CheckpointCoordinator::latest_completed() const {
    const std::lock_guard<std::mutex> lock(mutex_);
    if (completed_.empty()) {
        return std::nullopt;
    }
    return completed_.back();
}

std::vector<CompletedCheckpoint> CheckpointCoordinator::completed() const {
    const std::lock_guard<std::mutex> lock(mutex_);
    return completed_;
}

std::size_t CheckpointCoordinator::completed_count() const {
    const std::lock_guard<std::mutex> lock(mutex_);
    return completed_.size();
}

std::size_t CheckpointCoordinator::pending_count() const {
    const std::lock_guard<std::mutex> lock(mutex_);
    return pending_.size();
}

} // namespace ripple
