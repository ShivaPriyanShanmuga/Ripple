#pragma once

#include <ripple/serialization.hpp>
#include <ripple/state/memory_state_backend.hpp>
#include <ripple/state/state_backend.hpp>

#include <cstddef>
#include <filesystem>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace ripple {

/// State held in memory, persisted to a file on demand.
///
/// ## Why not write through on every `put`
///
/// The obvious file-backed design -- open the file, seek, write, fsync, on every
/// state update -- turns a per-record operation into a syscall and a disk flush.
/// A pipeline processing 100k records/sec would issue 100k fsyncs/sec and manage
/// perhaps a few hundred. It is not a slow implementation of the right idea; it
/// is the wrong idea.
///
/// So this backend batches: reads and writes hit memory, and `flush()` writes
/// the whole snapshot at once. That is the same shape real engines use, and it
/// is exactly what makes it compatible with checkpointing -- a checkpoint is
/// already a "persist everything at a consistent point" operation, so persisting
/// at any other time buys nothing.
///
/// ## What a production backend would do differently
///
/// Rewriting the entire state on every flush is O(total state), which is fine
/// for a checkpoint every few seconds and hopeless for very large state. RocksDB
/// -- what Flink actually uses -- is an LSM tree: writes append to a log, and
/// checkpoints capture immutable SST files incrementally, so a checkpoint costs
/// O(changed state) rather than O(all state). Building that is a project of its
/// own and is explicitly out of scope; the interface, however, does not have to
/// change to accommodate it, which is the point of having one.
class FileStateBackend final : public StateBackend {
public:
    /// Does not read the file. Call `load()` explicitly, so that starting fresh
    /// and restoring are visibly different decisions at the call site rather
    /// than a side effect of construction.
    explicit FileStateBackend(std::filesystem::path path);

    void set_current_key(StateKey key) override;
    [[nodiscard]] const StateKey& current_key() const noexcept override;
    [[nodiscard]] std::optional<std::span<const std::byte>>
    get(std::string_view state_name) const override;
    void put(std::string_view state_name, std::vector<std::byte> value) override;
    void remove(std::string_view state_name) override;
    [[nodiscard]] std::size_t key_count() const noexcept override;
    void clear() override;
    void write_snapshot(ByteWriter& writer) const override;
    void restore_snapshot(ByteReader& reader) override;

    /// Writes all state to the file.
    ///
    /// Writes to a temporary and renames over the target. `rename` within a
    /// filesystem is atomic, so a crash mid-flush leaves the previous complete
    /// state file rather than a half-written one -- and a truncated state file is
    /// worse than a stale one, because it fails at restore time when you are
    /// already recovering from something else.
    void flush() const;

    /// Reads state from the file, replacing everything currently held. A missing
    /// file is not an error: it is a job starting for the first time.
    /// Returns whether a file was found.
    bool load();

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
    MemoryStateBackend memory_;
};

} // namespace ripple
