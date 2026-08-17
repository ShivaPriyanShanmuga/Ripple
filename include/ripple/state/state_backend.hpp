#pragma once

#include <ripple/serialization.hpp>

#include <cstddef>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace ripple {

/// The serialized form of a key. Keys are bytes rather than a template
/// parameter, which is what lets a single non-template `StateBackend` serve
/// operators keyed on any type -- the same erasure argument as D-014, applied to
/// state instead of operators.
///
/// This is also the concrete reason serialization had to exist before keyed
/// state rather than after: without it there is no way to have one backend
/// implementation that is agnostic to key type.
using StateKey = std::vector<std::byte>;

/// Where keyed state lives.
///
/// Two-level addressing: every entry is identified by (current key, state name).
/// The key is set once per record by the keyed operator, and every state handle
/// then reads and writes under whatever key is current. Handles never take a key
/// themselves, which makes it structurally impossible for one operator to read
/// state under a key it is not currently processing.
///
/// ## On operator state
///
/// This interface covers *keyed* state -- partitioned by key, and in Stage 6 it
/// partitions across threads alongside the records. *Operator* state (a source's
/// read offset, for instance) is not keyed and does not belong here; it is
/// handled by `OperatorBase::snapshot_state`, which Stage 7 drives.
class StateBackend {
public:
    StateBackend() = default;
    virtual ~StateBackend() = default;

    StateBackend(const StateBackend&) = delete;
    StateBackend& operator=(const StateBackend&) = delete;
    StateBackend(StateBackend&&) = delete;
    StateBackend& operator=(StateBackend&&) = delete;

    /// Scopes every subsequent state access to this key.
    virtual void set_current_key(StateKey key) = 0;

    [[nodiscard]] virtual const StateKey& current_key() const noexcept = 0;

    /// Returns a view valid only until the next mutation of this backend.
    /// Callers deserialize immediately rather than storing the span -- the
    /// alternative is a copy on every read, which for a per-record path is not
    /// worth it.
    [[nodiscard]] virtual std::optional<std::span<const std::byte>>
    get(std::string_view state_name) const = 0;

    virtual void put(std::string_view state_name, std::vector<std::byte> value) = 0;

    virtual void remove(std::string_view state_name) = 0;

    /// Number of distinct keys holding state. The quantity that grows without
    /// bound if state is never cleared, which is the most common way a keyed
    /// streaming job runs out of memory.
    [[nodiscard]] virtual std::size_t key_count() const noexcept = 0;

    virtual void clear() = 0;

    /// ## Designed for Stage 7, not implemented by it
    ///
    /// Checkpointing needs to capture and restore a backend's entire contents.
    /// Defining that here -- and testing it now, since round-tripping a backend
    /// is meaningful on its own -- is what "design the interface for a future
    /// requirement without building the requirement" means in practice. Stage 7
    /// adds barriers, alignment, and a coordinator that *call* these; it does
    /// not change them.
    ///
    /// Implementations must write entries in a deterministic order, so that
    /// snapshotting identical state twice produces identical bytes. Without that
    /// property a checkpoint cannot be compared, deduplicated, or meaningfully
    /// diffed when something goes wrong.
    virtual void write_snapshot(ByteWriter& writer) const = 0;

    /// Replaces all current contents with the snapshot's.
    virtual void restore_snapshot(ByteReader& reader) = 0;
};

} // namespace ripple
