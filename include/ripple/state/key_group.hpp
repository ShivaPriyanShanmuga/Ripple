#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace ripple {

/// The number of hash buckets a job's keyspace is divided into. Fixed for the
/// life of the job and **independent of parallelism**.
///
/// This is the whole idea. Partitioning directly on `hash(key) % parallelism`
/// works until you change the parallelism, at which point every key moves and
/// none of them find their state. Partitioning on
/// `hash(key) % kMaxKeyGroups` and then assigning *ranges of key groups* to
/// subtasks means rescaling redistributes buckets rather than rehashing keys --
/// a key never changes group, only which subtask owns that group.
///
/// The cost is that this is the ceiling on parallelism: a job can never have
/// more subtasks than key groups, because a group cannot be split. Flink defaults
/// to 128 for the same reason and makes it configurable; changing it later
/// invalidates every existing checkpoint, so it is chosen once.
inline constexpr std::size_t kMaxKeyGroups = 128;

using KeyGroup = std::size_t;

/// A contiguous half-open span of key groups owned by one subtask.
struct KeyGroupRange {
    KeyGroup begin = 0;
    KeyGroup end = 0;

    [[nodiscard]] constexpr bool contains(KeyGroup group) const noexcept {
        return group >= begin && group < end;
    }

    [[nodiscard]] constexpr std::size_t size() const noexcept { return end - begin; }
};

/// FNV-1a over the **serialized** key bytes.
///
/// Hand-rolled rather than `std::hash`, and that is not stylistic.
/// `std::hash` gives no stability guarantee: an implementation may salt it per
/// process, and it certainly differs between libstdc++ and libc++. A key group
/// computed with it is written into a checkpoint and read back by a different
/// run -- possibly a different build -- so an unstable hash would scatter every
/// key to a different group on restore and silently lose all state.
///
/// Hashing the serialized bytes rather than the typed key is deliberate too: it
/// is the one representation both the partitioner and the state backend can
/// agree on, which is what makes the routing and the snapshot layout consistent
/// by construction rather than by convention.
[[nodiscard]] constexpr KeyGroup key_group_of(std::span<const std::byte> key) noexcept {
    constexpr std::uint64_t kOffsetBasis = 14695981039346656037ULL;
    constexpr std::uint64_t kPrime = 1099511628211ULL;

    std::uint64_t hash = kOffsetBasis;
    for (const std::byte byte : key) {
        hash ^= static_cast<std::uint64_t>(byte);
        hash *= kPrime;
    }
    return static_cast<KeyGroup>(hash % kMaxKeyGroups);
}

/// Which subtask owns a key group at a given parallelism.
///
/// This is the authoritative direction: routing a record asks this question.
[[nodiscard]] constexpr std::size_t subtask_for_key_group(KeyGroup group,
                                                          std::size_t parallelism) noexcept {
    return group * parallelism / kMaxKeyGroups;
}

/// The inverse: which key groups a subtask owns.
///
/// Must agree exactly with `subtask_for_key_group` or a rescale would leave gaps
/// (state nobody loads) or overlaps (state loaded twice). Derived by inverting
/// the floor division above -- `group * parallelism / kMaxKeyGroups == subtask`
/// holds precisely for groups in `[ceil(s*K/p), ceil((s+1)*K/p))`. The agreement
/// is asserted exhaustively in the tests rather than trusted to this comment.
[[nodiscard]] constexpr KeyGroupRange key_group_range_for(std::size_t subtask,
                                                          std::size_t parallelism) noexcept {
    const std::size_t begin = ((subtask * kMaxKeyGroups) + parallelism - 1) / parallelism;
    const std::size_t end = (((subtask + 1) * kMaxKeyGroups) + parallelism - 1) / parallelism;
    return KeyGroupRange{begin, end};
}

} // namespace ripple
