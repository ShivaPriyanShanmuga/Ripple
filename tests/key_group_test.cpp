#include <ripple/serialization.hpp>
#include <ripple/state/key_group.hpp>

#include <gtest/gtest.h>

#include <cstddef>
#include <set>
#include <string>
#include <vector>

namespace {

using ripple::KeyGroup;
using ripple::KeyGroupRange;
using ripple::kMaxKeyGroups;

// Protects: the two directions of the key-group mapping agree **exactly**.
//
// `subtask_for_key_group` answers "who owns this group" when routing a record;
// `key_group_range_for` answers "which groups do I own" when restoring state.
// If they disagree by even one group at one parallelism, a rescale silently
// leaves either a gap (state nobody loads, so those keys start from zero) or an
// overlap (state loaded by two subtasks, so those keys are counted twice).
//
// Checked exhaustively rather than argued, because the inverse of a floor
// division is exactly the kind of thing that is off by one at boundaries.
TEST(KeyGroupTest, RangeAndOwnershipAgreeAtEveryParallelism) {
    for (std::size_t parallelism = 1; parallelism <= kMaxKeyGroups; ++parallelism) {
        for (std::size_t subtask = 0; subtask < parallelism; ++subtask) {
            const KeyGroupRange range = ripple::key_group_range_for(subtask, parallelism);
            for (KeyGroup group = 0; group < kMaxKeyGroups; ++group) {
                const bool owns = ripple::subtask_for_key_group(group, parallelism) == subtask;
                EXPECT_EQ(range.contains(group), owns)
                    << "parallelism " << parallelism << ", subtask " << subtask << ", group "
                    << group;
            }
        }
    }
}

// Protects: the ranges partition the keyspace -- every group owned by exactly
// one subtask, at every parallelism.
TEST(KeyGroupTest, RangesCoverEveryGroupExactlyOnce) {
    for (std::size_t parallelism = 1; parallelism <= kMaxKeyGroups; ++parallelism) {
        std::vector<int> owners(kMaxKeyGroups, 0);
        for (std::size_t subtask = 0; subtask < parallelism; ++subtask) {
            const KeyGroupRange range = ripple::key_group_range_for(subtask, parallelism);
            for (KeyGroup group = range.begin; group < range.end; ++group) {
                ++owners[group];
            }
        }
        for (KeyGroup group = 0; group < kMaxKeyGroups; ++group) {
            EXPECT_EQ(owners[group], 1) << "group " << group << " has " << owners[group]
                                        << " owners at parallelism " << parallelism;
        }
    }
}

// Protects: a key's group never depends on parallelism.
//
// The entire premise of key groups. If the group moved when the parallelism
// changed we would be back to rehashing every key, which is the problem this
// exists to solve.
TEST(KeyGroupTest, AKeysGroupIsIndependentOfParallelism) {
    for (const std::string& zone : {std::string{"midtown"}, std::string{"brooklyn"},
                                    std::string{"queens"}, std::string{"a"}, std::string{""}}) {
        const auto key = ripple::serialize(zone);
        const KeyGroup group = ripple::key_group_of(key);
        EXPECT_LT(group, kMaxKeyGroups);
        EXPECT_EQ(group, ripple::key_group_of(key)) << "hash is not deterministic";
    }
}

// Protects: the hash is stable across builds and runs.
//
// Pinned to literal values on purpose. `std::hash` gives no stability guarantee --
// it may be salted per process and certainly differs between standard library
// implementations -- and a key group is written into a checkpoint and read back
// by a different run, possibly a different build. An unstable hash would scatter
// every key to a different group on restore and silently lose all state.
//
// If this test ever fails, the checkpoint format has changed incompatibly and
// every existing checkpoint is unreadable. That is exactly the sort of change
// that should be impossible to make by accident.
TEST(KeyGroupTest, HashIsPinnedSoCheckpointsStayReadable) {
    EXPECT_EQ(ripple::key_group_of(ripple::serialize(std::string{"midtown"})), 68U);
    EXPECT_EQ(ripple::key_group_of(ripple::serialize(std::string{"brooklyn"})), 43U);
    EXPECT_EQ(ripple::key_group_of(ripple::serialize(std::string{"queens"})), 92U);
}

// Protects: distinct keys spread across groups rather than clustering.
//
// Not a strong statistical claim -- just enough to catch a hash that returns a
// constant, which would route every key to one subtask and would otherwise show
// up only as inexplicably poor scaling.
TEST(KeyGroupTest, DistributesDistinctKeysAcrossManyGroups) {
    std::set<KeyGroup> groups;
    for (int i = 0; i < 1'000; ++i) {
        groups.insert(ripple::key_group_of(ripple::serialize("key-" + std::to_string(i))));
    }
    EXPECT_GT(groups.size(), kMaxKeyGroups / 2)
        << "only " << groups.size() << " of " << kMaxKeyGroups << " groups were used";
}

} // namespace
