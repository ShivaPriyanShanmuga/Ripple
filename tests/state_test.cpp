#include <ripple/aggregators.hpp>
#include <ripple/operators/keyed.hpp>
#include <ripple/pipeline.hpp>
#include <ripple/record.hpp>
#include <ripple/serialization.hpp>
#include <ripple/sink.hpp>
#include <ripple/source.hpp>
#include <ripple/state/file_state_backend.hpp>
#include <ripple/state/memory_state_backend.hpp>
#include <ripple/state/state_backend.hpp>
#include <ripple/state/state_handles.hpp>
#include <ripple/timestamp.hpp>

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

using ripple::AggregatingState;
using ripple::ByteReader;
using ripple::ByteWriter;
using ripple::ListState;
using ripple::MemoryStateBackend;
using ripple::Record;
using ripple::SumAggregator;
using ripple::ValueState;

void select_key(ripple::StateBackend& backend, const std::string& key) {
    backend.set_current_key(ripple::serialize(key));
}

// ---------------------------------------------------------------------------
// State handles
// ---------------------------------------------------------------------------

// Protects: state is genuinely partitioned by key.
//
// The single most important property of keyed state. If keys leaked into one
// another the results would be wrong in a way no type checks and no test of a
// single key detects -- and in Stage 6, where each key belongs to one thread,
// leakage would also mean two threads touching the same state.
TEST(ValueStateTest, IsolatesKeysFromOneAnother) {
    MemoryStateBackend backend;
    ValueState<std::int64_t> fares(backend, "fares");

    select_key(backend, "midtown");
    fares.update(100);

    select_key(backend, "brooklyn");
    fares.update(50);

    select_key(backend, "midtown");
    EXPECT_EQ(fares.value(), std::optional<std::int64_t>{100});

    select_key(backend, "brooklyn");
    EXPECT_EQ(fares.value(), std::optional<std::int64_t>{50});
}

// Protects: "never written" is distinguishable from "written zero".
//
// Collapsing the two is how "no trips yet" silently becomes "earned zero" --
// which then propagates into averages and ratios as a real data point rather
// than an absent one.
TEST(ValueStateTest, DistinguishesUnsetFromZero) {
    MemoryStateBackend backend;
    ValueState<std::int64_t> fares(backend, "fares");

    select_key(backend, "queens");
    EXPECT_FALSE(fares.value().has_value());

    fares.update(0);
    ASSERT_TRUE(fares.value().has_value());
    EXPECT_EQ(fares.value_or(-1), 0);
}

// Protects: different state names under one key do not collide.
TEST(ValueStateTest, SeparatesStateByName) {
    MemoryStateBackend backend;
    ValueState<std::int64_t> fares(backend, "fares");
    ValueState<std::int64_t> trips(backend, "trips");

    select_key(backend, "midtown");
    fares.update(100);
    trips.update(3);

    EXPECT_EQ(fares.value_or(-1), 100);
    EXPECT_EQ(trips.value_or(-1), 3);
}

TEST(ValueStateTest, ClearRemovesOnlyTheCurrentKey) {
    MemoryStateBackend backend;
    ValueState<std::int64_t> fares(backend, "fares");

    select_key(backend, "a");
    fares.update(1);
    select_key(backend, "b");
    fares.update(2);

    select_key(backend, "a");
    fares.clear();
    EXPECT_FALSE(fares.value().has_value());

    select_key(backend, "b");
    EXPECT_EQ(fares.value_or(-1), 2);
}

// Protects: clearing a key's last state releases the key itself.
//
// Without this the values are freed but one empty map node per key survives
// forever. That is a leak proportional to key cardinality, which for something
// like a user id is unbounded -- the classic way a keyed streaming job dies
// slowly.
TEST(MemoryStateBackendTest, ReleasesKeysWhoseStateIsFullyCleared) {
    MemoryStateBackend backend;
    ValueState<std::int64_t> fares(backend, "fares");

    for (int i = 0; i < 100; ++i) {
        select_key(backend, "key-" + std::to_string(i));
        fares.update(i);
    }
    EXPECT_EQ(backend.key_count(), 100U);

    for (int i = 0; i < 100; ++i) {
        select_key(backend, "key-" + std::to_string(i));
        fares.clear();
    }
    EXPECT_EQ(backend.key_count(), 0U);
}

TEST(ListStateTest, AccumulatesPerKey) {
    MemoryStateBackend backend;
    ListState<std::string> visits(backend, "visits");

    select_key(backend, "user-1");
    visits.add("home");
    visits.add("search");

    select_key(backend, "user-2");
    visits.add("checkout");

    select_key(backend, "user-1");
    EXPECT_EQ(visits.get(), (std::vector<std::string>{"home", "search"}));

    select_key(backend, "user-2");
    EXPECT_EQ(visits.get(), std::vector<std::string>{"checkout"});
}

TEST(ListStateTest, ReturnsEmptyForUnwrittenKeys) {
    MemoryStateBackend backend;
    const ListState<std::string> visits(backend, "visits");
    select_key(backend, "nobody");
    EXPECT_TRUE(visits.get().empty());
}

TEST(AggregatingStateTest, MaintainsARunningTotalPerKey) {
    MemoryStateBackend backend;
    AggregatingState<SumAggregator<std::int64_t>> total(backend, "total");

    select_key(backend, "midtown");
    total.add(10);
    total.add(5);

    select_key(backend, "queens");
    total.add(3);

    select_key(backend, "midtown");
    EXPECT_EQ(total.get(), 15);

    select_key(backend, "queens");
    EXPECT_EQ(total.get(), 3);
}

TEST(AggregatingStateTest, ReportsEmptyBeforeAnythingIsAdded) {
    MemoryStateBackend backend;
    AggregatingState<SumAggregator<std::int64_t>> total(backend, "total");

    select_key(backend, "midtown");
    EXPECT_TRUE(total.empty());
    EXPECT_EQ(total.get(), 0) << "identity value for an untouched key";

    total.add(0);
    EXPECT_FALSE(total.empty()) << "an explicit zero is not the same as no data";
}

// ---------------------------------------------------------------------------
// Snapshot and restore -- the interface Stage 7 will drive
// ---------------------------------------------------------------------------

// Protects: a backend's entire contents survive a round trip through bytes.
//
// This is the primitive checkpointing is built on. Stage 7 adds barriers,
// alignment and a coordinator that call these; it does not change them.
TEST(StateSnapshotTest, RoundTripsAllKeysAndStates) {
    MemoryStateBackend original;
    ValueState<std::int64_t> fares(original, "fares");
    ListState<std::string> visits(original, "visits");

    select_key(original, "midtown");
    fares.update(100);
    visits.add("a");
    select_key(original, "brooklyn");
    fares.update(50);

    ByteWriter writer;
    original.write_snapshot(writer);

    MemoryStateBackend restored;
    ByteReader reader(writer.bytes());
    restored.restore_snapshot(reader);
    EXPECT_TRUE(reader.exhausted()) << "snapshot format wrote more than restore consumed";

    const ValueState<std::int64_t> restored_fares(restored, "fares");
    const ListState<std::string> restored_visits(restored, "visits");

    select_key(restored, "midtown");
    EXPECT_EQ(restored_fares.value_or(-1), 100);
    EXPECT_EQ(restored_visits.get(), std::vector<std::string>{"a"});

    select_key(restored, "brooklyn");
    EXPECT_EQ(restored_fares.value_or(-1), 50);
    EXPECT_EQ(restored.key_count(), 2U);
}

// Protects: identical state snapshots to identical bytes.
//
// The reason both levels of the backend are ordered maps rather than hash maps.
// An unordered_map's iteration order depends on insertion history and bucket
// count, so two backends holding the same logical state would serialize
// differently -- and checkpoints could then never be compared, deduplicated, or
// diffed when something went wrong.
TEST(StateSnapshotTest, IsDeterministicRegardlessOfInsertionOrder) {
    // Each key always carries the same value, so the only thing differing
    // between the two calls is the order the entries were inserted in.
    const auto snapshot_of = [](const std::vector<std::pair<std::string, std::int64_t>>& entries) {
        MemoryStateBackend backend;
        ValueState<std::int64_t> fares(backend, "fares");
        for (const auto& [key, value] : entries) {
            select_key(backend, key);
            fares.update(value);
        }
        ByteWriter writer;
        backend.write_snapshot(writer);
        return writer.bytes();
    };

    EXPECT_EQ(snapshot_of({{"a", 1}, {"b", 2}, {"c", 3}}),
              snapshot_of({{"c", 3}, {"b", 2}, {"a", 1}}))
        << "snapshot bytes depend on insertion order";
}

TEST(StateSnapshotTest, RestoreReplacesExistingContents) {
    MemoryStateBackend source;
    ValueState<std::int64_t> value(source, "v");
    select_key(source, "kept");
    value.update(1);

    ByteWriter writer;
    source.write_snapshot(writer);

    MemoryStateBackend target;
    ValueState<std::int64_t> target_value(target, "v");
    select_key(target, "stale");
    target_value.update(999);

    ByteReader reader(writer.bytes());
    target.restore_snapshot(reader);

    EXPECT_EQ(target.key_count(), 1U);
    select_key(target, "stale");
    EXPECT_FALSE(target_value.value().has_value()) << "pre-restore state survived a restore";
}

// ---------------------------------------------------------------------------
// File-backed backend
// ---------------------------------------------------------------------------

class FileStateBackendTest : public ::testing::Test {
protected:
    void SetUp() override {
        directory_ =
            std::filesystem::temp_directory_path() /
            ("ripple-state-test-" +
             std::to_string(::testing::UnitTest::GetInstance()->random_seed() + counter_++));
        std::filesystem::create_directories(directory_);
    }

    void TearDown() override { std::filesystem::remove_all(directory_); }

    [[nodiscard]] std::filesystem::path state_file() const { return directory_ / "state.bin"; }

    std::filesystem::path directory_;
    static inline int counter_ = 0;
};

TEST_F(FileStateBackendTest, PersistsStateAcrossInstances) {
    {
        ripple::FileStateBackend backend(state_file());
        ValueState<std::int64_t> fares(backend, "fares");
        select_key(backend, "midtown");
        fares.update(100);
        select_key(backend, "queens");
        fares.update(7);
        backend.flush();
    }

    ripple::FileStateBackend reopened(state_file());
    ASSERT_TRUE(reopened.load());

    const ValueState<std::int64_t> fares(reopened, "fares");
    select_key(reopened, "midtown");
    EXPECT_EQ(fares.value_or(-1), 100);
    select_key(reopened, "queens");
    EXPECT_EQ(fares.value_or(-1), 7);
}

// Protects: a missing state file is a job starting fresh, not an error.
TEST_F(FileStateBackendTest, TreatsAMissingFileAsAnEmptyStart) {
    ripple::FileStateBackend backend(directory_ / "does-not-exist.bin");
    EXPECT_FALSE(backend.load());
    EXPECT_EQ(backend.key_count(), 0U);
}

// Protects: a corrupt state file fails loudly at load rather than restoring
// partial state.
//
// Restore happens when you are already recovering from something else. Silently
// restoring half a state file turns one incident into a much harder one.
TEST_F(FileStateBackendTest, RejectsACorruptStateFile) {
    {
        ripple::FileStateBackend backend(state_file());
        ValueState<std::int64_t> fares(backend, "fares");
        select_key(backend, "midtown");
        fares.update(100);
        backend.flush();
    }

    std::ofstream(state_file(), std::ios::binary | std::ios::app).put('\xFF');

    ripple::FileStateBackend backend(state_file());
    EXPECT_THROW((void)backend.load(), ripple::SerializationError);
}

// ---------------------------------------------------------------------------
// keyBy in a pipeline
// ---------------------------------------------------------------------------

struct Trip {
    std::string zone;
    std::int64_t fare;
};

// Protects: keyed aggregation end to end -- routing, state isolation, and the
// running total emitted per record.
TEST(KeyedOperatorTest, MaintainsIndependentRunningTotalsPerKey) {
    MemoryStateBackend backend;

    std::vector<Record<Trip>> input{
        ripple::make_record(Trip{"midtown", 10}, ripple::timestamp_from_millis(1)),
        ripple::make_record(Trip{"queens", 3}, ripple::timestamp_from_millis(2)),
        ripple::make_record(Trip{"midtown", 5}, ripple::timestamp_from_millis(3)),
        ripple::make_record(Trip{"queens", 4}, ripple::timestamp_from_millis(4)),
    };

    using Output = ripple::KeyedValue<std::string, std::int64_t>;
    auto sink = std::make_unique<ripple::CollectingSink<Output>>();
    auto* sink_ptr = sink.get();

    auto pipeline =
        ripple::from(std::make_unique<ripple::VectorSource<Trip>>(std::move(input)))
            .via(ripple::make_keyed_aggregate<Trip>(
                backend, [](const Trip& trip) { return trip.zone; },
                [](const Trip& trip) { return trip.fare; }, SumAggregator<std::int64_t>{}))
            .to(std::move(sink));
    pipeline.run();

    ASSERT_EQ(sink_ptr->records().size(), 4U);
    EXPECT_EQ(sink_ptr->records()[0].value.value, 10); // midtown: 10
    EXPECT_EQ(sink_ptr->records()[1].value.value, 3);  // queens:  3
    EXPECT_EQ(sink_ptr->records()[2].value.value, 15); // midtown: 10 + 5
    EXPECT_EQ(sink_ptr->records()[3].value.value, 7);  // queens:  3 + 4

    EXPECT_EQ(backend.key_count(), 2U);
}

// Protects: the aggregator receives the whole input value, and the key travels
// with the result so downstream can tell which total it is looking at.
TEST(KeyedOperatorTest, TagsEachResultWithItsKey) {
    MemoryStateBackend backend;

    std::vector<Record<Trip>> input{
        ripple::make_record(Trip{"midtown", 10}, ripple::timestamp_from_millis(1)),
        ripple::make_record(Trip{"queens", 3}, ripple::timestamp_from_millis(2)),
    };

    using Output = ripple::KeyedValue<std::string, std::int64_t>;
    auto sink = std::make_unique<ripple::CollectingSink<Output>>();
    auto* sink_ptr = sink.get();

    auto pipeline =
        ripple::from(std::make_unique<ripple::VectorSource<Trip>>(std::move(input)))
            .via(ripple::make_keyed_aggregate<Trip>(
                backend, [](const Trip& trip) { return trip.zone; },
                [](const Trip& trip) { return trip.fare; }, SumAggregator<std::int64_t>{}))
            .to(std::move(sink));
    pipeline.run();

    ASSERT_EQ(sink_ptr->records().size(), 2U);
    EXPECT_EQ(sink_ptr->records()[0].value.key, "midtown");
    EXPECT_EQ(sink_ptr->records()[1].value.key, "queens");
    // Event time is preserved through a keyed operator, same as any other.
    EXPECT_EQ(ripple::millis_since_epoch(sink_ptr->records()[0].event_time), 1);
}

} // namespace
