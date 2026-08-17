// Randomized property tests for windowing.
//
// Every other window test is hand-written: a specific input chosen to probe a
// specific boundary. Those are good at confirming behaviour someone already
// thought of, and useless against behaviour nobody did. These generate random
// streams instead and assert properties that must hold for *any* input.
//
// Seeded, so a failure is reproducible. A property test that cannot replay its
// own counterexample is barely better than no test -- the same reason the fault
// injection harness fixes its seed.

#include <ripple/aggregators.hpp>
#include <ripple/operators/watermark_generator.hpp>
#include <ripple/operators/window.hpp>
#include <ripple/pipeline.hpp>
#include <ripple/record.hpp>
#include <ripple/sink.hpp>
#include <ripple/source.hpp>
#include <ripple/timestamp.hpp>
#include <ripple/window.hpp>
#include <ripple/window_assigners.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace {

using ripple::Duration;
using ripple::Record;
using ripple::TumblingWindows;

struct Event {
    std::string key;
};

using Counted = ripple::WindowResult<std::string, std::size_t>;

struct GeneratedStream {
    std::vector<Record<Event>> records;
    std::size_t count = 0;
};

/// Random keys and jittered, deliberately out-of-order timestamps.
GeneratedStream generate(std::uint32_t seed, std::size_t count, std::size_t key_count,
                         std::int64_t jitter_millis) {
    std::mt19937 generator(seed);
    std::uniform_int_distribution<std::size_t> key_pick(0, key_count - 1);
    std::uniform_int_distribution<std::int64_t> jitter(-jitter_millis, jitter_millis);

    GeneratedStream stream;
    stream.records.reserve(count);
    std::int64_t clock = 0;
    for (std::size_t i = 0; i < count; ++i) {
        clock += 10;
        stream.records.push_back(
            ripple::make_record(Event{"k" + std::to_string(key_pick(generator))},
                                ripple::timestamp_from_millis(clock + jitter(generator))));
    }
    stream.count = count;
    return stream;
}

/// The final count per (key, window). Allowed lateness lets a window fire more
/// than once with a corrected value, so the last emission wins -- the same upsert
/// a real sink performs.
std::map<std::pair<std::string, std::int64_t>, std::size_t>
final_counts(const std::vector<Record<Counted>>& emitted) {
    std::map<std::pair<std::string, std::int64_t>, std::size_t> counts;
    for (const Record<Counted>& record : emitted) {
        counts[{record.value.key, ripple::millis_since_epoch(record.value.window.start)}] =
            record.value.value;
    }
    return counts;
}

struct RunOutcome {
    std::map<std::pair<std::string, std::int64_t>, std::size_t> counts;
    std::size_t late = 0;
};

RunOutcome run_windowed(const GeneratedStream& stream, Duration window_size, Duration bound,
                        Duration lateness) {
    auto window = ripple::make_keyed_window<Event>(
        [](const Event& event) { return event.key; }, [](const Event& event) { return event; },
        TumblingWindows{window_size}, ripple::CountAggregator<Event>{}, lateness);
    auto* window_ptr = window.get();

    auto sink = std::make_unique<ripple::CollectingSink<Counted>>();
    auto* sink_ptr = sink.get();

    auto pipeline = ripple::from(std::make_unique<ripple::VectorSource<Event>>(stream.records))
                        .via(ripple::make_bounded_out_of_orderness_watermarks<Event>(bound))
                        .via(std::move(window))
                        .to(std::move(sink));
    pipeline.run();

    return RunOutcome{final_counts(sink_ptr->records()), window_ptr->late_record_count()};
}

// Protects: every record is either counted in exactly one window or reported as
// late. Never both, never neither.
//
// The conservation law of a tumbling windowed job. It holds for any input, any
// window size, any watermark bound and any allowed lateness -- which is what
// makes it worth asserting over random streams rather than hand-picked ones.
//
// A violation in either direction is a serious bug that hand-written tests would
// struggle to find: records counted twice inflate results, records counted
// nowhere and not reported late vanish silently.
TEST(WindowPropertyTest, EveryRecordIsEitherWindowedExactlyOnceOrReportedLate) {
    for (std::uint32_t seed = 1; seed <= 25; ++seed) {
        std::mt19937 config(seed);
        const auto window_size =
            static_cast<std::int64_t>(std::uniform_int_distribution<int>(20, 500)(config));
        const auto bound =
            static_cast<std::int64_t>(std::uniform_int_distribution<int>(0, 300)(config));
        const auto lateness =
            static_cast<std::int64_t>(std::uniform_int_distribution<int>(0, 200)(config));
        const auto jitter =
            static_cast<std::int64_t>(std::uniform_int_distribution<int>(0, 400)(config));

        const GeneratedStream stream = generate(seed, 2'000, 8, jitter);
        const RunOutcome outcome =
            run_windowed(stream, Duration{window_size}, Duration{bound}, Duration{lateness});

        std::size_t windowed = 0;
        for (const auto& [key, count] : outcome.counts) {
            windowed += count;
        }

        EXPECT_EQ(windowed + outcome.late, stream.count)
            << "seed " << seed << ": window=" << window_size << "ms bound=" << bound
            << "ms lateness=" << lateness << "ms jitter=" << jitter << "ms -- " << windowed
            << " windowed + " << outcome.late << " late != " << stream.count << " total";
    }
}

// Protects: with a watermark bound wide enough that nothing can be late, the
// engine's output matches a direct grouping of the input.
//
// A differential test against a reference that is obviously correct because it is
// three lines long. Removing lateness from the picture makes the expected answer
// computable without modelling watermark progression, which is what lets the
// reference stay trivially auditable.
TEST(WindowPropertyTest, MatchesADirectGroupingWhenNothingCanBeLate) {
    for (std::uint32_t seed = 1; seed <= 15; ++seed) {
        constexpr std::int64_t kJitter = 300;
        constexpr std::int64_t kWindow = 250;
        // A bound far exceeding the jitter means the watermark can never
        // overtake a record that is still to come.
        constexpr std::int64_t kBound = 100'000;

        const GeneratedStream stream = generate(seed, 2'000, 6, kJitter);

        std::map<std::pair<std::string, std::int64_t>, std::size_t> expected;
        for (const Record<Event>& record : stream.records) {
            const std::int64_t millis = ripple::millis_since_epoch(record.event_time);
            const std::int64_t start = ripple::detail::floor_div(millis, kWindow) * kWindow;
            ++expected[{record.value.key, start}];
        }

        const RunOutcome outcome =
            run_windowed(stream, Duration{kWindow}, Duration{kBound}, Duration{0});

        EXPECT_EQ(outcome.late, 0U) << "seed " << seed << ": nothing should be late";
        EXPECT_EQ(outcome.counts, expected) << "seed " << seed;
    }
}

// Protects: window assignment is stable under permutation of arrival order.
//
// Event-time semantics mean the answer is a function of the data, not of the
// order it happened to arrive in. Shuffling the input must not change the result
// -- which is the whole reason event time exists, expressed as a property.
//
// A generous watermark bound is used so the shuffle cannot make records late;
// with a tight bound, arrival order legitimately changes which records are late,
// and the property would not hold.
TEST(WindowPropertyTest, ArrivalOrderDoesNotChangeTheAnswer) {
    for (std::uint32_t seed = 1; seed <= 10; ++seed) {
        constexpr std::int64_t kWindow = 200;
        constexpr std::int64_t kBound = 1'000'000;

        GeneratedStream ordered = generate(seed, 1'500, 5, 50);
        const RunOutcome in_order =
            run_windowed(ordered, Duration{kWindow}, Duration{kBound}, Duration{0});

        std::mt19937 shuffler(static_cast<std::mt19937::result_type>(seed) * 7919);
        std::shuffle(ordered.records.begin(), ordered.records.end(), shuffler);
        const RunOutcome shuffled =
            run_windowed(ordered, Duration{kWindow}, Duration{kBound}, Duration{0});

        EXPECT_EQ(shuffled.counts, in_order.counts)
            << "seed " << seed
            << ": shuffling arrival order changed the result, so the "
               "engine is not actually using event time";
    }
}

// Protects: window state is fully released once every window has expired.
//
// The memory property. A keyed windowed job that never releases keys grows
// without bound, and the growth is proportional to key cardinality -- which for
// something like a user id is unbounded. Checked with many keys so that a leak
// is visible rather than lost in noise.
TEST(WindowPropertyTest, ReleasesAllStateAfterEveryWindowExpires) {
    auto window = ripple::make_keyed_window<Event>(
        [](const Event& event) { return event.key; }, [](const Event& event) { return event; },
        TumblingWindows{Duration{100}}, ripple::CountAggregator<Event>{});
    auto* window_ptr = window.get();

    auto sink = std::make_unique<ripple::CollectingSink<Counted>>();
    const GeneratedStream stream = generate(99, 20'000, 5'000, 50);

    auto pipeline = ripple::from(std::make_unique<ripple::VectorSource<Event>>(stream.records))
                        .via(ripple::make_bounded_out_of_orderness_watermarks<Event>(Duration{10}))
                        .via(std::move(window))
                        .to(std::move(sink));
    pipeline.run();

    // The end-of-stream watermark expires everything.
    EXPECT_EQ(window_ptr->open_window_count(), 0U);
    EXPECT_EQ(window_ptr->keyed_state_size(), 0U)
        << "keys survived after all their windows expired -- a leak proportional to key "
           "cardinality";
}

} // namespace
