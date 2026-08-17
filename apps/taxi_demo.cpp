// Ripple demo: NYC-taxi-shaped trip stream.
//
// Two queries over the same data, both running on the parallel runtime with
// checkpointing enabled:
//
//   1. per-zone revenue in 5-minute tumbling windows
//   2. sessionization by vehicle -- how long a driver worked without a 30-minute
//      break, and what they earned during it
//
// Then the job is killed mid-stream and recovered from its last checkpoint, and
// the results are compared against an uninterrupted run.
//
// ## About the data
//
// The generator reproduces the properties of the real NYC TLC dataset that make
// it worth using (D-013), rather than a clean synthetic stream:
//
//   - **skew**: a few zones carry most of the traffic, so one subtask is always
//     hotter than the rest no matter how wide the job is;
//   - **out-of-order arrival**: trips are emitted in roughly, but not exactly,
//     event-time order, which is what watermarks exist for;
//   - **dirty records**: zero and negative fares, and timestamps decades out of
//     range, because the real dataset has them and they are what make the
//     late-data path matter rather than decorate.
//
// A synthetic generator is used here so the demo runs anywhere with no download.
// It is seeded, so two runs produce identical output.

#include <ripple/aggregators.hpp>
#include <ripple/checkpoint/checkpoint_coordinator.hpp>
#include <ripple/operator.hpp>
#include <ripple/operators/chain.hpp>
#include <ripple/operators/watermark_generator.hpp>
#include <ripple/operators/window.hpp>
#include <ripple/parallel/parallel_pipeline.hpp>
#include <ripple/record.hpp>
#include <ripple/sink.hpp>
#include <ripple/state/state_backend.hpp>
#include <ripple/timestamp.hpp>
#include <ripple/window.hpp>
#include <ripple/window_assigners.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <format>
#include <map>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace {

using ripple::Duration;
using ripple::Record;
using ripple::Timestamp;

constexpr std::int64_t kMinute = 60'000;
constexpr Duration kWindowSize{5 * kMinute};
constexpr Duration kSessionGap{30 * kMinute};
constexpr Duration kOutOfOrderBound{90'000};
constexpr std::size_t kParallelism = 4;

struct Trip {
    std::string zone;
    std::string vehicle;
    std::int64_t fare_cents = 0;
};

const auto kZoneOf = [](const Trip& trip) { return trip.zone; };
const auto kVehicleOf = [](const Trip& trip) { return trip.vehicle; };
const auto kFareOf = [](const Trip& trip) { return trip.fare_cents; };

using ZoneRevenue = ripple::WindowResult<std::string, std::int64_t>;
using DriverSession = ripple::WindowResult<std::string, std::int64_t>;

struct GeneratedStream {
    std::vector<Record<Trip>> trips;
    std::size_t dirty_records = 0;
};

GeneratedStream generate(std::size_t count) {
    std::mt19937 generator(20260817);

    const std::vector<std::string> zones{"Midtown Center", "Upper East Side",  "JFK Airport",
                                         "Times Square",   "LaGuardia",        "Union Square",
                                         "Harlem",         "Brooklyn Heights", "Astoria",
                                         "Bushwick",       "Riverdale",        "Bay Ridge"};
    // 1/rank weights: the top few zones dominate, as they do in the real data.
    std::vector<double> weights(zones.size());
    for (std::size_t i = 0; i < zones.size(); ++i) {
        weights[i] = 1.0 / static_cast<double>(i + 1);
    }
    std::discrete_distribution<std::size_t> zone_pick(weights.begin(), weights.end());
    std::uniform_int_distribution<int> vehicle_pick(1, 40);
    std::uniform_int_distribution<std::int64_t> fare(450, 8'500);
    // Arrival jitter stays inside the watermark bound for most records and
    // deliberately exceeds it for a few, so some data really is late.
    std::uniform_int_distribution<std::int64_t> jitter(-120'000, 20'000);
    std::uniform_int_distribution<int> dirt(0, 999);

    GeneratedStream stream;
    stream.trips.reserve(count);

    std::int64_t clock_millis = 0;
    for (std::size_t i = 0; i < count; ++i) {
        clock_millis += 40; // ~25 trips per simulated second

        std::int64_t event_millis = clock_millis + jitter(generator);
        std::int64_t fare_cents = fare(generator);

        const int roll = dirt(generator);
        if (roll < 3) {
            // Broken meter: a fare of zero or a negative "refund".
            fare_cents = roll == 0 ? 0 : -fare_cents;
            ++stream.dirty_records;
        } else if (roll < 5) {
            // Clock reset to 1970 or a meter stuck decades in the future. Both
            // appear verbatim in the real TLC data.
            event_millis = roll == 3 ? -3'000'000'000 : 4'000'000'000'000;
            ++stream.dirty_records;
        }

        stream.trips.push_back(
            ripple::make_record(Trip{zones[zone_pick(generator)],
                                     "cab-" + std::to_string(vehicle_pick(generator)), fare_cents},
                                ripple::timestamp_from_millis(event_millis)));
    }
    return stream;
}

std::string format_money(std::int64_t cents) {
    const std::int64_t remainder = cents % 100;
    return std::format("${}.{:02}", cents / 100, remainder < 0 ? -remainder : remainder);
}

std::string format_clock(Timestamp timestamp) {
    // The stream deliberately contains pre-epoch timestamps (D-032), so the
    // components have to be derived from the absolute value with the sign
    // carried separately -- otherwise C++'s truncating division prints
    // "00:-5:00" for a perfectly valid window before 1970.
    const std::int64_t millis = ripple::millis_since_epoch(timestamp);
    const std::int64_t magnitude = millis < 0 ? -millis : millis;
    return std::format("{}{}:{:02}:{:02}", millis < 0 ? "-" : "", magnitude / 3'600'000,
                       (magnitude / kMinute) % 60, (magnitude / 1'000) % 60);
}

/// The furthest into the past or future a trip's timestamp is believed.
///
/// ## Why sanitizing has to happen *before* the watermark generator
///
/// This is the single most instructive thing the dirty data teaches, and it was
/// found by running the demo rather than by reasoning about it.
///
/// Bounded out-of-orderness derives the watermark from the **maximum event time
/// seen** (D-024). One trip stamped in 2096 therefore drags the watermark to
/// 2096 minus the bound -- and every subsequent record, all of them perfectly
/// valid, is now late. A single broken meter silently discards the rest of the
/// stream.
///
/// It is worth being precise about why this is not a bug in the watermark
/// generator. Monotonicity is exactly what makes watermarks safe (a regressing
/// watermark would re-open fired windows), so the generator cannot defend
/// itself. The defence has to be upstream: reject implausible timestamps before
/// they can influence time at all.
///
/// The bound has to be tight enough to actually catch the bad data, and getting
/// that wrong is easy: the first version of this demo allowed anything before
/// the year 3000, while the broken meters stamp 2096 -- so the poison sailed
/// through and roughly 70% of all windows silently never fired. The symptom was
/// not an error but a plausible-looking report that was missing most of its
/// rows, which is the failure mode this whole project keeps running into.
///
/// A real deployment validates against a *business*-plausible range -- "not more
/// than an hour ahead of ingestion time" -- rather than against the limits of
/// the timestamp type. The demo's simulated clock starts at the epoch, so
/// anything beyond a year out is a broken meter.
constexpr std::int64_t kPlausibleFrom = -86'400'000;  // a day before the epoch
constexpr std::int64_t kPlausibleTo = 31'536'000'000; // one year after it

/// Drops trips whose timestamp cannot be believed.
///
/// Applied at ingest rather than as a pipeline stage because a `filter`
/// predicate sees the payload, not the record's event time -- and the event time
/// is precisely what is untrustworthy here. In a real deployment this is the
/// source connector's job for the same reason.
std::size_t sanitize(std::vector<Record<Trip>>& trips) {
    const std::size_t before = trips.size();
    std::erase_if(trips, [](const Record<Trip>& trip) {
        const std::int64_t millis = ripple::millis_since_epoch(trip.event_time);
        return millis < kPlausibleFrom || millis > kPlausibleTo;
    });
    return before - trips.size();
}

/// watermarks -> window, inside one subtask.
///
/// Chaining rather than two separate stages avoids a queue and a thread handoff
/// between operators that are only a few nanoseconds of work apart -- which is
/// why real engines chain and only break the chain where a shuffle forces it.
template<typename Assigner, typename KeySelector>
auto make_windowed_operator(KeySelector key_selector, Assigner assigner, Duration lateness,
                            std::string name) {
    return [key_selector, assigner, lateness, name](std::size_t, ripple::StateBackend&) {
        auto watermarks = ripple::make_bounded_out_of_orderness_watermarks<Trip>(kOutOfOrderBound);
        auto window = ripple::make_keyed_window<Trip>(
            key_selector, kFareOf, assigner, ripple::SumAggregator<std::int64_t>{}, lateness, name);
        return std::unique_ptr<ripple::Operator<Trip, ZoneRevenue>>(
            ripple::make_chain<Trip, Trip, ZoneRevenue>(std::move(watermarks), std::move(window),
                                                        name));
    };
}

struct QueryResult {
    std::map<std::pair<std::string, std::int64_t>, std::int64_t> by_key_and_window;
    std::size_t emitted = 0;
};

QueryResult collect(const std::vector<Record<ZoneRevenue>>& records) {
    QueryResult result;
    result.emitted = records.size();
    for (const Record<ZoneRevenue>& record : records) {
        // Allowed lateness means the same (key, window) can be emitted more than
        // once, with a corrected total (D-031). Last write wins -- the same
        // upsert requirement a real sink has.
        result.by_key_and_window[{record.value.key,
                                  ripple::millis_since_epoch(record.value.window.start)}] =
            record.value.value;
    }
    return result;
}

template<typename Assigner, typename KeySelector>
QueryResult run_query(const std::vector<Record<Trip>>& trips, KeySelector key_selector,
                      Assigner assigner, Duration lateness, const std::string& name,
                      ripple::CheckpointCoordinator* coordinator,
                      const ripple::RunOptions& options = {}) {
    ripple::CollectingSink<ZoneRevenue> sink;
    auto pipeline = ripple::make_parallel_pipeline<Trip, ZoneRevenue>(
        ripple::ParallelConfig{.parallelism = kParallelism,
                               .queue_capacity = 512,
                               .checkpoint_interval_records =
                                   coordinator != nullptr ? std::size_t{25'000} : std::size_t{0}},
        key_selector, make_windowed_operator(key_selector, assigner, lateness, name));
    pipeline.run(trips, sink, coordinator, options);
    return collect(sink.records());
}

void print_revenue(const QueryResult& result, std::size_t limit) {
    std::printf("\n  %-20s %-10s %14s\n", "zone", "window", "revenue");
    std::printf("  %s\n", std::string(46, '-').c_str());

    std::vector<std::pair<std::pair<std::string, std::int64_t>, std::int64_t>> rows(
        result.by_key_and_window.begin(), result.by_key_and_window.end());
    std::sort(rows.begin(), rows.end(),
              [](const auto& left, const auto& right) { return left.second > right.second; });

    for (std::size_t i = 0; i < std::min(limit, rows.size()); ++i) {
        std::printf("  %-20s %-10s %14s\n", rows[i].first.first.c_str(),
                    format_clock(ripple::timestamp_from_millis(rows[i].first.second)).c_str(),
                    format_money(rows[i].second).c_str());
    }
    std::printf("  ... %zu (zone, window) results total\n", rows.size());
}

void print_sessions(const QueryResult& result, std::size_t limit) {
    std::printf("\n  %-12s %-10s %14s\n", "vehicle", "session", "earned");
    std::printf("  %s\n", std::string(38, '-').c_str());

    std::vector<std::pair<std::pair<std::string, std::int64_t>, std::int64_t>> rows(
        result.by_key_and_window.begin(), result.by_key_and_window.end());
    std::sort(rows.begin(), rows.end(),
              [](const auto& left, const auto& right) { return left.second > right.second; });

    for (std::size_t i = 0; i < std::min(limit, rows.size()); ++i) {
        std::printf("  %-12s %-10s %14s\n", rows[i].first.first.c_str(),
                    format_clock(ripple::timestamp_from_millis(rows[i].first.second)).c_str(),
                    format_money(rows[i].second).c_str());
    }
    std::printf("  ... %zu driver sessions total\n", rows.size());
}

int run_demo(std::span<char* const> args) {
    std::size_t trip_count = 200'000;
    if (args.size() > 1) {
        trip_count = static_cast<std::size_t>(std::stoul(args[1]));
    }

    GeneratedStream stream = generate(trip_count);
    const std::size_t rejected = sanitize(stream.trips);
    std::printf("Ripple demo -- NYC-taxi-shaped trip stream\n\n");
    std::printf("  trips generated        %10zu\n", stream.trips.size());
    std::printf("  deliberately dirty     %10zu  (zero/negative fares, 1970 and 2096 clocks)\n",
                stream.dirty_records);
    std::printf("  rejected at ingest     %10zu  (implausible timestamps)\n", rejected);
    std::printf("  parallelism            %10zu\n", kParallelism);
    std::printf("  watermark bound        %10lld ms\n",
                static_cast<long long>(kOutOfOrderBound.count()));

    // ---- Query 1: per-zone revenue, 5-minute tumbling windows ----
    std::printf("\n=== Query 1: per-zone revenue, 5-minute tumbling windows ===\n");
    ripple::CheckpointCoordinator revenue_checkpoints(kParallelism + 1);
    const QueryResult revenue =
        run_query(stream.trips, kZoneOf, ripple::TumblingWindows{kWindowSize},
                  Duration{2 * kMinute}, "zone-revenue", &revenue_checkpoints);
    print_revenue(revenue, 8);
    std::printf("  window results emitted %10zu  (re-fires from allowed lateness included)\n",
                revenue.emitted);
    std::printf("  checkpoints completed  %10zu\n", revenue_checkpoints.completed_count());

    // ---- Query 2: sessionization by vehicle ----
    std::printf("\n=== Query 2: driver sessions (30-minute inactivity gap) ===\n");
    const QueryResult sessions =
        run_query(stream.trips, kVehicleOf, ripple::SessionWindows{kSessionGap}, Duration{0},
                  "driver-sessions", nullptr);
    print_sessions(sessions, 8);

    // ---- Fault injection ----
    std::printf("\n=== Fault injection: kill and recover ===\n");
    const std::size_t kill_point = stream.trips.size() / 2;

    ripple::CheckpointCoordinator crashed(kParallelism + 1);
    const QueryResult partial = run_query(
        stream.trips, kZoneOf, ripple::TumblingWindows{kWindowSize}, Duration{2 * kMinute},
        "zone-revenue", &crashed, ripple::RunOptions{.fail_after_records = kill_point});

    const auto checkpoint = crashed.latest_completed();
    if (!checkpoint.has_value()) {
        std::printf("\n  no checkpoint completed before the kill\n");
        return 1;
    }

    std::printf("\n  killed after           %10zu trips\n", kill_point);
    std::printf("  partial results        %10zu windows\n", partial.by_key_and_window.size());
    std::printf("  recovering from offset %10zu\n", checkpoint->source_offset);

    ripple::CheckpointCoordinator recovered_checkpoints(kParallelism + 1);
    const QueryResult recovered =
        run_query(stream.trips, kZoneOf, ripple::TumblingWindows{kWindowSize},
                  Duration{2 * kMinute}, "zone-revenue", &recovered_checkpoints,
                  ripple::RunOptions{.restore_from = &checkpoint.value()});

    // The recovered run only replays from the checkpoint onward, so on its own it
    // holds the tail. Merging it over the partial results is what a real
    // idempotent sink does: last write wins per (key, window).
    std::printf("  recovered alone        %10zu windows\n", recovered.by_key_and_window.size());
    std::printf("  clean run              %10zu windows\n", revenue.by_key_and_window.size());

    QueryResult merged = partial;
    for (const auto& [key, value] : recovered.by_key_and_window) {
        merged.by_key_and_window[key] = value;
    }

    const bool matches = merged.by_key_and_window == revenue.by_key_and_window;
    std::printf("  recovered + merged     %10zu windows\n", merged.by_key_and_window.size());
    std::printf("\n  results identical to an uninterrupted run: %s\n", matches ? "YES" : "NO");
    std::printf("\n  Note what that check is and is not. State is exactly-once: every window\n"
                "  total matches the clean run. Delivery was NOT -- the sink received the\n"
                "  windows between the checkpoint and the kill twice, which is why the merge\n"
                "  above is an upsert rather than an append.\n");

    return matches ? 0 : 1;
}

} // namespace

int main(int argc, char** argv) {
    // A span rather than pointer arithmetic on argv, and the whole body wrapped:
    // an exception escaping main produces nothing but a terminate message, which
    // is a poor way for a demo to fail.
    try {
        return run_demo(std::span<char* const>(argv, static_cast<std::size_t>(argc)));
    } catch (const std::exception& error) {
        std::printf("demo failed: %s\n", error.what());
        return 2;
    }
}
