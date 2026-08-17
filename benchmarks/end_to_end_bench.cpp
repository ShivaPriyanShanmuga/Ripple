// End-to-end harness: throughput, latency percentiles, checkpoint cost, and
// recovery time.
//
// Not a Google Benchmark target. Google Benchmark is built for repeating a small
// operation until the timing is stable, which is the wrong shape for "run a
// multi-threaded pipeline once and describe how it behaved". The numbers here
// are a distribution and a few one-shot durations, so the harness reports them
// itself.
//
// ## Why percentiles and never a mean alone
//
// A mean latency hides exactly the behaviour that matters. A pipeline that
// answers in 1ms 99% of the time and 900ms the rest has a mean around 10ms,
// which describes no request that ever happened. The tail is where queueing,
// checkpoint stalls and GC-like pauses show up, and it is what a user actually
// experiences. p99.9 is reported because at any real record rate it is reached
// many times a second.

#include <ripple/aggregators.hpp>
#include <ripple/checkpoint/checkpoint_coordinator.hpp>
#include <ripple/operator.hpp>
#include <ripple/operators/keyed.hpp>
#include <ripple/parallel/parallel_pipeline.hpp>
#include <ripple/record.hpp>
#include <ripple/sink.hpp>
#include <ripple/state/state_backend.hpp>
#include <ripple/timestamp.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
using Nanos = std::chrono::nanoseconds;

struct Event {
    std::string zone;
    std::int64_t fare = 0;
    /// Index in the input. With a paced source, record `i` is *scheduled* to
    /// enter the pipeline at `start + i / rate`, so latency can be computed
    /// against that schedule without stamping anything inside the engine.
    std::size_t sequence = 0;
};

using Output = ripple::KeyedValue<std::string, std::int64_t>;

const auto kZoneOf = [](const Event& event) { return event.zone; };
const auto kFareOf = [](const Event& event) { return event.fare; };

auto make_operator_factory() {
    return [](std::size_t, ripple::StateBackend& backend) {
        return std::unique_ptr<ripple::Operator<Event, Output>>(ripple::make_keyed_aggregate<Event>(
            backend, kZoneOf, kFareOf, ripple::SumAggregator<std::int64_t>{}));
    };
}

/// Records when each result reached the sink, so latency can be reconstructed
/// against the source's schedule afterwards.
class TimingSink final : public ripple::Sink<Output> {
public:
    void write(ripple::Record<Output>&& record) override {
        arrivals_.push_back(Clock::now());
        last_ = std::move(record.value);
    }

    [[nodiscard]] std::string_view name() const noexcept override { return "timing-sink"; }

    [[nodiscard]] const std::vector<Clock::time_point>& arrivals() const noexcept {
        return arrivals_;
    }

    [[nodiscard]] const Output& last() const noexcept { return last_; }

private:
    std::vector<Clock::time_point> arrivals_;
    Output last_;
};

std::vector<ripple::Record<Event>> generate(std::size_t count, std::size_t zone_count) {
    std::mt19937 generator(20260817);
    // Zipf-ish skew: a few zones carry most of the traffic, which is what real
    // keyed workloads look like and what makes key skew visible.
    std::discrete_distribution<std::size_t> zone_pick = [&] {
        std::vector<double> weights(zone_count);
        for (std::size_t i = 0; i < zone_count; ++i) {
            weights[i] = 1.0 / static_cast<double>(i + 1);
        }
        return std::discrete_distribution<std::size_t>(weights.begin(), weights.end());
    }();
    std::uniform_int_distribution<std::int64_t> fare(1, 100);

    std::vector<ripple::Record<Event>> input;
    input.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        input.push_back(ripple::make_record(
            Event{"zone-" + std::to_string(zone_pick(generator)), fare(generator), i},
            ripple::timestamp_from_millis(static_cast<std::int64_t>(i))));
    }
    return input;
}

/// Nearest-rank percentile over a sorted range.
double percentile(const std::vector<double>& sorted, double p) {
    if (sorted.empty()) {
        return 0.0;
    }
    const auto index = static_cast<std::size_t>(p / 100.0 * static_cast<double>(sorted.size() - 1));
    return sorted[index];
}

void report_latency(const std::string& label, std::vector<double> micros) {
    std::sort(micros.begin(), micros.end());
    const double mean = micros.empty() ? 0.0
                                       : std::accumulate(micros.begin(), micros.end(), 0.0) /
                                             static_cast<double>(micros.size());

    std::printf("\n%s (microseconds, n=%zu)\n", label.c_str(), micros.size());
    std::printf("  p50    %10.1f\n", percentile(micros, 50.0));
    std::printf("  p90    %10.1f\n", percentile(micros, 90.0));
    std::printf("  p99    %10.1f\n", percentile(micros, 99.0));
    std::printf("  p99.9  %10.1f\n", percentile(micros, 99.9));
    std::printf("  max    %10.1f\n", micros.empty() ? 0.0 : micros.back());
    // Printed last and only alongside the percentiles. On its own it would hide
    // the tail entirely, which is the part that matters.
    std::printf("  mean   %10.1f   <- never quote this without the tail\n", mean);
}

struct ThroughputResult {
    double records_per_second = 0.0;
    double seconds = 0.0;
};

ThroughputResult measure_throughput(const std::vector<ripple::Record<Event>>& input,
                                    std::size_t parallelism, std::size_t checkpoint_interval,
                                    ripple::CheckpointCoordinator* coordinator) {
    ripple::CollectingSink<Output> sink;
    auto pipeline = ripple::make_parallel_pipeline<Event, Output>(
        ripple::ParallelConfig{.parallelism = parallelism,
                               .queue_capacity = 1'024,
                               .checkpoint_interval_records = checkpoint_interval},
        kZoneOf, make_operator_factory());

    const auto started = Clock::now();
    pipeline.run(input, sink, coordinator);
    const auto elapsed = Clock::now() - started;

    const double seconds = std::chrono::duration<double>(elapsed).count();
    return ThroughputResult{static_cast<double>(input.size()) / seconds, seconds};
}

void run_throughput_section(const std::vector<ripple::Record<Event>>& input) {
    std::printf("\n=== Throughput (source unpaced, %zu records) ===\n\n", input.size());
    std::printf("  %-14s %16s %12s\n", "parallelism", "records/sec", "seconds");
    for (const std::size_t parallelism : {1U, 2U, 4U, 8U}) {
        const auto result = measure_throughput(input, parallelism, 0, nullptr);
        std::printf("  %-14zu %16.0f %12.3f\n", parallelism, result.records_per_second,
                    result.seconds);
    }
    std::printf("\n  Note: scaling is bounded by key skew -- the generator is deliberately\n"
                "  Zipf-skewed, so the hottest zone caps one subtask no matter the width.\n");
}

void run_latency_section(const std::vector<ripple::Record<Event>>& input) {
    constexpr std::size_t kRate = 200'000;
    constexpr std::size_t kParallelism = 4;

    TimingSink sink;
    auto pipeline = ripple::make_parallel_pipeline<Event, Output>(
        ripple::ParallelConfig{.parallelism = kParallelism, .queue_capacity = 1'024}, kZoneOf,
        make_operator_factory());

    pipeline.run(input, sink, nullptr, ripple::RunOptions{.target_records_per_second = kRate});

    // Measured against the origin the *source* paced from, not against a
    // timestamp taken before run(). The latter would fold pipeline setup into
    // every record as a constant offset.
    const auto started = pipeline.metrics().source_started_at;

    // Latency for the i-th result is measured against when record i was
    // *scheduled* to enter the pipeline, not when it was generated. Records are
    // emitted in order and this operator emits one result per record, so the
    // i-th arrival corresponds to the i-th input.
    const std::size_t batch = ripple::pacing_batch_size(kRate);
    std::vector<double> micros;
    micros.reserve(sink.arrivals().size());
    for (std::size_t i = 0; i < sink.arrivals().size(); ++i) {
        // Records enter in batches, all at the batch's due time -- so that, not
        // each record's nominal slot, is when this record actually started.
        const auto entered = started + Nanos{(i / batch) * batch * 1'000'000'000ULL / kRate};
        micros.push_back(
            std::chrono::duration<double, std::micro>(sink.arrivals()[i] - entered).count());
    }

    std::printf("\n=== Latency (source paced at %zu rec/s, parallelism %zu) ===\n", kRate,
                kParallelism);
    report_latency("  end-to-end: scheduled source time -> sink write", std::move(micros));
}

void run_checkpoint_section(const std::vector<ripple::Record<Event>>& input) {
    constexpr std::size_t kParallelism = 4;
    constexpr std::size_t kInterval = 50'000;

    const auto without = measure_throughput(input, kParallelism, 0, nullptr);

    ripple::CheckpointCoordinator coordinator(kParallelism + 1);
    const auto with = measure_throughput(input, kParallelism, kInterval, &coordinator);

    std::vector<double> durations;
    for (const auto& checkpoint : coordinator.completed()) {
        durations.push_back(std::chrono::duration<double, std::micro>(checkpoint.duration).count());
    }
    std::sort(durations.begin(), durations.end());

    std::printf("\n=== Checkpointing (every %zu records, parallelism %zu) ===\n\n", kInterval,
                kParallelism);
    std::printf("  throughput without checkpoints  %12.0f rec/s\n", without.records_per_second);
    std::printf("  throughput with checkpoints     %12.0f rec/s\n", with.records_per_second);
    std::printf("  cost                            %12.1f %%\n",
                100.0 * (1.0 - (with.records_per_second / without.records_per_second)));
    std::printf("  checkpoints completed           %12zu\n", coordinator.completed_count());
    if (!durations.empty()) {
        std::printf("  duration p50 / p99 / max        %8.1f / %8.1f / %8.1f us\n",
                    percentile(durations, 50.0), percentile(durations, 99.0), durations.back());
    }
    std::printf("\n  Duration is barrier injection -> last acknowledgement: roughly the\n"
                "  slowest single task's snapshot plus barrier travel, which is why the\n"
                "  barrier is forwarded before snapshotting (D-058).\n");
}

void run_recovery_section(const std::vector<ripple::Record<Event>>& input) {
    constexpr std::size_t kParallelism = 4;
    constexpr std::size_t kInterval = 20'000;
    const std::size_t kill_point = input.size() / 2;

    ripple::CheckpointCoordinator first_run(kParallelism + 1);
    ripple::CollectingSink<Output> sink;
    {
        auto pipeline = ripple::make_parallel_pipeline<Event, Output>(
            ripple::ParallelConfig{.parallelism = kParallelism,
                                   .queue_capacity = 1'024,
                                   .checkpoint_interval_records = kInterval},
            kZoneOf, make_operator_factory());
        pipeline.run(input, sink, &first_run, ripple::RunOptions{.fail_after_records = kill_point});
    }

    const auto checkpoint = first_run.latest_completed();
    if (!checkpoint.has_value()) {
        std::printf("\n=== Recovery ===\n\n  no checkpoint completed before the kill\n");
        return;
    }

    ripple::CheckpointCoordinator second_run(kParallelism + 1);
    const auto started = Clock::now();
    {
        auto pipeline = ripple::make_parallel_pipeline<Event, Output>(
            ripple::ParallelConfig{.parallelism = kParallelism, .queue_capacity = 1'024}, kZoneOf,
            make_operator_factory());
        pipeline.run(input, sink, &second_run,
                     ripple::RunOptions{.restore_from = &checkpoint.value()});
    }
    const auto elapsed = Clock::now() - started;

    const std::size_t replayed = input.size() - checkpoint->source_offset;
    std::printf("\n=== Recovery ===\n\n");
    std::printf("  killed after                    %12zu records\n", kill_point);
    std::printf("  recovered from offset           %12zu\n", checkpoint->source_offset);
    std::printf("  records replayed                %12zu\n", replayed);
    std::printf("  time to restore and drain       %12.3f s\n",
                std::chrono::duration<double>(elapsed).count());
    std::printf("\n  Recovery time is dominated by replay, not by loading state: the work is\n"
                "  proportional to how far past the last checkpoint the failure happened.\n"
                "  That is the real argument for frequent checkpoints, and the reason the\n"
                "  checkpoint cost above is the number to trade it against.\n");
}

} // namespace

int main(int argc, char** argv) {
    std::size_t record_count = 500'000;
    if (argc > 1) {
        record_count = static_cast<std::size_t>(std::stoul(argv[1]));
    }

    std::printf("Ripple end-to-end benchmark\n");
    std::printf("hardware_concurrency = %u\n", std::thread::hardware_concurrency());
    std::printf("\nBuild this with the `release` preset. Sanitizer builds are 5-20x slower\n"
                "and the numbers mean nothing.\n");

    const auto input = generate(record_count, 64);

    run_throughput_section(input);
    run_latency_section(input);
    run_checkpoint_section(input);
    run_recovery_section(input);

    std::printf("\n");
    return 0;
}
