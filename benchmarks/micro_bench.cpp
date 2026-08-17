// Component-level benchmarks.
//
// These measure the pieces in isolation, which is what makes them useful for
// deciding *where* to optimise -- an end-to-end number tells you the pipeline is
// slow, not which part of it is. Nothing here has been optimised: this is the
// baseline that a later change would have to beat, which is the only honest
// order to do it in.

#include <ripple/aggregators.hpp>
#include <ripple/concurrent/bounded_queue.hpp>
#include <ripple/operators/filter.hpp>
#include <ripple/operators/map.hpp>
#include <ripple/operators/window.hpp>
#include <ripple/pipeline.hpp>
#include <ripple/record.hpp>
#include <ripple/serialization.hpp>
#include <ripple/sink.hpp>
#include <ripple/source.hpp>
#include <ripple/state/key_group.hpp>
#include <ripple/state/memory_state_backend.hpp>
#include <ripple/state/state_handles.hpp>
#include <ripple/timestamp.hpp>
#include <ripple/window_assigners.hpp>

#include <benchmark/benchmark.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

void BM_SerializeInt64(benchmark::State& state) {
    for (auto _ : state) {
        benchmark::DoNotOptimize(ripple::serialize<std::int64_t>(1'700'000'000'000));
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_SerializeInt64);

void BM_SerializeString(benchmark::State& state) {
    const std::string value(static_cast<std::size_t>(state.range(0)), 'x');
    for (auto _ : state) {
        benchmark::DoNotOptimize(ripple::serialize(value));
    }
    state.SetBytesProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_SerializeString)->Arg(8)->Arg(64)->Arg(512);

void BM_DeserializeString(benchmark::State& state) {
    const auto bytes = ripple::serialize(std::string(64, 'x'));
    for (auto _ : state) {
        benchmark::DoNotOptimize(ripple::deserialize<std::string>(bytes));
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_DeserializeString);

// ---------------------------------------------------------------------------
// Keyed state
//
// Every access pays a serialize/deserialize round trip (D-038). That is the cost
// of a backend that can be swapped for a file- or disk-backed one and snapshotted
// without knowing the type; this measures what it actually costs.
// ---------------------------------------------------------------------------

void BM_ValueStateUpdate(benchmark::State& state) {
    ripple::MemoryStateBackend backend;
    ripple::ValueState<std::int64_t> value(backend, "v");
    backend.set_current_key(ripple::serialize(std::string{"key"}));

    std::int64_t counter = 0;
    for (auto _ : state) {
        value.update(counter++);
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_ValueStateUpdate);

void BM_ValueStateReadModifyWrite(benchmark::State& state) {
    ripple::MemoryStateBackend backend;
    ripple::AggregatingState<ripple::SumAggregator<std::int64_t>> total(backend, "total");
    backend.set_current_key(ripple::serialize(std::string{"key"}));

    for (auto _ : state) {
        total.add(1);
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_ValueStateReadModifyWrite);

/// Sweeping key count shows how state access degrades as the keyspace grows --
/// the ordered map is O(log n), and this is where that shows up.
void BM_KeyedStateAcrossManyKeys(benchmark::State& state) {
    ripple::MemoryStateBackend backend;
    ripple::AggregatingState<ripple::SumAggregator<std::int64_t>> total(backend, "total");

    const auto key_count = static_cast<std::size_t>(state.range(0));
    std::vector<std::vector<std::byte>> keys;
    keys.reserve(key_count);
    for (std::size_t i = 0; i < key_count; ++i) {
        keys.push_back(ripple::serialize("key-" + std::to_string(i)));
    }

    std::size_t index = 0;
    for (auto _ : state) {
        backend.set_current_key(keys[index++ % key_count]);
        total.add(1);
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_KeyedStateAcrossManyKeys)->Arg(1)->Arg(100)->Arg(10'000);

void BM_StateBackendSnapshot(benchmark::State& state) {
    ripple::MemoryStateBackend backend;
    ripple::ValueState<std::int64_t> value(backend, "v");
    const auto key_count = static_cast<std::size_t>(state.range(0));
    for (std::size_t i = 0; i < key_count; ++i) {
        backend.set_current_key(ripple::serialize("key-" + std::to_string(i)));
        value.update(static_cast<std::int64_t>(i));
    }

    for (auto _ : state) {
        ripple::ByteWriter writer;
        backend.write_snapshot(writer);
        benchmark::DoNotOptimize(writer.size());
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_StateBackendSnapshot)->Arg(100)->Arg(10'000);

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

void BM_KeyGroupHash(benchmark::State& state) {
    const auto key = ripple::serialize(std::string{"midtown-manhattan"});
    for (auto _ : state) {
        benchmark::DoNotOptimize(ripple::key_group_of(key));
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_KeyGroupHash);

// ---------------------------------------------------------------------------
// Window assignment
//
// The sliding case is the interesting one: assignment cost scales with
// size/slide, because a record genuinely belongs to that many windows at once.
// ---------------------------------------------------------------------------

void BM_TumblingAssign(benchmark::State& state) {
    const ripple::TumblingWindows assigner{ripple::Duration{60'000}};
    std::vector<ripple::TimeWindow> windows;
    std::int64_t millis = 0;
    for (auto _ : state) {
        windows.clear();
        assigner.assign(ripple::timestamp_from_millis(millis += 37), windows);
        benchmark::DoNotOptimize(windows.data());
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_TumblingAssign);

void BM_SlidingAssign(benchmark::State& state) {
    const ripple::SlidingWindows assigner{ripple::Duration{60'000},
                                          ripple::Duration{60'000 / state.range(0)}};
    std::vector<ripple::TimeWindow> windows;
    std::int64_t millis = 0;
    for (auto _ : state) {
        windows.clear();
        assigner.assign(ripple::timestamp_from_millis(millis += 37), windows);
        benchmark::DoNotOptimize(windows.data());
    }
    state.SetItemsProcessed(state.iterations());
    state.counters["windows_per_record"] = static_cast<double>(state.range(0));
}

BENCHMARK(BM_SlidingAssign)->Arg(2)->Arg(10)->Arg(60);

// ---------------------------------------------------------------------------
// Bounded queue
//
// The single-threaded numbers are the floor: mutex acquire, deque push, notify.
// The contended number is what actually matters, and the gap between them is the
// cost of handing work between threads.
// ---------------------------------------------------------------------------

void BM_QueuePushPopUncontended(benchmark::State& state) {
    ripple::BoundedQueue<std::int64_t> queue(1'024);
    for (auto _ : state) {
        (void)queue.push(1);
        benchmark::DoNotOptimize(queue.pop());
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_QueuePushPopUncontended);

void BM_QueueSingleProducerSingleConsumer(benchmark::State& state) {
    const auto capacity = static_cast<std::size_t>(state.range(0));

    for (auto _ : state) {
        state.PauseTiming();
        constexpr int kItems = 20'000;
        ripple::BoundedQueue<std::int64_t> queue(capacity);
        state.ResumeTiming();

        std::thread producer([&queue] {
            for (int i = 0; i < kItems; ++i) {
                (void)queue.push(i);
            }
            queue.close();
        });
        std::int64_t drained = 0;
        while (queue.pop().has_value()) {
            ++drained;
        }
        producer.join();
        benchmark::DoNotOptimize(drained);

        state.SetItemsProcessed(state.items_processed() + kItems);
    }
}

BENCHMARK(BM_QueueSingleProducerSingleConsumer)->Arg(8)->Arg(1'024)->UseRealTime();

// ---------------------------------------------------------------------------
// Single-threaded pipeline
//
// The per-record cost of the operator chain itself: one virtual call per
// operator plus the collector indirection (D-014). This is the number that would
// justify or refute that design choice.
// ---------------------------------------------------------------------------

void BM_SingleThreadedMapFilter(benchmark::State& state) {
    const auto record_count = static_cast<std::size_t>(state.range(0));

    for (auto _ : state) {
        state.PauseTiming();
        std::vector<ripple::Record<std::int64_t>> input;
        input.reserve(record_count);
        for (std::size_t i = 0; i < record_count; ++i) {
            input.push_back(
                ripple::make_record(static_cast<std::int64_t>(i),
                                    ripple::timestamp_from_millis(static_cast<std::int64_t>(i))));
        }
        auto sink = std::make_unique<ripple::CollectingSink<std::int64_t>>();
        auto pipeline =
            ripple::from(std::make_unique<ripple::VectorSource<std::int64_t>>(std::move(input)))
                .via(ripple::make_map<std::int64_t>([](std::int64_t v) { return v * 2; }))
                .via(ripple::make_filter<std::int64_t>([](std::int64_t v) { return v % 3 != 0; }))
                .to(std::move(sink));
        state.ResumeTiming();

        pipeline.run();
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_SingleThreadedMapFilter)->Arg(100'000)->UseRealTime();

void BM_SingleThreadedTumblingWindow(benchmark::State& state) {
    const auto record_count = static_cast<std::size_t>(state.range(0));

    for (auto _ : state) {
        state.PauseTiming();
        std::vector<ripple::Record<std::int64_t>> input;
        input.reserve(record_count);
        for (std::size_t i = 0; i < record_count; ++i) {
            input.push_back(ripple::make_record(
                std::int64_t{1}, ripple::timestamp_from_millis(static_cast<std::int64_t>(i) * 10)));
        }
        auto sink = std::make_unique<
            ripple::CollectingSink<ripple::WindowResult<ripple::GlobalWindowKey, std::int64_t>>>();
        auto pipeline =
            ripple::from(std::make_unique<ripple::VectorSource<std::int64_t>>(std::move(input)))
                .via(ripple::make_window<std::int64_t>(
                    ripple::TumblingWindows{ripple::Duration{1'000}},
                    ripple::SumAggregator<std::int64_t>{}))
                .to(std::move(sink));
        state.ResumeTiming();

        pipeline.run();
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_SingleThreadedTumblingWindow)->Arg(100'000)->UseRealTime();

} // namespace

BENCHMARK_MAIN();
