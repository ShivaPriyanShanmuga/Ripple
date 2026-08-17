#pragma once

#include <ripple/collector.hpp>
#include <ripple/concurrent/bounded_queue.hpp>
#include <ripple/concurrent/worker_group.hpp>
#include <ripple/operator.hpp>
#include <ripple/parallel/stream_element.hpp>
#include <ripple/record.hpp>
#include <ripple/sink.hpp>
#include <ripple/state/memory_state_backend.hpp>
#include <ripple/watermark.hpp>

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace ripple {

/// Pushes an operator's output into a queue instead of calling the next operator
/// directly.
///
/// ## This is the payoff of D-014
///
/// `Collector<T>` was defined in Stage 1 as an *interface* specifically so this
/// could exist. Every operator -- map, filter, window, keyed aggregate -- was
/// written against `Collector<T>&` and calls `out.collect(...)` exactly as
/// before. **Not one line of operator code changed to make the engine parallel.**
/// Had the Stage 1 chain been welded together with CRTP, there would be no seam
/// here to insert a queue into.
///
/// `collect` blocks when the queue is full, and that blocking *is* the
/// backpressure: a subtask that cannot hand its output on stops consuming its
/// own input, so its input queue fills, and the pressure propagates upstream on
/// its own.
template<typename T>
class QueueCollector final : public Collector<T> {
public:
    QueueCollector(BoundedQueue<StreamElement<T>>& queue, std::size_t channel) noexcept
        : queue_(&queue), channel_(channel) {}

    void collect(Record<T>&& record) override {
        // Blocks while the downstream queue is full. Nothing here signals,
        // measures, or negotiates -- the block is the entire mechanism.
        (void)queue_->push(StreamElement<T>{std::move(record)});
    }

    void emit_watermark(Watermark watermark) override {
        (void)queue_->push(StreamElement<T>{ChannelWatermark{channel_, watermark}});
    }

    /// Announces that this channel is finished. See `ChannelClosed` for why a
    /// fan-in queue cannot simply be closed by whichever producer finishes first.
    void close_channel() { (void)queue_->push(StreamElement<T>{ChannelClosed{channel_}}); }

private:
    BoundedQueue<StreamElement<T>>* queue_;
    std::size_t channel_;
};

struct ParallelConfig {
    std::size_t parallelism = 4;

    /// Deliberately small by default. A large capacity delays the onset of
    /// backpressure and hides a slow stage behind a deep buffer; it does not
    /// make the pipeline faster, it makes the problem later and less visible.
    std::size_t queue_capacity = 64;
};

/// What each queue and subtask actually did. The point of instrumenting this is
/// that backpressure and key skew are otherwise invisible -- the job simply runs
/// slower than expected with nothing obviously wrong.
struct ParallelMetrics {
    /// Records handled by each subtask. Wildly uneven entries mean **key skew**:
    /// hash partitioning sends every record for a given key to one subtask, so a
    /// single hot key caps the whole job at one core's throughput no matter how
    /// much parallelism is configured. Adding threads cannot fix it; only
    /// changing the key can.
    std::vector<std::size_t> records_per_subtask;

    /// How often the source blocked pushing into each subtask's input queue.
    std::vector<std::size_t> input_queue_push_blocks;

    /// How often a subtask blocked pushing into the sink's queue. A high value
    /// here with low values above localises the slow stage as the sink.
    std::size_t sink_queue_push_blocks = 0;

    [[nodiscard]] std::size_t total_input_push_blocks() const {
        std::size_t total = 0;
        for (const std::size_t blocks : input_queue_push_blocks) {
            total += blocks;
        }
        return total;
    }
};

/// Runs one operator across several threads, partitioned by key.
///
/// Topology:
/// ```
///   source (calling thread)
///        |  partition by hash(key) % parallelism
///        v
///   [q0] [q1] ... [qN-1]          bounded
///     |    |        |
///   sub0 sub1 ... subN-1          own operator + own state backend each
///     \    |        /
///      \   |       /  fan-in
///       [ sink queue ]            bounded
///             |
///          sink thread
/// ```
///
/// The source runs on the calling thread on purpose: when the pipeline is
/// saturated, `run()` itself blocks, which is backpressure reaching the caller.
///
/// Deliberately not a general DAG runner. The shape above exercises everything
/// this stage is about -- partitioned fan-out, fan-in with watermark merging,
/// and backpressure across two queue layers -- without an arbitrary-topology
/// scheduler that would be mostly bookkeeping.
template<typename In, typename Out, typename KeyHash, typename OperatorFactory>
class ParallelPipeline {
public:
    /// `key_hash` maps a payload to a hash. `make_operator(index, backend)`
    /// builds one operator per subtask.
    ///
    /// Each subtask gets its **own** operator instance and its **own** state
    /// backend. That is what makes keyed state lock-free (D-038): partitioning
    /// guarantees one subtask per key, so no two threads ever touch the same
    /// state and there is nothing to protect. Sharing one backend behind a mutex
    /// would not merely be slower, it would fail to scale -- contention grows
    /// with thread count.
    ParallelPipeline(ParallelConfig config, KeyHash key_hash, OperatorFactory make_operator)
        : config_(config), key_hash_(std::move(key_hash)),
          make_operator_(std::move(make_operator)) {}

    /// Runs to completion, then returns. Blocks the calling thread.
    void run(std::vector<Record<In>> input, Sink<Out>& sink) {
        const std::size_t parallelism = config_.parallelism;

        std::vector<std::unique_ptr<BoundedQueue<StreamElement<In>>>> input_queues;
        input_queues.reserve(parallelism);
        for (std::size_t i = 0; i < parallelism; ++i) {
            // One heap allocation per queue, so each queue's mutex and counters
            // sit on their own cache lines rather than sharing one with a
            // neighbouring queue's. Two subtasks hammering adjacent mutexes on a
            // single line would ping-pong that line between cores -- false
            // sharing, which looks like contention on data the threads never
            // actually share. Whether padding *within* the queue matters is a
            // Stage 9 measurement, not a guess to make here.
            input_queues.push_back(
                std::make_unique<BoundedQueue<StreamElement<In>>>(config_.queue_capacity));
        }

        BoundedQueue<StreamElement<Out>> sink_queue(config_.queue_capacity);

        std::vector<std::unique_ptr<MemoryStateBackend>> backends;
        std::vector<std::unique_ptr<Operator<In, Out>>> operators;
        backends.reserve(parallelism);
        operators.reserve(parallelism);
        for (std::size_t i = 0; i < parallelism; ++i) {
            backends.push_back(std::make_unique<MemoryStateBackend>());
            operators.push_back(make_operator_(i, *backends.back()));
        }

        std::vector<std::size_t> records_per_subtask(parallelism, 0);

        {
            WorkerGroup workers;

            for (std::size_t i = 0; i < parallelism; ++i) {
                workers.spawn("subtask-" + std::to_string(i), [&, i](const std::stop_token&) {
                    run_subtask(i, *input_queues[i], sink_queue, *operators[i],
                                records_per_subtask[i]);
                });
            }

            workers.spawn("sink",
                          [&](const std::stop_token&) { run_sink(parallelism, sink_queue, sink); });

            // --- source, on the calling thread ---------------------------
            for (Record<In>& record : input) {
                const std::size_t partition = key_hash_(record.value) % parallelism;
                (void)input_queues[partition]->push(StreamElement<In>{std::move(record)});
            }

            // Broadcast, not partitioned. A watermark is a statement about time
            // and applies to every subtask; routing it to one would leave the
            // others frozen in event time, so their windows would never fire.
            for (auto& queue : input_queues) {
                (void)queue->push(StreamElement<In>{ChannelWatermark{0, kEndOfStreamWatermark}});
            }

            // Shutdown, in the order D-045 requires: close the queues first, so
            // blocked workers wake, and only then let WorkerGroup's destructor
            // stop and join. Reversing it hangs.
            //
            // Closing is sufficient on the *input* side because each of these
            // queues has exactly one producer -- this thread. The sink queue has
            // N producers and must instead be terminated by counting
            // ChannelClosed elements.
            for (auto& queue : input_queues) {
                queue->close();
            }
        } // workers joined here

        // Only now. `records_per_subtask` is written by the subtask threads, so
        // reading it before the join above would be a data race -- and the queue
        // counters would be a mid-flight snapshot rather than the final totals.
        collect_metrics(input_queues, sink_queue, records_per_subtask);
    }

    [[nodiscard]] const ParallelMetrics& metrics() const noexcept { return metrics_; }

private:
    static void run_subtask(std::size_t index, BoundedQueue<StreamElement<In>>& input,
                            BoundedQueue<StreamElement<Out>>& output, Operator<In, Out>& op,
                            std::size_t& record_count) {
        QueueCollector<Out> collector(output, index);

        while (std::optional<StreamElement<In>> element = input.pop()) {
            std::visit(
                [&](auto&& alternative) {
                    using Alternative = std::decay_t<decltype(alternative)>;
                    if constexpr (std::is_same_v<Alternative, Record<In>>) {
                        ++record_count;
                        op.process(std::forward<decltype(alternative)>(alternative), collector);
                    } else if constexpr (std::is_same_v<Alternative, ChannelWatermark>) {
                        op.on_watermark(alternative.watermark, collector);
                    }
                    // ChannelClosed is unused on the input side: these queues
                    // have a single producer and are terminated by close().
                },
                std::move(*element));
        }

        // Tell the fan-in consumer this channel is finished. Skipping this
        // leaves the sink waiting forever for a count it will never reach.
        collector.close_channel();
    }

    static void run_sink(std::size_t parallelism, BoundedQueue<StreamElement<Out>>& queue,
                         Sink<Out>& sink) {
        // The fan-in point, and the only place the min-across-channels rule from
        // Stage 2 actually bites. Each subtask advances in event time
        // independently; the sink may claim only as much progress as its
        // *slowest* input has made.
        WatermarkTracker tracker(parallelism);
        std::size_t open_channels = parallelism;

        while (std::optional<StreamElement<Out>> element = queue.pop()) {
            bool finished = false;
            std::visit(
                [&](auto&& alternative) {
                    using Alternative = std::decay_t<decltype(alternative)>;
                    if constexpr (std::is_same_v<Alternative, Record<Out>>) {
                        // Only this one thread ever touches the sink, which is
                        // why no lock is needed around it.
                        sink.write(std::forward<decltype(alternative)>(alternative));
                    } else if constexpr (std::is_same_v<Alternative, ChannelWatermark>) {
                        if (const std::optional<Watermark> combined =
                                tracker.update(alternative.channel, alternative.watermark)) {
                            sink.on_watermark(*combined);
                        }
                    } else {
                        --open_channels;
                        finished = open_channels == 0;
                    }
                },
                std::move(*element));

            if (finished) {
                break;
            }
        }
        queue.close();
    }

    void collect_metrics(
        const std::vector<std::unique_ptr<BoundedQueue<StreamElement<In>>>>& input_queues,
        const BoundedQueue<StreamElement<Out>>& sink_queue,
        const std::vector<std::size_t>& records_per_subtask) {
        metrics_.input_queue_push_blocks.clear();
        metrics_.input_queue_push_blocks.reserve(input_queues.size());
        for (const auto& queue : input_queues) {
            metrics_.input_queue_push_blocks.push_back(queue->push_block_count());
        }
        metrics_.sink_queue_push_blocks = sink_queue.push_block_count();
        metrics_.records_per_subtask = records_per_subtask;
    }

    ParallelConfig config_;
    KeyHash key_hash_;
    OperatorFactory make_operator_;
    ParallelMetrics metrics_;
};

template<typename In, typename Out, typename KeyHash, typename OperatorFactory>
[[nodiscard]] auto make_parallel_pipeline(ParallelConfig config, KeyHash key_hash,
                                          OperatorFactory make_operator) {
    return ParallelPipeline<In, Out, KeyHash, OperatorFactory>(config, std::move(key_hash),
                                                               std::move(make_operator));
}

} // namespace ripple
