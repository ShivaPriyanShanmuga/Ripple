#pragma once

#include <ripple/checkpoint/checkpoint_coordinator.hpp>
#include <ripple/collector.hpp>
#include <ripple/concurrent/bounded_queue.hpp>
#include <ripple/concurrent/worker_group.hpp>
#include <ripple/operator.hpp>
#include <ripple/parallel/stream_element.hpp>
#include <ripple/record.hpp>
#include <ripple/serialization.hpp>
#include <ripple/sink.hpp>
#include <ripple/state/key_group.hpp>
#include <ripple/state/memory_state_backend.hpp>
#include <ripple/state/state_backend.hpp>
#include <ripple/watermark.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace ripple {

/// Pushes an operator's output into a queue instead of calling the next operator
/// directly.
///
/// ## The payoff of D-014
///
/// `Collector<T>` was defined in Stage 1 as an *interface* precisely so this
/// could exist. Swapping it in made the engine parallel and **not one line of
/// operator code changed** -- map, filter, window and keyed aggregate are all
/// still written against `Collector<T>&` and still just call `out.collect(...)`.
/// A compile-time welded chain (the CRTP design rejected in D-014) would have no
/// seam here at all.
///
/// `collect` blocking on a full queue *is* the backpressure. Nothing in this
/// design signals, measures, or negotiates.
template<typename T>
class QueueCollector final : public Collector<T> {
public:
    QueueCollector(BoundedQueue<StreamElement<T>>& queue, std::size_t channel) noexcept
        : queue_(&queue), channel_(channel) {}

    void collect(Record<T>&& record) override {
        (void)queue_->push(StreamElement<T>{channel_, std::move(record)});
    }

    void emit_watermark(Watermark watermark) override {
        (void)queue_->push(StreamElement<T>{channel_, watermark});
    }

    /// Forwards a checkpoint barrier downstream, in line with the records.
    void emit_barrier(CheckpointBarrier barrier) {
        (void)queue_->push(StreamElement<T>{channel_, barrier});
    }

    void close_channel() { (void)queue_->push(StreamElement<T>{channel_, EndOfChannel{}}); }

private:
    BoundedQueue<StreamElement<T>>* queue_;
    std::size_t channel_;
};

struct ParallelConfig {
    std::size_t parallelism = 4;

    /// Deliberately small. A large capacity delays the onset of backpressure and
    /// hides a slow stage behind a deep buffer; it does not make the pipeline
    /// faster, only the problem later and less visible.
    std::size_t queue_capacity = 64;

    /// Inject a checkpoint barrier every N source records. Zero disables
    /// checkpointing entirely.
    std::size_t checkpoint_interval_records = 0;
};

/// Everything a run needs beyond its input, sink and coordinator.
/// Records emitted per pacing sleep.
///
/// Exposed rather than kept private because a harness measuring latency has to
/// compute the *same* schedule the source paced against. All records in a batch
/// enter the pipeline together at the batch's due time, so measuring each one
/// against its own nominal slot reports negative latency for everything but the
/// first -- an artifact, not a fast pipeline. One definition, used by both.
[[nodiscard]] constexpr std::size_t pacing_batch_size(std::size_t records_per_second) noexcept {
    return records_per_second / 1'000 > 0 ? records_per_second / 1'000 : 1;
}

struct RunOptions {
    /// Restore from this checkpoint instead of starting fresh. Each subtask's
    /// state backend is repopulated from it and the source rewinds to its
    /// offset.
    ///
    /// The parallelism **may differ** from the run that produced the
    /// checkpoint. Keys are routed by key group (a fixed number of hash buckets,
    /// independent of parallelism), so a rescale redistributes ranges of groups
    /// rather than rehashing keys. Each subtask reads every old snapshot and
    /// keeps the groups it now owns.
    ///
    /// The one thing that does not survive a rescale is *operator* state, which
    /// is restored only when the parallelism is unchanged. Redistributing it
    /// needs a per-operator merge rule -- Flink's union-list and split-list
    /// redistribution -- and none of this engine's operators hold any, so the
    /// machinery would be untested weight.
    const CompletedCheckpoint* restore_from = nullptr;

    /// Simulate a crash: stop emitting after this many records.
    ///
    /// Fidelity limit, stated plainly. All in-memory state is discarded (the
    /// backends are destroyed when `run` returns), which is the part that
    /// matters. What it does *not* simulate is losing records already in flight,
    /// because terminating threads mid-operation is not something that can be
    /// done safely in-process. That does not weaken the property under test:
    /// recovery replays from the last completed checkpoint's offset, which
    /// re-delivers exactly the records a graceful drain may have delivered.
    std::optional<std::size_t> fail_after_records = std::nullopt;

    /// Feed the source at a fixed rate rather than flat out. Zero means
    /// unlimited, which is what throughput measurement wants.
    ///
    /// Latency measured on a *saturated* pipeline is dominated by queueing and
    /// grows with input size -- it measures how deep the backlog got, not how
    /// long a record takes to get through. Pacing below saturation is what makes
    /// a latency percentile mean the latter, which is the number anyone actually
    /// wants. Reporting a saturated latency as if it were the second is one of
    /// the more common ways benchmarks mislead.
    std::size_t target_records_per_second = 0;
};

struct ParallelMetrics {
    /// Records handled by each subtask. Wildly uneven entries mean **key skew**:
    /// hash partitioning sends every record for a key to one subtask, so a single
    /// hot key caps the job at one core's throughput. Adding threads cannot fix
    /// it; only changing the key can.
    std::vector<std::size_t> records_per_subtask;
    std::vector<std::size_t> input_queue_push_blocks;
    std::size_t sink_queue_push_blocks = 0;

    /// When the source began emitting.
    ///
    /// Exposed because a latency measurement has to use the same clock origin
    /// the source paced against. Timing from before `run()` folds pipeline
    /// setup -- thread spawn, backend construction -- into every single record's
    /// latency as a constant offset, which then swamps the number being
    /// measured and looks like a uniformly slow pipeline.
    std::chrono::steady_clock::time_point source_started_at{};

    /// Workers that terminated by throwing. **Empty means the run completed.**
    ///
    /// Exposed because the alternative is a failed run that looks like a
    /// successful one: the engine now shuts down cleanly when an operator
    /// throws instead of deadlocking, and without this the caller could not tell
    /// that outcome from a normal finish.
    std::vector<WorkerFailure> worker_failures;

    /// How many elements the sink set aside while waiting for a barrier to
    /// arrive on every channel. This is the **cost of alignment** made visible: a
    /// large number means one subtask is lagging and the rest are stalled on it.
    std::size_t alignment_buffered_elements = 0;

    [[nodiscard]] std::size_t total_input_push_blocks() const {
        std::size_t total = 0;
        for (const std::size_t blocks : input_queue_push_blocks) {
            total += blocks;
        }
        return total;
    }
};

/// Runs one operator across several threads, partitioned by key, with optional
/// asynchronous barrier snapshotting.
///
/// ```
///   source (calling thread)   -- injects barriers, broadcast
///        |  partition by hash(key) % parallelism
///        v
///   [q0] [q1] ... [qN-1]          bounded
///     |    |        |
///   sub0 sub1 ... subN-1          own operator + own state backend each
///     \    |        /
///      \   |       /  fan-in -- the only multi-input operator, so the only
///       [ sink queue ]            place alignment happens
///             |
///          sink thread
/// ```
template<typename In, typename Out, typename KeySelector, typename OperatorFactory>
class ParallelPipeline {
public:
    ParallelPipeline(ParallelConfig config, KeySelector key_selector, OperatorFactory make_operator)
        : config_(config), key_selector_(std::move(key_selector)),
          make_operator_(std::move(make_operator)) {}

    /// Runs to completion. If `coordinator` is non-null it must have been
    /// constructed with `parallelism + 1` tasks -- one per subtask, plus the sink.
    void run(std::vector<Record<In>> input, Sink<Out>& sink,
             CheckpointCoordinator* coordinator = nullptr, const RunOptions& options = {}) {
        const std::size_t parallelism = config_.parallelism;

        std::vector<std::unique_ptr<BoundedQueue<StreamElement<In>>>> input_queues;
        input_queues.reserve(parallelism);
        for (std::size_t i = 0; i < parallelism; ++i) {
            // One heap allocation per queue, so each queue's mutex and counters
            // sit on their own cache lines rather than sharing one with a
            // neighbour's. Two subtasks hammering adjacent mutexes on a single
            // line would ping-pong it between cores -- false sharing, which looks
            // like contention on data the threads never actually share.
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

            if (options.restore_from == nullptr) {
                continue;
            }
            restore_subtask(i, parallelism, *options.restore_from, *backends[i], *operators[i]);
        }

        std::vector<std::size_t> records_per_subtask(parallelism, 0);
        SinkTask sink_task(parallelism, parallelism, coordinator);
        std::vector<WorkerFailure> failures;

        {
            WorkerGroup workers;

            for (std::size_t i = 0; i < parallelism; ++i) {
                workers.spawn("subtask-" + std::to_string(i), [&, i](const std::stop_token&) {
                    run_subtask(i, *input_queues[i], sink_queue, *operators[i], *backends[i],
                                records_per_subtask[i], coordinator);
                });
            }

            workers.spawn("sink", [&](const std::stop_token&) { sink_task.run(sink_queue, sink); });

            // --- source, on the calling thread -------------------------------
            //
            // Rewinding to the checkpoint's offset is what makes the source
            // *replayable*, and it is half of end-to-end exactly-once. A source
            // that cannot be rewound -- a UDP socket, say -- makes the property
            // unachievable no matter how correct the engine is: the data is
            // simply gone.
            std::size_t offset =
                options.restore_from != nullptr ? options.restore_from->source_offset : 0;
            const std::size_t start = offset;

            const auto run_started_at = std::chrono::steady_clock::now();
            source_started_at_ = run_started_at;

            for (std::size_t index = start; index < input.size(); ++index) {
                if (options.fail_after_records.has_value() &&
                    offset >= *options.fail_after_records) {
                    break;
                }

                if (options.target_records_per_second > 0) {
                    // Paced in batches, against an absolute schedule.
                    //
                    // Two things are deliberate here, and getting either wrong
                    // silently produces a benchmark that measures nothing.
                    //
                    // *Absolute* schedule from the run's start rather than a
                    // sleep per record: a fixed per-record sleep accumulates the
                    // scheduler's overshoot on every iteration and drifts badly
                    // over a long run.
                    //
                    // *Batched* because a sleep shorter than the scheduler's
                    // granularity (~50us on Linux) overshoots by an order of
                    // magnitude. At 200k rec/s the per-record interval is 5us,
                    // so sleeping per record puts the deadline permanently in
                    // the past, `sleep_until` returns immediately every time,
                    // and pacing degrades into running flat out -- while the
                    // harness still reports the result as "paced latency". That
                    // is a benchmark quietly measuring saturation instead.
                    // Sleeping roughly every millisecond's worth of records
                    // keeps each sleep well above the granularity floor.
                    const std::size_t batch = pacing_batch_size(options.target_records_per_second);
                    if ((index - start) % batch == 0) {
                        const auto due = run_started_at + std::chrono::nanoseconds{
                                                              (index - start) * 1'000'000'000ULL /
                                                              options.target_records_per_second};
                        std::this_thread::sleep_until(due);
                    }
                }

                Record<In>& record = input[index];
                // Route by key group, never by `hash % parallelism` directly.
                // A key's group never changes; only which subtask owns that
                // group does, which is what makes a rescale possible at all.
                const StateKey key = serialize(key_selector_(record.value));
                const std::size_t partition = subtask_for_key_group(key_group_of(key), parallelism);
                (void)input_queues[partition]->push(StreamElement<In>{0, std::move(record)});
                ++offset;

                if (config_.checkpoint_interval_records > 0 && coordinator != nullptr &&
                    offset % config_.checkpoint_interval_records == 0) {
                    // The source records its own position *before* the barrier
                    // enters the stream, so the checkpoint's offset and every
                    // snapshot downstream describe the same cut.
                    const CheckpointId id = coordinator->trigger(offset, parallelism);

                    // Broadcast, not partitioned -- exactly like a watermark. A
                    // barrier is an instruction to every task; routing it to one
                    // subtask would leave the others never snapshotting, and the
                    // checkpoint could never complete.
                    for (auto& queue : input_queues) {
                        (void)queue->push(StreamElement<In>{0, CheckpointBarrier{id}});
                    }
                }
            }

            // Announce end of stream only on a clean finish. A simulated crash
            // must not tell downstream that time has advanced to infinity --
            // that would fire every open window on the way out, which is exactly
            // what a crash does not do.
            if (!options.fail_after_records.has_value()) {
                for (auto& queue : input_queues) {
                    (void)queue->push(StreamElement<In>{0, kEndOfStreamWatermark});
                }
            }

            // Shutdown in the order D-045 requires: close the queues so blocked
            // workers wake, then let WorkerGroup's destructor stop and join.
            // Closing suffices here because each input queue has exactly one
            // producer -- this thread. The sink queue has N and is instead
            // terminated by counting EndOfChannel elements.
            for (auto& queue : input_queues) {
                queue->close();
            }

            // Joined explicitly rather than left to the destructor: the failure
            // list is only meaningful once every worker has finished, and
            // reading it first reports an empty list for a run that failed.
            // `join()` is idempotent, so the destructor's join is a no-op.
            workers.join();
            failures = workers.failures();
        } // workers joined here

        metrics_.worker_failures = std::move(failures);
        collect_metrics(input_queues, sink_queue, records_per_subtask, sink_task);
    }

    [[nodiscard]] const ParallelMetrics& metrics() const noexcept { return metrics_; }

private:
    /// Terminates a subtask's channels on **every** exit path, including the one
    /// an exception takes.
    ///
    /// Without this the engine deadlocks when an operator throws, in two
    /// separate places:
    ///
    ///   1. the fan-in sink counts one `EndOfChannel` per channel before it
    ///      exits, so a channel that never announces itself leaves the sink
    ///      waiting forever -- and `WorkerGroup`'s destructor then joins that
    ///      thread;
    ///   2. the source keeps pushing into the dead subtask's input queue until
    ///      it fills, and then blocks in `push` forever.
    ///
    /// Both are the same mistake as a hand-written `join()` on the happy path
    /// only (D-046): cleanup that is written as a statement rather than as a
    /// destructor is cleanup that the exception path skips.
    ///
    /// The input queue is closed *first* so the source unblocks promptly rather
    /// than waiting on the downstream announcement.
    class SubtaskExitGuard {
    public:
        SubtaskExitGuard(BoundedQueue<StreamElement<In>>& input,
                         QueueCollector<Out>& collector) noexcept
            : input_(&input), collector_(&collector) {}

        ~SubtaskExitGuard() {
            input_->close();
            collector_->close_channel();
        }

        SubtaskExitGuard(const SubtaskExitGuard&) = delete;
        SubtaskExitGuard& operator=(const SubtaskExitGuard&) = delete;
        SubtaskExitGuard(SubtaskExitGuard&&) = delete;
        SubtaskExitGuard& operator=(SubtaskExitGuard&&) = delete;

    private:
        BoundedQueue<StreamElement<In>>* input_;
        QueueCollector<Out>* collector_;
    };

    // -----------------------------------------------------------------------
    // Subtask
    // -----------------------------------------------------------------------
    static void run_subtask(std::size_t index, BoundedQueue<StreamElement<In>>& input,
                            BoundedQueue<StreamElement<Out>>& output, Operator<In, Out>& op,
                            StateBackend& backend, std::size_t& record_count,
                            CheckpointCoordinator* coordinator) {
        QueueCollector<Out> collector(output, index);
        const SubtaskExitGuard guard(input, collector);

        while (std::optional<StreamElement<In>> element = input.pop()) {
            std::visit(
                [&](auto&& payload) {
                    using Payload = std::decay_t<decltype(payload)>;
                    if constexpr (std::is_same_v<Payload, Record<In>>) {
                        ++record_count;
                        op.process(std::forward<decltype(payload)>(payload), collector);
                    } else if constexpr (std::is_same_v<Payload, Watermark>) {
                        op.on_watermark(payload, collector);
                    } else if constexpr (std::is_same_v<Payload, CheckpointBarrier>) {
                        snapshot_and_acknowledge(payload, index, op, backend, collector,
                                                 coordinator);
                    }
                    // EndOfChannel is unused on the input side: these queues have
                    // a single producer and are terminated by close().
                },
                std::move(element->payload));
        }
    }

    /// The heart of the algorithm, from one task's point of view.
    ///
    /// The barrier arrived **in line with the records**, so every record before
    /// it has already been processed by this task and none after it has. That is
    /// the whole trick: this task's state right now reflects exactly the prefix
    /// of the stream ahead of the barrier, and every other task independently
    /// reaches the same conclusion about its own prefix. Add the snapshots up and
    /// they describe one consistent cut -- with nobody ever having stopped.
    ///
    /// ## Snapshotting without stopping the world
    ///
    /// This serializes synchronously, on the task's own thread, with **no lock at
    /// all**. Not a shortcut: partitioning (D-050) means no other thread can
    /// touch this task's state, so there is nothing to exclude. The only cost is
    /// that this one subtask stalls while serializing; every other subtask keeps
    /// running and no global pause ever occurs.
    ///
    /// Rejected alternatives, which start to matter once state is large enough
    /// for the stall to show:
    ///   - **Brief locking** -- take a lock, serialize, release. Pointless here:
    ///     there is no second thread to lock against.
    ///   - **Double-buffering** -- keep two copies, swap, serialize the inactive
    ///     one on a background thread. Removes the stall by *doubling*
    ///     steady-state memory, which for a keyed job with large state is the
    ///     dominant cost.
    ///   - **Copy-on-write** -- snapshot logically and pay only for state mutated
    ///     during the snapshot. Best asymptotics, by far the most complex: with
    ///     no OS page-level support it means a persistent data structure for every
    ///     state type, pushing a heavy constraint into `StateBackend` for a stall
    ///     nobody has measured.
    ///
    /// Synchronous serialization is the right default at the state sizes this
    /// engine handles. Stage 9 is where a benchmark would say otherwise.
    static void snapshot_and_acknowledge(CheckpointBarrier barrier, TaskId task,
                                         const Operator<In, Out>& op, const StateBackend& backend,
                                         QueueCollector<Out>& collector,
                                         CheckpointCoordinator* coordinator) {
        // Forward first, snapshot second.
        //
        // The cut is identical either way -- no record is processed in between --
        // but forwarding first lets downstream begin aligning while this task is
        // still serializing, which keeps end-to-end checkpoint duration close to
        // the *slowest single task* rather than the sum of all of them. This is
        // what real engines do.
        collector.emit_barrier(barrier);

        if (coordinator == nullptr) {
            return;
        }

        ByteWriter writer;
        backend.write_snapshot(writer);
        op.snapshot_state(writer);
        coordinator->acknowledge(barrier.checkpoint_id, task, std::move(writer).take());
    }

    // -----------------------------------------------------------------------
    // Sink -- the only multi-input operator, so the only place alignment happens
    // -----------------------------------------------------------------------
    class SinkTask {
    public:
        class QueueCloser {
        public:
            explicit QueueCloser(BoundedQueue<StreamElement<Out>>& queue) noexcept
                : queue_(&queue) {}

            ~QueueCloser() { queue_->close(); }

            QueueCloser(const QueueCloser&) = delete;
            QueueCloser& operator=(const QueueCloser&) = delete;
            QueueCloser(QueueCloser&&) = delete;
            QueueCloser& operator=(QueueCloser&&) = delete;

        private:
            BoundedQueue<StreamElement<Out>>* queue_;
        };

        SinkTask(std::size_t channels, TaskId task_id, CheckpointCoordinator* coordinator)
            : channels_(channels), task_id_(task_id), coordinator_(coordinator), tracker_(channels),
              barrier_seen_(channels, false), channel_closed_(channels, false), buffered_(channels),
              open_channels_(channels) {}

        void run(BoundedQueue<StreamElement<Out>>& queue, Sink<Out>& sink) {
            // Closing on every exit path, for the mirror-image reason: a sink
            // that throws would otherwise leave every subtask blocked pushing
            // into a queue nobody drains.
            const QueueCloser closer(queue);

            while (true) {
                std::optional<StreamElement<Out>> element = next(queue);
                if (!element.has_value()) {
                    break;
                }

                const std::size_t channel = element->channel;

                // ## Barrier alignment
                //
                // Once a channel has delivered barrier N, everything after it on
                // that channel belongs *after* the cut and must not touch the
                // state being snapshotted. So it is set aside and replayed once
                // every channel has caught up.
                //
                // **What alignment buys**: the snapshot reflects exactly the
                // pre-barrier prefix of every input. Skip it, process the fast
                // channel's post-barrier records, and those records are baked into
                // the snapshot *and* replayed after recovery -- counted twice.
                // That is exactly the difference between aligned (exactly-once)
                // and unaligned (at-least-once) checkpointing.
                //
                // **What it costs**: latency. The operator waits for its slowest
                // input.
                //
                // Flink instead stops *reading* the channel, which backpressures
                // the sender. Buffering here produces the identical cut, and
                // therefore identical semantics, trading that backpressure for
                // memory held during alignment. `alignment_buffered_elements`
                // exists so the trade is visible rather than assumed.
                if (aligning_.has_value() && barrier_seen_[channel]) {
                    ++buffered_count_;
                    buffered_[channel].push_back(std::move(*element));
                    continue;
                }

                if (dispatch(std::move(*element), channel, sink)) {
                    break;
                }
            }
        }

        [[nodiscard]] std::size_t buffered_count() const noexcept { return buffered_count_; }

    private:
        std::optional<StreamElement<Out>> next(BoundedQueue<StreamElement<Out>>& queue) {
            // Replayed elements first: they arrived before anything still sitting
            // in the queue and must keep their place in the stream.
            if (!pending_.empty()) {
                StreamElement<Out> element = std::move(pending_.front());
                pending_.pop_front();
                return element;
            }
            return queue.pop();
        }

        /// Returns true when the stream is over.
        // The payload is moved out of `element`; the check does not track moves
        // of subobjects.
        // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
        bool dispatch(StreamElement<Out>&& element, std::size_t channel, Sink<Out>& sink) {
            bool finished = false;
            std::visit(
                [&](auto&& payload) {
                    using Payload = std::decay_t<decltype(payload)>;
                    if constexpr (std::is_same_v<Payload, Record<Out>>) {
                        // Only this thread ever touches the sink, hence no lock.
                        ++records_written_;
                        sink.write(std::forward<decltype(payload)>(payload));
                    } else if constexpr (std::is_same_v<Payload, Watermark>) {
                        if (const std::optional<Watermark> combined =
                                tracker_.update(channel, payload)) {
                            sink.on_watermark(*combined);
                        }
                    } else if constexpr (std::is_same_v<Payload, CheckpointBarrier>) {
                        on_barrier(payload, channel);
                    } else {
                        channel_closed_[channel] = true;
                        --open_channels_;
                        // A channel that will never send another barrier must not
                        // hold alignment open forever.
                        if (aligning_.has_value()) {
                            complete_alignment_if_ready();
                        }
                        finished = open_channels_ == 0;
                    }
                },
                std::move(element.payload));
            return finished;
        }

        void on_barrier(CheckpointBarrier barrier, std::size_t channel) {
            if (!aligning_.has_value()) {
                aligning_ = barrier.checkpoint_id;
                std::fill(barrier_seen_.begin(), barrier_seen_.end(), false);
            }
            barrier_seen_[channel] = true;
            complete_alignment_if_ready();
        }

        void complete_alignment_if_ready() {
            if (!aligning_.has_value()) {
                return;
            }
            for (std::size_t channel = 0; channel < channels_; ++channel) {
                if (!barrier_seen_[channel] && !channel_closed_[channel]) {
                    return;
                }
            }

            // Every input has reached the barrier, so this sink's state now
            // reflects exactly the pre-barrier prefix of every channel.
            if (coordinator_ != nullptr) {
                ByteWriter writer;
                Serializer<std::size_t>::write(writer, records_written_);
                coordinator_->acknowledge(*aligning_, task_id_, std::move(writer).take());
            }
            aligning_.reset();

            // Replay what was set aside; those elements belong to the next epoch.
            for (std::deque<StreamElement<Out>>& channel_buffer : buffered_) {
                for (StreamElement<Out>& element : channel_buffer) {
                    pending_.push_back(std::move(element));
                }
                channel_buffer.clear();
            }
        }

        std::size_t channels_;
        TaskId task_id_;
        CheckpointCoordinator* coordinator_;
        WatermarkTracker tracker_;

        std::optional<CheckpointId> aligning_;
        std::vector<bool> barrier_seen_;
        std::vector<bool> channel_closed_;
        std::vector<std::deque<StreamElement<Out>>> buffered_;
        std::deque<StreamElement<Out>> pending_;

        std::size_t open_channels_;
        std::size_t records_written_ = 0;
        std::size_t buffered_count_ = 0;
    };

    void collect_metrics(
        const std::vector<std::unique_ptr<BoundedQueue<StreamElement<In>>>>& input_queues,
        const BoundedQueue<StreamElement<Out>>& sink_queue,
        const std::vector<std::size_t>& records_per_subtask, const SinkTask& sink_task) {
        metrics_.input_queue_push_blocks.clear();
        metrics_.input_queue_push_blocks.reserve(input_queues.size());
        for (const auto& queue : input_queues) {
            metrics_.input_queue_push_blocks.push_back(queue->push_block_count());
        }
        metrics_.sink_queue_push_blocks = sink_queue.push_block_count();
        metrics_.records_per_subtask = records_per_subtask;
        metrics_.alignment_buffered_elements = sink_task.buffered_count();
        metrics_.source_started_at = source_started_at_;
    }

    /// Assembles one subtask's state from a checkpoint, which may have been
    /// taken at a different parallelism.
    ///
    /// Every old snapshot is read and each subtask keeps only the key groups it
    /// now owns. At unchanged parallelism this degenerates to "subtask i reads
    /// blob i and keeps everything", which is why there is no separate path for
    /// the common case.
    static void restore_subtask(std::size_t subtask, std::size_t parallelism,
                                const CompletedCheckpoint& checkpoint, StateBackend& backend,
                                Operator<In, Out>& op) {
        const KeyGroupRange range = key_group_range_for(subtask, parallelism);
        backend.clear();

        for (const auto& [task, blob] : checkpoint.task_state) {
            if (task >= checkpoint.parallelism) {
                continue; // the sink's snapshot, not a keyed subtask's
            }
            ByteReader reader(blob);
            backend.merge_snapshot(reader, range);

            // Operator state is written after the backend's, by the same task,
            // and is offered to *every* subtask with the range it now owns. An
            // operator whose state is keyed -- a window operator's is -- keeps
            // the groups it owns and drops the rest, so it redistributes exactly
            // like backend state. One whose state is genuinely unkeyed ignores
            // the range and restores nothing on a rescale.
            op.restore_state(reader, range);
        }
    }

    ParallelConfig config_;
    KeySelector key_selector_;
    OperatorFactory make_operator_;
    ParallelMetrics metrics_;
    std::chrono::steady_clock::time_point source_started_at_{};
};

/// `key_selector` maps a payload to its key. The **same** function the keyed
/// operator uses -- passing a separate hash function would let the two disagree,
/// which would route records to a subtask that does not hold their key's state
/// and split the results silently.
template<typename In, typename Out, typename KeySelector, typename OperatorFactory>
[[nodiscard]] auto make_parallel_pipeline(ParallelConfig config, KeySelector key_selector,
                                          OperatorFactory make_operator) {
    return ParallelPipeline<In, Out, KeySelector, OperatorFactory>(config, std::move(key_selector),
                                                                   std::move(make_operator));
}

} // namespace ripple
