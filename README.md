# Ripple

A stream-processing engine in C++20, built from scratch to understand how one
actually works — event time and watermarks, windowing, keyed state,
multi-threaded parallelism with backpressure, Chandy-Lamport checkpointing, and
exactly-once recovery.

Roughly 9,800 lines. 155 tests, every one of them green under ThreadSanitizer,
AddressSanitizer, UndefinedBehaviorSanitizer, Clang and GCC.

---

## What this is, and what it isn't

This is a learning project that implements the interesting half of Apache Flink:
the algorithms. It is **not** faster than Flink, not a replacement for it, and
missing most of what makes Flink a product. Where a real engine and this one
differ, the difference is documented rather than glossed.

The engine costs about **47× a hand-written loop** doing the same aggregation.
That number is in here because it is the honest one — see
[Benchmarks](#benchmarks).

What it does implement, properly:

| | |
| --- | --- |
| **Event time** | Watermarks generated at the source, propagated in-band, merged by minimum across input channels |
| **Windowing** | Tumbling, sliding, and session windows with merging; per-key state; allowed lateness and a side output for late data |
| **Keyed state** | Value / list / aggregating state over a pluggable backend (in-memory and file-backed), hand-written serialization |
| **Parallelism** | Multiple subtasks per operator, hash partitioning by key group, bounded queues, backpressure |
| **Checkpointing** | Chandy-Lamport asynchronous barrier snapshotting with alignment at multi-input operators |
| **Recovery** | Restore from checkpoint, rewind the source, idempotent sink; verified by fault injection |
| **Rescaling** | Key groups, so a job restores at a different parallelism without losing state |

---

## Quick start

```bash
# Prerequisites: CMake >= 3.20, Ninja, Clang 18 (or GCC 13)
sudo apt install ninja-build clang clang-tidy clang-format

cmake --preset release
cmake --build --preset release

# The demo: per-zone revenue in 5-minute windows, driver sessionization,
# then a mid-stream kill and recovery.
./build/release/bin/ripple_taxi_demo

# Tests, under any of dev / dev-gcc / asan / tsan
cmake --preset tsan && cmake --build --preset tsan && ctest --preset tsan
```

> **On WSL2 / Ubuntu 24.04**, ThreadSanitizer aborts at startup with
> `unexpected memory mapping` because the distribution sets more ASLR entropy
> than TSan's shadow layout tolerates. Fix: `sudo sysctl -w vm.mmap_rnd_bits=28`.

---

## Architecture

### The central idea

Three kinds of element flow through the same pipeline, through the same queues,
**in order**:

| Element | Meaning |
| --- | --- |
| **Record** | Actual data, carrying an event timestamp |
| **Watermark** | "I have seen everything up to time T" |
| **Barrier** | "Snapshot your state now" |

Only the first is data. The other two are *control signals travelling in the data
path*, and both of the system's non-obvious properties fall out of that:

- A watermark cannot overtake the records ahead of it, so an operator receiving
  watermark `T` knows every record older than `T` has already passed through **it
  specifically** — event-time progress with no global clock and no coordination.
- A barrier cannot overtake records either, so every operator's snapshot reflects
  the identical prefix of the input stream even though each was taken at a
  different wall-clock instant — a consistent distributed snapshot **with nothing
  ever pausing**.

### Runtime

```
              source (calling thread)
                 injects barriers (broadcast) and paces if asked
                 |
                 |  partition: key -> serialize -> key group -> subtask
                 v
      +----------+----------+----------+
      |          |          |          |
   [queue0]  [queue1]  [queue2]  [queue3]        bounded; blocking = backpressure
      |          |          |          |
   subtask0  subtask1   subtask2  subtask3       own operator + own state backend
      |          |          |          |          (partitioned => no locks)
      +----------+----+-----+----------+
                      |  fan-in
               [ sink queue ]                     the only multi-input operator,
                      |                           so the only place alignment happens
                 sink thread
                      |
              checkpoint coordinator  <-- the only out-of-band component
```

An operator is written once against `Collector<T>` and never learns which
runtime it is in. The same interface is:

- a direct call to the next operator (single-threaded pipeline),
- a push into a bounded queue (parallel runtime),
- a hand-off to a chained operator in the same subtask.

That seam is why making the engine parallel changed **no operator code at all**.

---

## Design decisions

All 74 are in [`docs/DESIGN.md`](docs/DESIGN.md) with their rejected
alternatives. The ones that shaped everything else:

### Type erasure: virtual base for ownership, static types on the edges

Storing a heterogeneous DAG of operators is really *two* problems with different
answers. **Ownership and traversal** get erased — `vector<unique_ptr<OperatorBase>>`,
so checkpointing can walk every operator. **The data path** stays statically
typed — `Operator<In, Out>::process(Record<In>&&, Collector<Out>&)`, so a
mis-wired pipeline is a compile error.

*Rejected: CRTP / compile-time chaining.* Fastest possible, and fatal here: a
chain welded together at compile time has no seam to insert a queue into and no
runtime collection to walk for snapshotting. That cost — one virtual call per
record per operator — bought the parallel runtime and checkpointing.

*This is the design you'd want in a latency-critical system with fixed topology.
Same technique, opposite verdict, because the constraints differ.*

### Records move; they are never shared

Value semantics with moves. *Rejected `shared_ptr`*: every hop is an atomic
refcount touch on a cache line bouncing between cores, and the contention gets
**worse** as you add threads — the inverse of the point. *Rejected pooling*:
lifetime complexity at thread boundaries, bought before any measurement asked for
it. Enforced by a test that fails if any payload is copied on the single-consumer
path.

### Partitioning removes locking rather than optimising it

Each subtask owns a key range and its own state backend. There is no mutex around
keyed state anywhere, because there is nothing shared to protect. A shared map
behind a lock would not merely run slower — it would fail to scale, since
contention grows with thread count.

### Bounded queues *are* the backpressure

A slow sink fills its queue; the subtask blocks in `collect`; it stops draining
its own input; that fills; the source blocks. No signalling, no rate limiter, no
measurement — an emergent property of the queues being bounded. An unbounded
queue doesn't degrade more gracefully, it OOM-kills the process under exactly the
load where you needed it to survive.

### Barriers travel with the data

*Rejected: stop-the-world snapshots.* Correct, simple, unusable — throughput
drops to zero and it gets **worse** with more tasks, because you wait for the
slowest every time.

### Serialization is designed by hand, and `deserialize` demands full consumption

C++ has no reflection, so every object↔bytes conversion is written by hand. The
defining bug of that approach is writing three fields and reading two, which
corrupts silently. `deserialize` therefore requires the reader to be **fully
consumed** and throws otherwise, converting the whole bug class into an exception
at the point of the mistake.

### Key groups, not `hash(key) % parallelism`

Keys map to one of 128 fixed buckets, independent of parallelism; subtasks own
ranges of buckets. A key never changes bucket, so a rescale redistributes buckets
instead of rehashing keys. The hash is hand-rolled FNV-1a, not `std::hash` —
`std::hash` has no stability guarantee across processes or implementations, and a
key group gets written into a checkpoint and read back by a different build.

---

## Correctness methodology

The most useful thing this project produced is not a feature. It is a habit:
**break the property deliberately, confirm the check fails, restore.** It was
applied five times and found something every time.

| What was broken | What the checks did |
| --- | --- |
| Removed the lock from `close()` | Every timing-based test **still passed** under TSan — the sleeps meant the accesses never overlapped, so TSan's shadow history had evicted them. A contention test written in response catches it instantly. |
| Disabled barrier alignment | Checkpoints recorded 32 records at a cut where the source had emitted 20 — twelve post-cut records baked in, which recovery would count twice. That *is* the aligned/unaligned distinction, as a number. |
| Disabled state restore, kept the rewind | Final totals came out **9 where the answer was 24** — exactly the pre-cut records dropped. |
| Off-by-one in the key-group range | Exhaustive property tests caught it immediately; the **end-to-end rescale tests passed anyway**, because five keys only sample five of 128 groups. |
| Ran the demo application | Found a genuine latent bug: `WindowOperator` inherited a no-op `snapshot_state`, so **windowed jobs checkpointed nothing**. Nothing crashed — recovery restarted every partially-filled window from empty and produced totals that were plausible and quietly short. |
| Made an operator throw mid-run | **Deadlocked.** End-of-channel was announced as the last statement of a function, so the exception path skipped it and the fan-in sink waited forever; behind that, the source blocked on a queue nobody would drain. Fixed with RAII — the same rule the project already applied to threads, applied to channels. |

Two conclusions worth stating plainly, because they generalise:

**A check that can pass while the property it protects is violated is worse than
no check.** It converts acknowledged ignorance into confident wrongness. This
applies to a UBSan configured to log and continue, a green TSan run over tests
that never contend, and an end-to-end test that samples the space thinly.

**A test suite that exercises one kind of state proves nothing about the other
kinds.** Every recovery test used a keyed aggregate, whose state lives in the
backend, so operator-held state was entirely untested while the suite looked
thorough.

### What is actually verified

- **155 tests**, green under `dev` (Clang), `dev-gcc`, `asan` (ASan+UBSan) and
  `tsan`. Sanitizer builds use `-O1` with assertions live and
  `-fno-sanitize-recover=all`, so UBSan aborts rather than logging.
- **Concurrency** — the bounded queue and shutdown path are tested with genuine
  contention, repeated under TSan, with a CTest timeout so a hang fails the build
  rather than hanging CI.
- **The differential oracle** — parallel output must equal single-threaded output
  per key. Valid because event-time semantics make output a pure function of
  input. It and TSan find **disjoint** bug classes: a race can produce identical
  output on ten thousand runs and still be undefined behaviour.
- **Fault injection** — the job is killed at seeded pseudo-random offsets,
  recovered from whatever checkpoint had completed, and the final state must match
  an uninterrupted run every time. Covers failure before any checkpoint completes,
  immediately after one, and repeated failures.
- **Property tests** — the key-group mapping is checked exhaustively over every
  (parallelism, subtask, group) triple. Windowing is checked over seeded random
  streams for conservation (every record is windowed exactly once or reported
  late), agreement with a direct grouping, invariance under shuffled arrival
  order, and full state release.
- **Failure paths** — an operator or sink that throws must shut the pipeline down
  rather than hang, and the failure must be *reported*, so an incomplete run
  cannot be mistaken for a clean one.

---

## Benchmarks

Measured on an **11th Gen Intel i5-1135G7** (4 cores / 8 threads, 2.4 GHz), 8 GB
RAM, WSL2 on Ubuntu 24.04, Clang 18, `release` preset (`-O3 -DNDEBUG`).

> **These are indicative, not authoritative.** Checkpoint cost measured 3.5% on
> one run and −0.1% on the next; throughput at parallelism 4 has ranged from 546k
> to 725k rec/s — same binary, same input. A laptop under WSL2 with no CPU pinning
> is not a benchmarking environment. Quoting a single run as a result is the same
> error as quoting a mean latency without its tail.

### Components

| | |
| --- | --- |
| Tumbling window assignment | ~1.1 ns/record |
| Sliding assignment, 2 / 10 / 60 windows | 5.7 / 16.1 / 104 ns — **linear in windows per record** |
| Key-group hash | ~12 ns |
| Serialize `int64` / 64-byte string | 31 / 56 ns |
| Keyed state read-modify-write, 1 / 100 / 10k keys | 80 / 135 / 257 ns |
| Bounded queue push+pop, uncontended | 22 ns |
| Bounded queue 1P1C, **capacity 8** | **173k items/s** |
| Bounded queue 1P1C, **capacity 1024** | **4.35M items/s** |

**A 25× throughput difference from queue capacity alone.** At capacity 8 the
threads ping-pong on the condition variable and live in the scheduler instead of
moving data. That is the argument for chaining adjacent operators, and it is
invisible without measuring.

### End to end

| | |
| --- | --- |
| Hand-written loop, no engine | ~34,000,000 rec/s |
| Ripple, parallelism 4 | ~550,000–725,000 rec/s |
| **Cost of the abstractions** | **~47×** |
| Latency at a paced 200k rec/s | p50 **346 µs** · p90 584 µs · p99 **1.16 ms** · p99.9 **1.37 ms** |
| Checkpoint every 50k records | ~0–4% throughput cost, ~2 ms duration |
| Recovery after a kill at 50% | 0.17–0.24 s, dominated by **replay**, not by loading state |

The component benchmarks account for only ~250 ns of real work per record, so the
overwhelming majority of that 47× is coordination: per-record queue handoffs,
allocation, and a serialize/deserialize round trip on every state access. Batching
across queues is the obvious first optimisation.

Throughput *falls* from parallelism 4 to 8 — at 8 the job wants ten threads on an
eight-thread machine.

Percentiles are always reported together, never a mean alone: a pipeline that
answers in 1 ms 99% of the time and 900 ms the rest has a mean near 10 ms, which
describes no request that ever happened.

---

## Known limitations

- **Single process.** Parallelism is threads, not machines. Barriers and
  watermarks would work identically over a network; the transport is the missing
  piece, not the algorithm.
- **State must fit in memory.** A checkpoint is O(all state); a production engine
  uses an LSM tree so it is O(changed state).
- **Sinks must be idempotent.** No two-phase commit, so end-to-end exactly-once
  requires the sink to support upserts.
- **Unkeyed operator state does not survive a rescale.** Keyed state and window
  state redistribute by key group; genuinely unkeyed state (a source offset) would
  need union-list/split-list redistribution.
- **Aligned checkpoints only.** No unaligned mode, so a slow subtask stalls the
  checkpoint.
- **No watermark idleness detection.** A silent input channel freezes the minimum
  and stops every downstream window firing — documented, tested as expected
  behaviour, not yet fixed.
- **Alignment buffers rather than backpressures.** The fan-in is one shared queue,
  so a barriered channel's records are set aside instead of the channel being
  stopped. Identical cut and identical semantics, trading backpressure for memory.
- **The benchmark harness reports single runs**, which the variance above shows is
  not enough.

---

## What I'd do differently

**Design the transport element type in Stage 1, not Stage 2.** Operators taking
`Record<T>` was the honest Stage 1 design, and the refactor to add watermarks was
instructive — but knowing now that the queue must carry one closed variant, I'd
shape that boundary earlier.

**Test each *kind* of state, not each feature.** The window checkpointing bug
existed because every recovery test happened to use backend-resident state. A
matrix of (state location × failure mode) would have caught it on day one.

**Make the state backend model `(key, subkey)`.** Window state lives in the
operator's own map purely because the backend's flat key-value interface can't
express `(key, window)`. That one gap is why window state needed a parallel
snapshot path — and why it was missed.

**Batch across queues from the start.** The 25× capacity finding and the 47×
abstraction cost both point at the same thing: one record per handoff is the wrong
granularity. Records should move in batches with a single lock acquisition.

**Test the failure paths from the start.** No test ever made an operator throw,
so a deadlock sat in the engine through four stages of "TSan-clean" claims. TSan
finds races; it does not find missing cleanup on an exception path. That needed a
test that deliberately fails.

**Build the benchmark harness with the same scepticism as the engine.** Three
separate measurement bugs produced confident, wrong numbers — pacing finer than
the scheduler can honour, timing from the wrong clock origin, and mismatching the
source's batching. The harness had no tests. It should have.

---

## Repository layout

```
include/ripple/        the engine, mostly headers
  concurrent/          bounded queue, worker group
  operators/           map, filter, window, keyed aggregate, chain, watermarks
  parallel/            parallel runtime, stream element
  state/               backends, state handles, key groups
  checkpoint/          coordinator
  sinks/               idempotent sink
src/                   non-template definitions
tests/                 155 tests across 16 targets
benchmarks/            Google Benchmark components + end-to-end harness
apps/                  the taxi demo
docs/DESIGN.md         72 decisions, each with its rejected alternatives
docs/INTERVIEW.md      concepts and question lists per stage
```

Built in ten stages, one commit each, each self-contained and green under every
sanitizer configuration. The commit log is the build order.
