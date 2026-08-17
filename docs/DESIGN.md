# Ripple — Design Log

A mini stream-processing engine (a simplified Apache Flink) in C++20, built from
scratch for the purpose of understanding what a stream engine actually does.

This file is a running decision log. Every entry records what was chosen, what
was rejected, and why — so that a decision can be revisited on its merits later
rather than re-argued from scratch.

---

## 1. Goals

- **Event-time correctness.** Same input produces the same output, regardless of
  when records arrive or how the pipeline is scheduled.
- **Throughput, not latency.** This is explicitly a throughput system. Virtual
  dispatch per operator is fine. Clarity and correct concurrency beat
  micro-optimization. We optimize in Stage 9, where a benchmark points.
- **Provable concurrency.** Every test target runs clean under ThreadSanitizer.
  A "probably fine" concurrent system is a broken one that has not been observed
  yet.
- **Fault tolerance that means something.** Asynchronous barrier snapshotting,
  recovery, and exactly-once *state* semantics, verified by fault injection.

### Non-goals

- Distribution across machines. Parallelism is threads within one process.
  Barriers and watermarks would work the same way over a network; the algorithm
  is the interesting part, not the transport.
- SQL, a planner, or a query optimizer.
- Beating Flink on anything.

---

## 2. Architecture sketch

### The central idea

Three kinds of element flow through the same pipeline, through the same queues,
in order:

| Element       | Meaning                                    | Introduced |
| ------------- | ------------------------------------------ | ---------- |
| **Record**    | Actual data, carrying an event timestamp   | Stage 1    |
| **Watermark** | "I have seen everything up to time T"      | Stage 2    |
| **Barrier**   | "Snapshot your state now"                  | Stage 7    |

Only the first is data. The other two are **control signals travelling in the
data path**, and both of this system's clever properties follow from that:

- Because a watermark cannot overtake the records ahead of it, an operator that
  receives watermark `T` knows every record older than `T` has already passed
  through *it specifically* — event-time progress with no global clock.
- Because a barrier cannot overtake records either, every operator's snapshot
  reflects the identical prefix of the input stream, even though each was taken
  at a different wall-clock instant — a consistent distributed snapshot with no
  stop-the-world pause.

### Planes

- **Data plane** — sources → operators → sinks. Push-based. Single-threaded
  function calls in Stages 1–4; the same logical graph becomes threads connected
  by bounded queues in Stage 6, with no change to operator code. That invariance
  is the test of whether the Stage 1 interface was right.
- **Control plane** — one component, the checkpoint coordinator (Stage 7). The
  only thing that communicates out-of-band. Deliberately minimal: the default
  answer to "should the coordinator do this?" is no, put it in the stream.

### Layers

```
 S9    Benchmarks · metrics · demo application
 S7-8  Checkpoint coordinator · barriers · alignment · recovery
 S5-6  Runtime: bounded queues, thread lifecycle, shuffle, backpressure
 S4    Keyed state · state backend · serialization
 S3    Windowing: assigners, triggers, window state, lateness
 S2    Event time: watermark generation and propagation
 S1    Core: Record, Operator, Sink, Pipeline, DAG
 S0    Build, presets, sanitizers, CI
```

### Build order rationale

Stages 1–4 define **what the right answer is**, single-threaded and
deterministic. Stages 5–6 make it **parallel without changing the answer**.

This ordering is deliberate and the alternative is tempting. Building the
threading first means every wrong output has two candidate explanations —
broken window semantics or a race — and no way to tell them apart. Building
semantics first yields a **deterministic oracle**: in Stage 6, parallel output
must equal single-threaded output for the same input. Any divergence is the
runtime, known immediately and for free.

### Known seams

Places where an earlier stage will need rework for a later one. Recorded here so
they are planned rather than discovered.

| Seam | Stages | Decision |
| ---- | ------ | -------- |
| `Record` vs. a record-or-watermark-or-barrier element type | 1 → 2, 7 | **Accept the refactor.** Stage 1 has no watermarks, so a Record-only interface is the honest Stage 1 design. Pre-building the variant would mean carrying an abstraction whose justification has not been met. |
| Serialization interface | 4 → 7 | **Anticipate.** C++ has no reflection, so state serialization must be hand-designed, and retrofitting it across every state type later is expensive. Stage 4 designs and tests the interface; Stage 7 is its first consumer. Interface only — no checkpoint machinery in Stage 4. |
| Replayable source offsets | 1 → 8 | **Accept the refactor.** Exactly-once needs sources to rewind. Small change, taken in Stage 8. |

---

## 3. Decision log

### D-001 — CMake with CMakePresets, not a hand-rolled build

**Chosen:** CMake ≥ 3.20 (have 3.28.3), `CMakePresets.json` v6, Ninja generator.
Five configurations: `dev`, `dev-gcc`, `asan`, `tsan`, `release`.

**Rejected:** Bazel (heavier than a single-binary project justifies, and worse
IDE integration); Meson (fine, but CMake is what C++ shops actually use, and
fluency in it is transferable); raw Makefiles (no dependency management, and
`FetchContent` alone is worth the whole thing).

**Consequence:** `cmake --preset tsan` instead of remembering a twelve-flag
command line. Configurations are committed and identical locally and in CI, so
"works on my machine" has one fewer place to hide.

---

### D-002 — Dependencies via FetchContent

**Chosen:** GoogleTest fetched and built from source, pinned to `v1.15.2`.

**Rejected:** `find_package` against a system-installed GoogleTest; git
submodules.

**Why this is a correctness requirement, not a convenience:** ThreadSanitizer
needs **every translation unit in the process** instrumented. A system
GoogleTest is compiled without `-fsanitize=thread`; linking it produces false
positives inside gtest's own internals and can hide real races that cross the
boundary. FetchContent builds dependencies with *our* flags, so each sanitizer
configuration is internally consistent.

Tag pinned rather than tracking a branch: a moving dependency makes a CI failure
unreproducible, which is the specific failure mode that wastes the most time.

---

### D-003 — ASan+UBSan and TSan as separate build configurations

**Chosen:** `asan` = `-fsanitize=address,undefined`; `tsan` =
`-fsanitize=thread`. Two configurations, two CI jobs.

**Rejected:** One combined sanitizer build. It does not exist.

**Why they do not compose:** ASan and TSan are both *shadow memory* sanitizers.
Each maintains bookkeeping bytes for every byte of application memory, and each
must reserve a large region of the address space at a fixed offset. Their
required layouts collide directly — no assignment satisfies both. They also each
interpose on the allocator and the same libc entry points. Clang rejects the
combination in the driver:

```
$ clang++ -fsanitize=address,thread x.cpp
clang++: error: invalid argument '-fsanitize=address' not allowed with '-fsanitize=thread'
```

**Why UBSan composes with either:** UBSan is predominantly *inline*
instrumentation — checks emitted at the use site, calling a small runtime. It
claims no shadow region, so it has nothing to conflict over.

---

### D-004 — `-fno-sanitize-recover=all` in the ASan configuration

**Chosen:** UBSan aborts on the first finding.

**Rejected:** The default, where UBSan prints a diagnostic and continues.

**Rationale:** With the default, a test exercising undefined behaviour still
exits zero. CI reports green while UB happens. A sanitizer that cannot fail a
build is a logging framework.

Paired with `-fno-omit-frame-pointer`, without which the stack traces in
sanitizer reports are unusable — precisely when they are most needed.

---

### D-005 — Sanitizer builds use `Debug` with `-O1`, not `RelWithDebInfo`

**Chosen:** `CMAKE_BUILD_TYPE=Debug` with `CMAKE_CXX_FLAGS_DEBUG` overridden to
`-g -O1`.

**Rejected:** `RelWithDebInfo` (which is `-O2 -g -DNDEBUG`), and plain `Debug`
at `-O0`.

**Rationale:** two independent reasons.

- `RelWithDebInfo` defines `NDEBUG`, which compiles out every `assert`. Assertions
  are precisely the internal invariant checks we want *enabled* in a sanitizer
  run.
- `-O0` makes TSan brutally slow and perturbs thread timing enough to hide races
  that `-O1` exposes. `-O1` is the documented sweet spot: fast enough to run,
  still readable in a backtrace.

---

### D-006 — Clang as primary compiler, GCC in CI

**Chosen:** Clang 18 for all local work and all sanitizer configurations. GCC 13
builds and tests the non-sanitizer configuration in CI (`dev-gcc`).

**Rejected:** GCC-only (weaker TSan, and its diagnostics disagree with
clang-tidy's parser); Clang-only (a single frontend accepts sloppiness that is
not actually valid C++).

---

### D-007 — Curated clang-tidy check set

**Chosen:** `bugprone-*`, `concurrency-*`, `performance-*`, `modernize-*`,
`cppcoreguidelines-*`, `readability-*`, `misc-*`, minus a documented exclusion
list. `HeaderFilterRegex` restricts findings to our own headers.

**Rejected:** Enabling everything. A linter that fires on every function trains
you to ignore the linter, after which it protects nothing. Each exclusion in
`.clang-tidy` carries a written reason so the list stays honest.

`concurrency-mt-unsafe` in particular earns its place in a project like this.

---

### D-008 — Warning flags carried on an INTERFACE target

**Chosen:** An INTERFACE library `ripple::warnings` that targets link.

**Rejected:** Setting global `CMAKE_CXX_FLAGS`. Global flags also apply to
FetchContent dependencies, burying our warnings under thousands from other
people's code. Combined with `SYSTEM` on the dependency declaration, warnings
now come only from code we wrote.

Notable inclusions: `-Wnon-virtual-dtor` and `-Woverloaded-virtual` (Stage 1
introduces polymorphic operator base classes, where both mistakes are easy and
silent), and `-Wold-style-cast` (forces `static_cast`/`reinterpret_cast`, which
are greppable when auditing).

---

### D-009 — Google Benchmark declared but not fetched until Stage 9

**Chosen:** `RIPPLE_BUILD_BENCHMARKS`, default `OFF`, with the
`FetchContent_Declare` inside the guard.

**Rejected:** Fetching it now. Nothing uses it for nine stages; it would be
download and build time on every clean configure, in service of nothing.

---

### D-010 — No C++20 modules

**Chosen:** Conventional headers and translation units.

**Rejected:** C++20 modules, despite the project being C++20 throughout.

**Rationale:** module support across CMake 3.28 + Clang 18 + FetchContent-built
dependencies is not yet smooth, and clang-tidy's module support is worse. The
cost would be paid in build-system debugging, and the benefit is compile time on
a project too small to need it. Revisit if the toolchain matures.

---

### D-011 — Prove the TSan configuration before trusting it

**Chosen:** Before writing any engine code, build a program with a deliberate
unsynchronized data race under the `tsan` preset and confirm ThreadSanitizer
reports it. Delete the program once it has done its job.

**Result:** confirmed. TSan reported `data race`, naming both conflicting writes
and the global, and exited 66.

**Rationale:** a sanitizer configuration that has never been observed catching a
bug is decoration. Every later claim in this project that something is
"TSan-clean" depends on this one check having been done.

This also flushed out the environment issue documented below.

---

### D-012 — Environment: ASLR entropy and ThreadSanitizer

Ubuntu 24.04 ships `vm.mmap_rnd_bits = 32`. TSan's fixed shadow layout assumes
less ASLR entropy and aborts at startup with
`FATAL: ThreadSanitizer: unexpected memory mapping`, which reads as a broken
toolchain rather than a host setting.

Fix: `sudo sysctl -w vm.mmap_rnd_bits=28`. Applied in the CI `tsan` job. Not
required on the current development machine, but recorded because the symptom is
badly misleading.

---

### D-013 — Demo dataset: NYC TLC for the demo, synthetic for tests

**Chosen:** NYC TLC taxi trip data as the Stage 9 demo dataset; a seeded
synthetic generator as a test and benchmark fixture.

**Rejected:** A synthetic generator as the demo dataset.

**Rationale:** you cannot be surprised by data you generated. If out-of-orderness
comes from a distribution we chose, then tuning bounded-out-of-orderness against
it validates the generator, not the engine. TLC supplies disorder, key skew
(Manhattan zones dominate, so Stage 6's hot-partition problem appears without
being simulated), and genuinely dirty records — zero fares, negative durations,
timestamps in 2001 and 2098 — which makes Stage 3's allowed-lateness and
side-output paths load-bearing rather than decorative.

The synthetic generator is still built, as a fixture: deterministic, seeded,
arbitrarily large, and requiring no network in CI.

**Known cost:** TLC moved to Parquet in 2022 and a Parquet reader is a dependency
we do not want. Mitigation: use a pre-2022 CSV month, or convert once offline and
commit a sampled slice. Raw downloads are gitignored either way.

---

### D-014 — Type erasure: virtual base for ownership, static types on the edges

**The problem.** `Map<Trip, Fare>` and `Filter<Fare, Fare>` are unrelated types.
A pipeline must own both in one container, iterate them (Stage 7 snapshots every
operator), and connect them without losing type safety.

**Chosen.** Split the problem in two, because it is two problems:

- *Ownership and traversal* → erased. `std::vector<std::unique_ptr<OperatorBase>>`,
  where `OperatorBase` is a non-template class whose only real job is to give
  `unique_ptr` a virtual destructor to call.
- *The data path* → static. `Operator<In, Out>::process(Record<In>&&,
  Collector<Out>&)`. Types are known at wiring time, so a mismatched connection
  is a compile error.

One virtual call per record per operator. Accepted deliberately: this is a
throughput system, and against per-record parsing, hashing, and state lookup a
predicted indirect call is noise. Revisit only if Stage 9 says otherwise.

**Rejected — A: fully static templates / CRTP.** Fastest possible; the compiler
can inline an entire chain into one loop. Rejected on four counts, the last two
fatal: topology must be known at compile time, so no runtime-built DAG; template
error messages become genuinely unusable; **Stage 6 has no seam to insert a
queue into** a chain welded together at compile time; and **Stage 7 cannot walk
a compile-time chain** to snapshot it.

*This is exactly the right design for a latency-critical system with fixed
topology — it is what the author's limit-order-book engine uses. Same technique,
opposite verdict, because the constraints differ. Worth stating in an interview.*

**Rejected — B: `std::function` composition.** No performance advantage:
`std::function` *is* type erasure with an indirect call, plus possible heap
allocation. The real objection is that operators must be **objects with
identity and state** — Stage 4 gives them keyed state, Stage 7 gives them
`snapshot()`/`restore()`, Stage 6 gives them a name and a parallelism — and a
callable can be asked none of that. Composition-by-capture also makes the graph
implicit and untraversable.

**Rejected — C: `std::variant` payload.** A closed set: every payload type in
the system must be listed in one central header, so adding an operator means
recompiling the world. `sizeof(variant)` is its largest alternative, so one 4 KB
payload type taxes every record in a system built to move millions per second.
And it does not even address operator storage.

*Variants are right for genuinely closed sets. Stage 2's stream element — record,
watermark, or barrier — is exactly that, and is expected to use one.*

---

### D-015 — Record ownership: value semantics with moves

**Chosen.** `Record<T>` is an aggregate passed as `Record<T>&&`. Each operator
takes ownership, transforms, and moves downstream. Exactly one owner at a time.

**Rejected — `shared_ptr<Record>`.** Every hop is an atomic refcount increment
and a matching decrement. Once Stage 6 puts operators on different threads,
those atomics hit a cache line bouncing between cores, millions of times a
second — and the contention *worsens* as threads are added, which is the exact
inverse of the point of parallelism. It also implies shared mutable state.

**Rejected — pool / arena allocation.** Avoids malloc entirely, but records
crossing thread boundaries need either a thread-safe pool (locks or atomics,
back to the previous problem) or per-thread pools with a migration story. Real
complexity purchased before any measurement demands it. *This is the
latency-project instinct and it is the wrong one here.* Revisit in Stage 9 if
profiling says allocation is hot.

**Known limitation.** Moves work for exactly one consumer. Branching DAGs and
sliding windows (Stage 3, one record in five windows) require genuine copies.
The interface therefore takes `Record<T>&&` unconditionally, and the *pipeline*
owns the decision to copy on fan-out — keeping the common path free and
confining copies to the code that knows a fan-out is happening.

Enforced by test: `PipelineOwnershipTest.DoesNotCopyPayloadsOnTheSingleConsumerPath`
fails if any payload is copied. A copy has no visible symptom other than lost
throughput, so it is asserted rather than trusted.

---

### D-016 — `Timestamp` is a chrono type, not `std::int64_t`

**Chosen.** `using Timestamp = std::chrono::sys_time<std::chrono::milliseconds>`.

**Rejected.** Raw `std::int64_t` milliseconds, which is what Flink uses.

**Rationale.** The type system then enforces the arithmetic windowing depends on:
`Timestamp - Timestamp` yields a `Duration`, `Timestamp + Duration` yields a
`Timestamp`, and **`Timestamp + Timestamp` does not compile**. That last one is
the purchase: adding two timestamps is meaningless and is precisely the slip
that window-boundary arithmetic invites. With `int64` it compiles silently and
produces a window in the year 55000.

Serialization is unaffected — `.time_since_epoch().count()` gives the `int64`
back at the system boundary, in one place.

---

### D-017 — Push execution, not pull

**Chosen.** Sources push records forward; operators are plain function calls.

**Rejected.** Pull (iterator/`next()`) execution.

**Rationale.** Data arrival is not under the consumer's control, so a pull model
misrepresents the problem. Concretely: a pull operator must be **resumable**,
which makes every operator a coroutine or a hand-written state machine. Fan-out
is trivial when pushing (call two collectors) and requires buffering when
pulling. Watermarks and barriers both originate at the source and travel
forward, which is push's direction.

**Accepted cost.** Pull gives backpressure free — a consumer only asks when
ready. Push must be *given* backpressure via bounded queues. That is precisely
why Stages 5 and 6 exist.

---

### D-018 — Linear chain now; general DAG deferred

**Chosen.** `from(source).via(op).via(op).to(sink)`.

**Rejected.** A branching DAG in Stage 1. Nothing in Stage 1 needs it, and
branching is what forces the copy-on-fan-out logic. A chain is a special case of
a DAG, so generalizing later is additive rather than a rewrite.

---

### D-019 — The builder deduces concrete node types, not base types

**Problem found while building.** `from(std::unique_ptr<VectorSource<int>>)` did
not compile against a parameter of `std::unique_ptr<Source<Out>>`. Template
argument deduction requires an exact match and will not look through
`unique_ptr<Derived>` to find `unique_ptr<Base>` — the derived-to-base
conversion is only available *after* deduction succeeds.

**Chosen.** Deduce the concrete type and recover the payload types from member
aliases (`Source::OutputType`, `Sink::InputType`, `Operator::InputType/OutputType`),
with a `static_assert` verifying the connection.

**Consequence.** A mis-wired pipeline reports
`operator input type does not match the previous stage's output type` instead of
a page of overload-resolution failures.

---

### D-020 — Collectors are heap-allocated and owned by `unique_ptr`, not stored by value

The builder wires stages by holding `Collector<Out>**` — the address of a
not-yet-filled slot inside the previous stage's collector. This is what allows
the pipeline to be *described* front-to-back while the connections are
inherently back-to-front.

Those slot pointers stay valid across `push_back` only because the vector holds
`unique_ptr`s rather than collector objects: reallocation moves pointers and
leaves the pointed-to objects at fixed addresses. Storing collectors by value
would dangle every outstanding slot pointer on the next append. Recorded because
it is a lifetime invariant that is invisible at the call site.

---

### D-021 — Watermarks as a separate virtual method, not a variant element

**The Stage 1→2 seam, resolved.** Operators now need to see two kinds of thing.
Two ways to express that:

**Chosen.** A second virtual, `on_watermark(Watermark, Collector<Out>&)`, with a
default body that forwards the watermark downstream unchanged.

**Rejected — `process(std::variant<Record<T>, Watermark>&&)`.** It would force
*every* operator to dispatch on the alternative, including map and filter, which
have no notion of time and should not have to mention watermarks at all. The
default-forwarding virtual gives them the correct behaviour for free: neither
`MapOperator` nor `FilterOperator` changed by a single line in this stage.

**Note for Stage 6.** The variant is still coming, at a different layer. A
bounded queue between threads has to *transport* one element type, and that
element is `Record | Watermark | Barrier` — a genuinely closed set defined by the
engine, which is exactly what D-014 said variants are right for. Interface shape
and transport shape are separate decisions.

**Consequence for Stage 3.** A window operator overriding `on_watermark` **must
still forward the watermark** after firing its windows. Forgetting to is a
plausible bug with a nasty signature: event time stops advancing for everything
downstream, so the pipeline holds state forever and produces no output while
looking entirely healthy.

---

### D-022 — Watermark generation is an operator, not logic inside `Source`

**Chosen.** `BoundedOutOfOrdernessWatermarks<T>`, an `Operator<T, T>` placed
immediately after the source.

**Rejected.** Building generation into the `Source` base class.

**Rationale.** The strategy becomes pluggable and independently unit-testable,
and sources stay concerned only with producing records. This is also what Flink
does in practice, despite the concept being described as source-level.

---

### D-023 — The record is emitted before the watermark derived from it

The single most consequential line-ordering decision in the stage.

`process()` forwards the record, *then* computes and emits any watermark.
Reversed, a record's own arrival would produce a watermark that renders that
same record late: downstream, a window could close and free its state one
instant before receiving a record that belonged in it.

The failure mode is why this is asserted rather than trusted. It is invisible in
aggregate — record counts stay correct, no error is raised, and only the
boundary records of each window quietly land in the wrong place or vanish.

Enforced by `BoundedOutOfOrdernessTest.EmitsTheRecordBeforeTheWatermarkItProduces`,
which uses a zero lag so the generated watermark exactly equals the record's own
event time, making any ordering slip observable.

---

### D-024 — Watermarks are emitted only when they advance

The generator tracks the **maximum** event time seen, not the most recent, and
emits only when `max - bound` exceeds the last emitted watermark.

Tracking the most recent event time instead would make the watermark oscillate
with every straggler. A regressing watermark re-opens a window that has already
fired, producing a second, contradictory result for a period that was supposed
to be complete. `WatermarkTracker` enforces the same invariant per channel
rather than trusting its inputs.

---

### D-025 — End of input emits a maximal watermark

When `Source::run` returns, the runner emits `Watermark{kMaxTimestamp}`.

Without it, a finite job silently drops whatever windows were still open when
the input ran out — the last few minutes of every run simply missing, with no
error and plausible-looking totals. Also emitted for an empty input, so a source
that produces nothing still terminates event time downstream rather than leaving
operators waiting forever.

---

### D-026 — `WatermarkTracker` built now, consumed in Stage 6

The minimum-across-input-channels rule is the conceptual core of watermark
propagation and belongs to this stage, but Stage 2's topology is a linear chain,
so no operator yet has more than one input.

**Decision:** build and fully test it now as a self-contained unit; wire it in
when Stage 6 introduces fan-in. Recorded as a deliberate exception to the
otherwise strict no-building-ahead rule, on the grounds that the alternative is
a stage that omits its own central concept.

**Why minimum and not maximum.** Taking the maximum is the natural-looking
mistake and is catastrophic rather than merely wrong: the operator would claim
to have seen everything up to the *fastest* channel's time, fire windows on that
basis, then receive perfectly on-time records from the slower channel that now
look late — silent data loss proportional to how far the channels have diverged.

**The pathology this rule implies, asserted deliberately in
`IdleChannelStallsProgress`:** a channel that goes silent stops advancing its
watermark, so the minimum stops advancing, so every downstream window stops
firing. Records keep flowing, nothing errors, and output simply stops. Knowing
this is the *expected* consequence of the minimum rule rather than a bug is what
makes it diagnosable in production.

---

### D-027 — Windows are half-open intervals `[start, end)`

**Rejected:** inclusive ends. A record landing exactly on a boundary would
belong to two adjacent tumbling windows and be counted twice. The error appears
only at boundaries, so it survives any test that does not deliberately probe
them, and it inflates totals by a small, plausible-looking amount.

Related: `TimeWindow::overlaps` tests **strict** overlap, not adjacency.
`[1000,2000)` meeting `[2000,3000)` is precisely the case where a session's
inactivity gap was met exactly and the session must end. Treating adjacency as
overlap would silently glue every session in the stream into one.

---

### D-028 — Assigners are template parameters, not a virtual interface

**Chosen:** `TumblingWindows`, `SlidingWindows`, `SessionWindows` as small value
types passed as template arguments to `WindowOperator`.

**Rejected:** a `WindowAssigner` abstract base with virtual `assign`.

**Rationale.** Assignment is pure arithmetic, called once per record, and --
unlike the operator graph -- the set of assigners does not need to be extensible
at runtime from configuration. There is no indirection to buy anything here.

Each assigner exposes `static constexpr bool is_merging`, which lets the window
operator `if constexpr` the entire merge machinery out of existence for the
non-merging cases. A virtual interface could not do that.

*Note the contrast with D-014: virtual dispatch was accepted for operators
because the topology must be runtime-constructible and traversable. Neither
applies to assigners. The rule is not "virtual is fine" or "templates are fast",
it is "erase where you need runtime variation, and only there".*

---

### D-029 — Windows hold one accumulator, not their records

**Chosen:** an aggregator interface (`create` / `add` / `result` / `merge`) that
folds each record into an accumulator as it arrives.

**Rejected:** buffering every record in a window and aggregating at fire time.

**Rationale.** A summing window holds 8 bytes instead of the whole window's
worth of data. That is the difference between a windowed job that runs
indefinitely and one that grows until it is killed. `CollectAggregator` exists
for cases genuinely needing raw contents, and its docstring says plainly that it
forfeits this property.

Duck-typed template parameter rather than a virtual interface, for the same
reason as D-028: `add` is the hottest call in a windowed pipeline and there is
no runtime-pluggability requirement.

---

### D-030 — Only session windows merge, and merging constrains the aggregator

Tumbling and sliding boundaries are fixed by arithmetic on the timestamp alone;
no record can move them. A session's boundaries are determined by the data, so a
record arriving between two existing sessions **proves there was never a gap
between them** -- and the two sessions were never really two.

**Consequence:** merging assigners require the aggregator to supply `merge`,
which forces the aggregation to be associative. That is a real constraint and it
is the reason merging is exposed as a property of the assigner rather than
assumed everywhere.

**Algorithm.** Windows live in a `std::map` ordered by start, and merging scans
only *adjacent* entries. That is sufficient: if window A overlapped a
non-adjacent window C, it would necessarily also overlap everything between
them. After a merge the scan resumes from the merged window rather than moving
on, because the merged window may now reach far enough to overlap the next one.

---

### D-031 — Allowed lateness re-fires windows; downstream must treat results as upserts

A window fires when the watermark passes its end, but its state is retained
until `end + allowed_lateness`. A record arriving in that interval is admitted,
marks the window dirty, and the window emits **again** with a corrected result.

**The consequence downstream, which is easy to miss:** with allowed lateness the
same window is emitted more than once. A sink must treat window results as
upserts keyed by the window, not as appends. This is the same idempotence
requirement Stage 8 needs for end-to-end exactly-once, arriving three stages
early.

Records that missed every window go to a **side output** rather than being
dropped, and are counted even when no handler is attached. Late data is a
symptom -- of a watermark bound set too tight, or a misbehaving upstream -- and
data that vanishes without a trace is indistinguishable from data that was never
sent.

---

### D-032 — Window assignment uses floor division

C++ integer division truncates toward zero: `-1 / 5000` is `0`, not `-1`. A
truncating implementation therefore assigns every pre-epoch timestamp to the
window starting at zero.

Not hypothetical. The demo dataset contains meter timestamps stamped in 1900 and
2098, which is one of the reasons it was chosen (D-013) -- a synthetic generator
would never have produced this bug.

---

### D-033 — Window results are stamped at `end - 1ms`, and emitted before the watermark

A window's output record carries the last instant the window covers, not its
exclusive end. Stamping at `end` would place the result in the *next* window if
it flowed into a second windowing operator -- an off-by-one shifting every
downstream aggregate by exactly one window.

`on_watermark` emits all results **before** forwarding the watermark, for the
same reason as D-023: results carry timestamps inside the window they came from
and must reach downstream before the watermark that would render them late.

**And it must forward the watermark at all.** An override that fires windows and
swallows the watermark freezes event time for the entire rest of the pipeline:
downstream windows never fire, never free state, and the job produces no output
while looking perfectly healthy. `WindowOperatorTest.ForwardsWatermarksDownstream`
exists solely to catch that.

---

## 4. Stage status

| Stage | Description | Status |
| ----- | ----------- | ------ |
| 0 | Scaffolding, sanitizers, CI | **Complete** |
| 1 | Core dataflow, type erasure | **Complete** — 21 tests, clean under dev/gcc/asan/tsan |
| 2 | Event time and watermarks | **Complete** — 32 tests, clean under dev/gcc/asan/tsan |
| 3 | Windowing | **Complete** — 53 tests, clean under dev/gcc/asan/tsan |
| 4 | Keyed state, backend, serialization | Not started |
| 5 | Concurrency primitives | Not started |
| 6 | Parallelism, partitioning, backpressure | Not started |
| 7 | Checkpointing (Chandy-Lamport ABS) | Not started |
| 8 | Recovery and exactly-once | Not started |
| 9 | Benchmarks and demo application | Not started |
| 10 | README | Not started |
