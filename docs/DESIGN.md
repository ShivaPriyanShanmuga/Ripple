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

### D-034 — Serialization by trait specialisation, with a full-consumption check

C++ has no reflection. Nothing can generate this for us, so every
object-to-bytes conversion is written by hand. That is why it is a design task
here and a library call in Flink.

**Chosen:** `Serializer<T>` specialised per type with a `write` and a `read`,
constrained by a `Serializable` concept. Built-ins for arithmetic types,
`bool`, `std::string`, `std::vector`, `std::optional`, `std::pair`, raw byte
blobs, `Timestamp` and `TimeWindow`.

**Rejected — a self-describing tagged format** (protobuf/msgpack style). It buys
schema evolution across versions, which we do not need since both ends of every
conversion are compiled together, and charges for it on every field of every
record.

**Rejected — macro-generated field visitors** (`RIPPLE_FIELDS(a, b, c)`). It
would remove the duplication between `write` and `read`, which is a real
benefit. Rejected because macro-generated code is invisible to a debugger and
produces errors naming the macro rather than the mistake.

**The risk that rejection leaves, and how it is closed.** Writing three fields
while reading two is *the* hand-rolled-serialization bug: it corrupts everything
after it in the stream and is noticed, if at all, as inexplicably wrong data far
from the cause. `deserialize` therefore requires the reader to be **fully
consumed** and throws otherwise, converting that entire bug class into an
exception at the point of the mistake. A truncated stream and an implausible
length prefix throw for the same reason -- corrupt state must fail loudly,
because restore happens exactly when you are already recovering from something
else.

---

### D-035 — Fixed-width little-endian encoding

Integers are written little-endian regardless of host byte order, and `bool` as
exactly one byte rather than `sizeof(bool)`.

Native order is marginally faster and works perfectly until a state file written
on one machine is restored on another with the opposite endianness, at which
point every integer is silently wrong -- not corrupt-looking, just wrong. The
cost is a branch that compiles away on every platform anyone uses.

**Rejected — varint lengths.** Smaller for the common short case, at the cost of
a branchy decode; four bytes per string is not what limits this system.

Asserted directly on the bytes in `EncodesIntegersLittleEndian`, because on a
little-endian development machine the correct and buggy versions are
indistinguishable by round trip.

---

### D-036 — Keys are stored as bytes, which is why serialization comes first

`StateBackend` is a non-template class and its keys are `std::vector<std::byte>`.
That is what lets one backend implementation serve operators keyed on any type --
the same erasure argument as D-014, applied to state instead of operators.

It is also the concrete reason serialization had to be designed before keyed
state rather than alongside checkpointing: without it there is no key-type-
agnostic backend at all.

---

### D-037 — Both levels of the backend are ordered maps, not hash maps

A deliberate trade of some lookup speed for a property checkpointing needs:
**iteration order is deterministic**, so snapshotting identical state twice
produces identical bytes. An `unordered_map`'s iteration order depends on
insertion history and bucket count, and without byte-stability checkpoints can
never be compared, deduplicated, or diffed when something goes wrong.

The inner map uses `std::less<>` so a `string_view` state name is a
heterogeneous lookup rather than materialising a temporary `std::string` on
every state read.

Separately: `remove` drops a key entirely once its last state is gone. Without
that, clearing state frees the values but leaks one empty map node per key
forever -- a leak proportional to key cardinality, which for something like a
user id is unbounded, and the classic way a keyed streaming job dies slowly.

---

### D-038 — State handles are views; `KeyedOperator::process` is `final`

A handle owns nothing: it holds a backend reference and a state name, and every
operation applies to whatever key is currently set. A handle has **no way to name
a key**, so touching another key's state is not a mistake one can make.

`KeyedOperator::process` is `final` and subclasses override `process_keyed`. The
base sets the current key first, so a subclass cannot forget to. Were `process`
overridable, the failure mode would be an operator reading state under whichever
key was current from the *previous* record -- plausible, wrong, non-deterministic
results that no type would catch.

**Why this removes locking rather than optimising it.** Partitioning guarantees
one processor per key, so keyed state has no shared resource to protect. The
alternative -- one map behind a mutex, contended by every thread on every record
-- does not merely run slower, it fails to scale, because contention grows with
thread count. Real engines partition rather than lock for exactly this reason.

**Accepted cost:** a serialize/deserialize round trip on every state access. Pure
overhead against a plain `std::map<Key, T>` for the in-memory case, and what
makes the backend swappable and lets Stage 7 snapshot state whose type it knows
nothing about.

---

### D-039 — The file backend batches; it does not write through

**Rejected — writing to the file on every `put`.** That turns a per-record
operation into a syscall and a disk flush. A pipeline at 100k records/sec would
issue 100k fsyncs/sec and manage a few hundred. It is not a slow implementation
of the right idea, it is the wrong idea.

**Chosen:** reads and writes hit memory; `flush()` persists everything at once.
This is the same shape real engines use and is exactly what makes it compatible
with checkpointing, which is already a "persist at a consistent point" operation.

`flush()` writes to a temporary and renames over the target. `rename` within a
filesystem is atomic, so a crash mid-flush leaves the previous complete file
rather than a truncated one -- and a truncated state file is far worse than a
stale one, because it fails at restore time.

**What production would do differently:** rewriting all state per flush is
O(total state), fine for a checkpoint every few seconds and hopeless for large
state. RocksDB -- what Flink uses -- is an LSM tree, so a checkpoint costs
O(changed state). Out of scope, and notably **the interface would not need to
change** to accommodate it.

---

### D-040 — `snapshot_state` / `restore_state` on `OperatorBase`, defaulted to no-ops

The stage brief asked to design an interface for a future requirement without
building it. Two kinds of state exist and they need different homes:

- **Keyed state** — partitioned by key, lives in `StateBackend`, and in Stage 6
  partitions across threads alongside the records.
- **Operator state** — not keyed: a source's read offset, a window operator's
  open windows, a watermark generator's high-water mark. Lives in the operator
  and is captured through `snapshot_state`.

Defaulted to no-ops because most operators are stateless: map and filter hold
nothing between records and so say nothing about checkpointing at all.

Stage 7 adds barriers, alignment and a coordinator that **call** these; it does
not change them. `StateBackend::write_snapshot`/`restore_snapshot` are already
implemented and tested, since round-tripping a backend is meaningful on its own.

---

### Known gap — keyed windowing

Stage 3's windows are global to the stream and Stage 4's keyed state is not yet
wired into them, so per-key windows do not exist yet. Stage 9's demo query
(per-region revenue in 5-minute tumbling windows) needs them.

Deferred deliberately rather than overlooked: the stage brief scoped Stage 4 to
`keyBy`, backends and serialization. The change is to give `WindowOperator` a key
selector and nest its window map one level deeper, and it is the first thing to
do when the demo application is built.

---

### D-041 — `BoundedQueue`: the backpressure primitive

**Chosen:** mutex + two condition variables + `std::deque`, fixed capacity,
blocking `push`/`pop`, plus a `close()` that wakes everyone.

**Two condition variables, not one.** With a single variable a producer freeing a
slot cannot tell whether the waiters are producers or consumers, so it must
`notify_all` -- waking every blocked thread so that one can proceed. Two
variables let each notification reach only threads that can act on it.

**`notify_one` on the normal path, `notify_all` on close.** One item became
available, so one consumer can progress; waking the rest is a thundering herd
that costs context switches and buys nothing. `close()` is the exception --
every waiter must learn the stream is over.

**Rejected — a lock-free queue.** Genuinely faster under contention, and the
wrong tool: we *want* to block, because blocking is the backpressure. A lock-free
queue would also be unbounded or spin, and would be far harder to prove correct.
Wrong instinct for a throughput system; right one for the order-book project.

---

### D-042 — Why `wait` takes a predicate

Two independent reasons, and knowing only one produces code that is wrong in the
other case:

1. **Spurious wakeups.** `wait` may return with no notification at all -- the
   standard permits it because preventing it costs more on some platforms than
   re-checking does.
2. **Stolen wakeups.** Even a genuine notification proves nothing by the time
   the woken thread runs: between the notifier releasing the lock and this
   thread reacquiring it, another consumer can take the slot. The condition was
   true when signalled and false on arrival.

The predicate overload is `while (!pred()) wait(lock);`, which handles both by
construction.

---

### D-043 — `close()` sets its flag under the lock: the lost-wakeup bug

`cv.wait(lock, pred)` holds the mutex while `pred()` runs, then *atomically*
releases the mutex and enqueues the thread on the condition variable — atomic
with respect to the mutex, so a thread that also takes the mutex cannot slip
between those steps.

Set `closed_ = true` **without** the lock and it can. A consumer evaluates the
predicate, reads `closed_ == false`, and decides to sleep; `close()` then sets
the flag and calls `notify_all()` before the consumer has enqueued itself; the
consumer enqueues and sleeps forever, having missed the only notification it was
ever going to get. The program hangs at shutdown — intermittently, under load,
usually not on the machine you tested on.

Notifying *outside* the lock is fine and marginally cheaper: a thread woken while
the notifier still holds the mutex would only block again on acquiring it.

---

### D-044 — TSan only catches a race between accesses that genuinely overlap

Verified experimentally rather than assumed, and the most useful thing learned in
this stage.

The lock was removed from `close()` and the suite re-run under ThreadSanitizer.
**Every timing-based test still passed.** They sleep 150 ms before closing, so the
consumer is parked inside `wait` long before the writer runs; TSan reports a race
only while it still holds the earlier access in its shadow history, and a gap
that large evicts it.

A probe hammering `close()` against concurrent `pop()`/`is_closed()` with no
sleeps reported it immediately:

```
WARNING: ThreadSanitizer: data race
  Write of size 1 by thread T2:                             <- close(), unlocked
  Previous read of size 1 by thread T1 (mutexes: write M0): <- predicate, locked
SUMMARY: ... bounded_queue.hpp:141 in BoundedQueue<int>::close()
```

`CloseIsSafeAgainstConcurrentPopsAndObservers` was added to the suite as a
result, and confirmed to fail with the bug and pass with the fix.

**The generalisation, which is the point:** a green TSan run over tests that never
actually contend proves nothing. Concurrency tests must create genuine overlap,
not merely involve more than one thread. This is the same theme as D-004 (UBSan
that recovers) and the Stage 0 quiz: *a check that can pass while the property it
protects is violated is worse than no check*, because it converts acknowledged
ignorance into confident wrongness.

---

### D-045 — Shutdown is two steps, and a stop request is not one of them

`std::stop_token` is **cooperative**: it sets a flag and runs callbacks. It
interrupts nothing. A worker blocked inside `std::condition_variable::wait` --
which is where `BoundedQueue::pop()` puts it -- knows nothing about stop tokens
and will sit there forever regardless of how many times `request_stop()` is
called.

Correct shutdown, in order:
1. **close the queues**, which wakes every blocked producer and consumer;
2. **request stop and join**, which `WorkerGroup`'s destructor does.

Do only the second and the pipeline hangs. This is the single most common
multi-stage-pipeline shutdown failure, and
`WorkerGroupTest.StopRequestAloneDoesNotWakeAWorkerBlockedOnAQueue` asserts the
trap **directly** — it verifies that a stop request does *not* wake the worker.
That is the contract, not a defect being tolerated, and encoding it stops
someone later "fixing" shutdown by adding another `request_stop()`.

**Rejected — `std::condition_variable_any` with its stop-token-aware `wait`.** It
would collapse the two steps into one. It works with any lockable rather than
only `unique_lock<mutex>` and pays for that generality on every wait, and
closing the queue is the more honest model anyway: "this stream is finished" is
information the queue has, not the thread.

Every test target now carries a CTest `TIMEOUT` of 60s, so a shutdown regression
fails the build instead of hanging CI forever.

---

### D-046 — `std::jthread` and RAII thread lifetime

`std::thread`'s destructor calls `std::terminate()` if the thread is still
joinable, making every `std::thread` a hand-written `join()` on *every* exit path
including the exception path -- and the one that gets forgotten kills the
process. `std::jthread` requests a stop and joins in its own destructor, so
correct shutdown is the default rather than a discipline.

`WorkerGroup` is therefore mostly `std::vector<std::jthread>`. What it adds:

- **Exception capture.** An exception escaping a thread's entry point calls
  `std::terminate` -- the process dies with a stack trace from the wrong thread
  and no indication of which worker failed. Catching at the boundary turns that
  into a recorded `WorkerFailure`.
- **Member ordering.** `workers_` is declared *last* so it is destroyed *first*
  (reverse declaration order), guaranteeing the jthreads join before the failure
  list and its mutex are gone. The explicit `join()` in the destructor body
  already ensures this; the ordering means the class stays sound if that line is
  ever removed.

---

### D-047 — `QueueCollector`: the Stage 1 seam pays off

`Collector<T>` was defined in Stage 1 as an *interface* precisely so a
queue-pushing implementation could replace the direct-call one. Swapping it in
made the engine parallel and **not one line of operator code changed** -- map,
filter, window and keyed-aggregate are all still written against
`Collector<T>&` and still just call `out.collect(...)`.

This is the concrete vindication of rejecting CRTP in D-014: a compile-time
welded chain has no seam to insert a queue into. The cost paid there (one virtual
call per record per operator) bought exactly this.

`collect` blocking on a full queue *is* the backpressure. There is no signalling,
no measurement, and no rate limiter anywhere in the design.

---

### D-048 — `StreamElement` is a variant, and only in the transport

The queues carry `std::variant<Record<T>, ChannelWatermark, ChannelClosed>`.

This is the case D-014 explicitly reserved variants for. Payload types are an
**open** set defined by users, so a variant over them would be a central registry
everyone must edit. Stream elements are the opposite: a **closed set fixed by the
engine** -- data, time progress, end of channel. Stage 7 adds a fourth
alternative for barriers and nothing else changes shape.

**The layering matters.** The operator interface never sees this type: operators
have `process` and `on_watermark` as separate virtuals (D-021), so map and filter
still need no knowledge that watermarks exist. The variant lives only in the
transport, because a queue must carry one concrete type. Interface shape and
transport shape are separate decisions, and getting that wrong in either
direction would have forced every operator to dispatch on an alternative it does
not care about.

---

### D-049 — Fan-out closes queues; fan-in counts channels

An asymmetry worth stating because it is not arbitrary.

**Input queues (one producer, the source):** `close()` is sufficient. `pop()`
returning `nullopt` means the stream is over.

**The sink queue (N producers):** `close()` is *wrong* -- the first subtask to
finish would close the queue out from under the others. End-of-stream must
therefore travel **as an element** (`ChannelClosed{channel}`), and the consumer
exits once it has counted one from every channel.

`ChannelWatermark` carries its channel index for the same structural reason: a
fan-in consumer must take the **minimum** across channels, and an untagged
watermark would look like progress on every channel at once -- which is the
"take the maximum" mistake that silently discards on-time data from slower
channels.

This is where `WatermarkTracker` (D-026, built in Stage 2 with nothing to consume
it) finally does its job.

---

### D-050 — One state backend per subtask, not one shared behind a lock

Each subtask gets its own operator instance **and its own `MemoryStateBackend`**.

That is what makes keyed state lock-free rather than merely fast: hash
partitioning guarantees one subtask per key, so no two threads ever touch the
same state and there is nothing to protect. Sharing one backend behind a mutex
would not simply run slower -- it would fail to scale, because contention grows
with thread count. Partitioning removes the shared resource instead of guarding
it.

The property is asserted rather than assumed:
`RoutesEveryRecordForAKeyToTheSameSubtask` checks each key's final running total,
which can only be correct if every one of that key's records was accumulated in
one place. Split a key across subtasks and each keeps a partial total that is
never reconciled -- wrong, plausible, and non-deterministic.

---

### D-051 — Key skew is instrumented, because it is otherwise invisible

`ParallelMetrics::records_per_subtask` exists to make skew legible. Hash
partitioning sends every record for one key to one subtask, so a single hot key
caps the whole job at one core's throughput regardless of configured parallelism.
**Adding threads cannot fix it; only changing the key can.**

Asserted directly in `ConcentratesAHotKeyOnASingleSubtask`: with one key and four
subtasks, one handles everything and three are idle. Without the metric this
presents as "the job is mysteriously slower than the thread count suggests".

Queue capacity defaults small (64) deliberately. A large capacity delays the
onset of backpressure and hides a slow stage behind a deep buffer; it does not
make the pipeline faster, only the problem later and less visible.

---

### D-052 — Backpressure is demonstrated with a negative control

`PropagatesBackpressureFromASlowSinkToTheSource` uses a deliberately slow sink
and asserts both counters rise: blocks on the sink queue (the sink is the
bottleneck) *and* blocks on the input queues (the pressure reached the source).
Records written must equal records produced -- backpressure throttles, it never
drops.

`DoesNotBlockOnTheSinkQueueWhenTheSinkKeepsUp` is the negative control, and it is
the more important test of the two. Without it the first would pass on a pipeline
that blocks unconditionally -- which would look like working backpressure and be
a permanent throughput ceiling.

---

### D-053 — False sharing on queue metadata

Each queue is a separate heap allocation (`unique_ptr`), so one queue's mutex and
block counters do not share a cache line with a neighbouring queue's. Two
subtasks hammering adjacent mutexes on one line would ping-pong that line between
cores -- contention on data the threads never actually share.

Whether padding *within* a queue matters (between `not_full_` and `not_empty_`,
say) is left to Stage 9 to measure. Adding `alignas` on a guess is exactly the
micro-optimisation this project defers until a benchmark points at it.

---

### Bug found by the test suite — dangling sink pointer

The first version of the oracle test built the sequential `Pipeline` in an inner
scope and read its sink through a raw pointer afterwards. `Pipeline` **owns** its
sink, so the pointer dangled the moment the pipeline was destroyed. It segfaulted
immediately.

Recorded because the fix is a habit rather than a patch: results are now copied
out *inside* the owning scope. The same shape -- `sink.get()` before
`.to(std::move(sink))` -- appears throughout the test suite and is only safe while
the pipeline outlives the read.

---

### D-054 — Chandy-Lamport: barriers travel with the data

**Chosen:** the source injects a `CheckpointBarrier` into the stream; it flows
through the same queues as records, in order; each task snapshots when the
barrier reaches it, acknowledges to the coordinator, and forwards it on.

**Rejected — a stop-the-world pause.** Halt every task, snapshot, resume. It is
simple and correct, and unusable: throughput drops to zero for the duration, and
it gets *worse* with more tasks because you wait for the slowest every time. At a
checkpoint every few seconds a job could spend a third of its life paused.

**Why in-band works.** A barrier cannot overtake the records ahead of it, so a
task receiving barrier N knows every record before it has already been processed
**by that task** and none after it has. Its state therefore reflects exactly the
prefix of the stream ahead of the barrier -- and every other task independently
reaches the same conclusion about its own prefix. Add the snapshots together and
they describe one consistent cut, with nobody ever having stopped. That is the
whole algorithm, and it is the same ordering property that makes watermarks work
(D-021).

**Barriers are broadcast, not partitioned** -- like watermarks, for the same
reason. A barrier is an instruction to every task; routing it to one subtask by
key would leave the others never snapshotting, so the checkpoint could never
complete.

---

### D-055 — A checkpoint is unusable until every task acknowledges

The coordinator holds a checkpoint in `pending_` until it has one snapshot per
task, and only then does it become visible through `latest_completed()`.

A partial checkpoint is not partially useful, it is **corrupt**. Restoring from a
set where task 3 never reported would reset tasks 0-2 to the cut while task 3
kept state from some later point -- the cut would not be a cut, and exactly-once
would be silently untrue.

For the same reason a late acknowledgement for an already-completed checkpoint is
**ignored** rather than applied. Accepting a straggler would replace one task's
snapshot with state from a different point, and the set would no longer describe
any single cut.

The `source_offset` lives in the same record as the task state because they are
one atomic fact about the same cut. Restoring state without rewinding the source
would skip every record in between.

---

### D-056 — Barrier alignment, and what it buys

The sink is the only multi-input operator, so it is the only place alignment
happens.

When barrier N arrives on channel `c`, everything that follows on `c` belongs
*after* the cut and must not touch the state being snapshotted. So the sink sets
those elements aside and replays them once every channel has delivered its
barrier.

**What it buys:** the snapshot reflects exactly the pre-barrier prefix of every
input. **What it costs:** latency -- the operator waits for its slowest input.

That is precisely the aligned (exactly-once) versus unaligned (at-least-once)
distinction, and it was **demonstrated rather than asserted**. Disabling the
alignment branch and re-running produced:

```
checkpoint 1 snapshotted 32 records but the source was at offset 20
checkpoint 2 snapshotted 50 records but the source was at offset 40
```

Twelve post-cut records baked into the first checkpoint. On recovery those would
be replayed *and* already present in the restored state -- counted twice. Exactly
the double-count alignment exists to prevent.

Note that `BarriersDoNotOvertakeRecords` still passed with alignment disabled,
which is correct: subtasks have a single input each and need no alignment. Only
the fan-in operator does.

**Implementation difference from Flink, stated honestly.** Flink stops *reading*
the channel, which backpressures the sender. Ripple buffers in the consumer
instead, because its fan-in is a single shared queue rather than one channel per
upstream. The cut -- and therefore the semantics -- is identical; the trade is
backpressure-during-alignment for memory-during-alignment.
`ParallelMetrics::alignment_buffered_elements` exists so that cost is visible.

---

### D-057 — Snapshotting without stopping the world

Each task serializes its own state **synchronously, on its own thread, with no
lock at all**.

That is not a shortcut. Partitioning (D-050) means no other thread can touch this
task's state, so there is nothing to exclude. The only cost is that one subtask
stalls while serializing; every other subtask keeps running and no global pause
ever occurs.

**Rejected — brief locking.** Take a lock, serialize, release. Pointless here:
there is no second thread to lock against.

**Rejected — double-buffering.** Keep two copies, swap, serialize the inactive
one on a background thread. Removes the stall by *doubling* steady-state memory,
which for a keyed job with large state is the dominant cost.

**Rejected — copy-on-write.** Snapshot logically and pay only for state mutated
during the snapshot. Best asymptotics and by far the most complex: with no OS
page-level support it means a persistent data structure for every state type,
pushing a heavy constraint into `StateBackend` for a stall nobody has measured.

Synchronous serialization is the right default at these state sizes. Stage 9 is
where a benchmark would say otherwise.

---

### D-058 — Forward the barrier before snapshotting

The cut is identical either way -- no record is processed between the two -- but
forwarding first lets downstream begin aligning while this task is still
serializing. That keeps end-to-end checkpoint duration close to the **slowest
single task** rather than the sum of all of them, and it is what real engines do.

---

### Caught by the two-compiler CI (D-006)

`for (const std::string& zone : {"a", "b", "c", "d"})` binds the reference to a
temporary `std::string` constructed on **every iteration**. Clang accepts it
silently; GCC's `-Wrange-loop-construct` rejected it.

Recorded because it is the first time the second frontend paid for itself, and it
is exactly the class of thing D-006 predicted: a single compiler accepts
sloppiness that is not obviously wrong until another one names it.

---

### D-059 — Recovery: state and source offset are one fact, restored together

`RunOptions::restore_from` repopulates each subtask's state backend from the
checkpoint **and** rewinds the source to the offset recorded in that same
checkpoint.

Doing one without the other is not a partial fix, it is a different bug in each
direction:
- restore state, start the source from zero → every record before the cut counted
  twice;
- rewind the source, start with empty state → everything before the cut lost.

The second was confirmed by experiment. Disabling `restore_snapshot` while
leaving the rewind produced final totals of **9 where the correct answer was 24**
-- precisely the pre-cut records dropped. That is the third use in this project of
"break the property, confirm the check fails, restore" (see also D-044 and
D-056).

Snapshots are read back in the same order they were written -- backend first,
then operator state -- and `deserialize`'s full-consumption rule (D-034) is what
catches the two drifting apart. A silent mismatch here would restore plausible
but wrong state during a recovery, which is the worst possible moment for it.

**Constraint worth stating:** the parallelism must match the run that produced
the checkpoint. Subtask *i* restores task *i*'s snapshot, and hash partitioning
sends a key to `hash(key) % parallelism` -- change the parallelism and keys land
on subtasks holding somebody else's state. Real engines solve this with **key
groups**: a fixed number of hash buckets assigned to subtasks, so rescaling
redistributes buckets rather than rehashing keys. Out of scope here; recorded as
a real limitation rather than an oversight.

---

### D-060 — What "exactly-once" actually means, and who has to provide it

Exactly-once does **not** mean each record is delivered once. Records are
absolutely re-sent after a failure -- replay is how recovery works. It means the
**effect on state** is as if each record were processed exactly once.

The end-to-end property needs three things, and the engine supplies only one:

1. **A replayable source** -- one that can be rewound to a position. A file or a
   Kafka topic can; a UDP socket cannot, and no amount of engine correctness
   recovers data that is simply gone.
2. **Consistent snapshots** -- Stage 7. *This is the only part the engine owns.*
3. **An idempotent or transactional sink** -- otherwise a perfect engine still
   double-counts in the outside world. Writing "$500 revenue for Midtown" twice
   into a database that *adds* is a real error no checkpoint can undo.

`DeliveryIsAtLeastOnceWhileTheEffectIsExactlyOnce` asserts both halves in one
test, deliberately: the interesting claim is not "the answer is right", it is
"the answer is right **despite** duplicate delivery". It checks that the sink
genuinely received more writes than there were input records *and* that the final
per-key totals match an uninterrupted run.

`AnAppendingSinkWouldDoubleCount` is the negative control, and it is the more
important of the two. Without it, correct results could simply mean no replay
occurred, and the sink requirement would look like an unnecessary precaution.

**Why two-phase commit appears in real sinks.** Upserting works when results are
keyed and replacing is meaningful. When they are not -- appending rows, sending
emails, charging cards -- the sink writes into an uncommitted transaction and
commits only once the checkpoint containing those writes is confirmed complete.
Pre-commit on barrier, commit on checkpoint-complete notification: the same two
phases, driven by the checkpoint protocol.

---

### D-061 — The fault-injection harness, and its fidelity limit

`fail_after_records` stops the source mid-stream; all in-memory state is then
discarded when `run` returns, which is the part that matters. The harness kills
the job at seeded pseudo-random offsets, recovers from whatever checkpoint had
completed, resumes, and asserts the final state matches an uninterrupted run
every time.

Seeded deliberately: **a fault-injection harness that cannot replay its own
failure is close to useless.**

**What it does not simulate**, stated plainly rather than glossed: losing records
already in flight, because terminating threads mid-operation cannot be done
safely in-process. This does not weaken the property under test -- recovery
replays from the last completed checkpoint's offset, which re-delivers exactly
the records a graceful drain may have delivered.

Cases covered on purpose: failure *before* any checkpoint completes (replay from
the beginning is correct, not exceptional), failure immediately after one, and
repeated failures where each recovery must start from the **newest** completed
checkpoint rather than the first.

A crash also does **not** emit the end-of-stream watermark. Announcing that time
has advanced to infinity on the way out would fire every open window, which is
exactly what a crash does not do.

---

### D-062 — Key groups: state that survives a rescale

Supersedes the limitation recorded under D-059.

**The problem.** Routing on `hash(key) % parallelism` works until the parallelism
changes, at which point every key moves and none of them find their state.
Scaling a stateful job up or down would mean starting from zero.

**Chosen.** A fixed number of hash buckets -- `kMaxKeyGroups = 128` -- decided
once and **independent of parallelism**. A key's group is
`hash(key) % kMaxKeyGroups` and never changes; only *which subtask owns that
group* does. Each subtask owns a contiguous range of groups, snapshots are tagged
by group, and a restore has every subtask read every old snapshot and keep the
groups it now owns.

The cost is a ceiling on parallelism -- a job can never have more subtasks than
key groups, because a group cannot be split. Flink makes the same trade with the
same default and the same consequence: changing it later invalidates every
existing checkpoint.

**A latent bug this fixed.** The pipeline previously took a `key_hash`
(payload → hash) while `KeyedOperator` separately took a `KeySelector`
(payload → key), and *nothing enforced that the two agreed*. Disagreement would
route records to a subtask that did not hold their key's state and split the
results silently. There is now one key selector, and the group is derived from
the serialized key bytes, so routing and snapshot layout agree by construction
rather than by convention.

**Why the hash is hand-rolled.** `std::hash` offers no stability guarantee: it may
be salted per process and certainly differs between standard library
implementations. A key group is written into a checkpoint and read back by a
different run, possibly a different build -- an unstable hash would scatter every
key to a different group on restore and silently lose all state. FNV-1a over the
serialized bytes is stable, and the test pins literal values so that changing it
is impossible to do by accident.

**What still does not survive a rescale:** *operator* state, restored only when
the parallelism is unchanged. Redistributing it needs a per-operator merge rule
(Flink's union-list and split-list redistribution), and no operator in this engine
holds any, so the machinery would be untested weight.

**Accepted cost:** the key is serialized twice per record -- once by the pipeline
to compute the group, once by `KeyedOperator` to scope the state. Correctness and
a single source of truth first; Stage 9 measures whether it matters.

---

### D-063 — Exhaustive property tests over sampled end-to-end tests

The two directions of the key-group mapping -- "who owns this group" when routing,
"which groups do I own" when restoring -- must agree at every parallelism. They
are checked **exhaustively**, over every (parallelism, subtask, group) triple.

That thoroughness is not decorative, and the reason is worth recording. Breaking
`key_group_range_for` with a floor instead of a ceiling was caught immediately by
the property tests -- and the **end-to-end rescale tests passed anyway**. With five
zones they sample five of 128 groups, and none happened to land on the broken
boundary.

An end-to-end test that samples the space thinly is not a proof. This is the same
lesson as D-044 (TSan needs genuine contention) and D-056 (alignment verified by
removing it), arriving from a third direction: *a green test proves only what it
actually exercised.*

---

### D-064 — Windows become keyed, sharing one implementation with the global case

Closes the gap recorded at the end of Stage 4. `WindowOperator` now takes a
`KeySelector` (whose window is this) and a `ValueSelector` (what part of the
record is aggregated), holding `key -> window -> accumulator`.

With the default `GlobalKeySelector` there is exactly one key and the operator
behaves as an unkeyed windower, so the two cases share one implementation rather
than one being a copy of the other with an extra map.

Windows are nested *under* the key rather than keyed by a flat `(key, window)`
pair, because session merging must scan a single key's windows in time order. A
flat map ordered by `(key, window)` would happen to work, but only by accident of
that ordering.

**A leak this introduced and closes:** purging a key's last window leaves an
empty entry in the key map. Without erasing it, the window maps shrink while the
*key* map grows forever -- unbounded for something like a user id. The same
failure the state backend guards against in D-037, asserted by
`ReleasesKeysWhoseWindowsHaveAllExpired`.

---

### D-065 — Operator chaining

`ChainedOperator<In, Mid, Out>` runs two operators as one, with the first writing
into an adapter that is a `Collector<Mid>` feeding the second.

**Why chain rather than give each operator its own thread and queue:** for
adjacent stages a queue is strictly worse -- every record pays a mutex, a
condition-variable notify and a thread handoff to travel a few nanoseconds of
actual work. Real engines chain and break the chain only where the topology
forces a shuffle. The measured queue handoff cost below makes this concrete.

It needs no cooperation from either operator: neither knows it is chained. This
is the third distinct use of the `Collector` seam from D-014, after the
direct-call collector and the queue collector.

Watermarks route *through* the adapter rather than around it, deliberately. The
first operator may react to a watermark by emitting records (a window firing),
and those records must reach the second before the watermark does -- otherwise
the second treats its upstream's output as late.

---

### D-066 — Window contents are operator state, and were not being checkpointed

**A real bug, found by running the demo application rather than by reasoning.**

Window state lives in the operator's own map, not in the `StateBackend`, because
it is indexed by `(key, window)` which the backend's flat key-value interface
does not model. The backend snapshot therefore does not cover it -- and
`WindowOperator` inherited the no-op `snapshot_state` from D-040. **A windowed
job checkpointed nothing at all.**

The symptom was not a crash. Recovery restarted every partially-filled window
from empty and produced totals that were plausible and quietly short. The demo
surfaced it as "results identical to an uninterrupted run: NO".

Fixed by implementing `snapshot_state`/`restore_state` on the window operator,
including `current_watermark_` -- restoring windows without it would leave the
operator believing no time had passed, so it would re-admit records it had
already declared late, resurrecting data into windows downstream had been told
were final.

Two regression tests now cover it, including that a window which had already
fired does **not** fire again after restore: the `dirty` flag is part of the
state, and re-firing would duplicate a completed window.

*The general lesson: the Stage 4 decision to default `snapshot_state` to a no-op
was right for map and filter, and it silently made a stateful operator look
checkpointed when it was not. A default that is correct for most implementers is
still a trap for the ones it is wrong for.*

---

### D-067 — Benchmark methodology, and two ways a harness lies

**Percentiles, never a mean alone.** A mean latency hides exactly the behaviour
that matters: a pipeline answering in 1ms 99% of the time and 900ms the rest has
a mean near 10ms, describing no request that ever happened. p50/p90/p99/p99.9 and
max are reported; the mean is printed last, labelled, and only alongside them.

**Google Benchmark for components, a custom harness end to end.** Google
Benchmark repeats a small operation until the timing stabilises, which is the
wrong shape for "run a multi-threaded pipeline once and describe how it
behaved" -- that produces a distribution and a few one-shot durations.

**Two measurement bugs found and fixed while building this**, both of which
produced confident, wrong numbers:

1. **Pacing finer than the scheduler.** At 200k rec/s the per-record interval is
   5us; `sleep_until` overshoots by an order of magnitude, the deadline goes
   permanently into the past, and pacing silently degrades to running flat out --
   while the harness still labels the result "paced latency". It was measuring
   saturation. Fixed by pacing in batches of roughly a millisecond's worth of
   records.
2. **Measuring against the wrong clock origin.** Timing from before `run()` folds
   pipeline setup -- thread spawn, backend construction -- into every record as a
   constant offset. It showed as a uniform ~3.4ms p50 with a suspiciously tight
   spread. Fixed by exposing `ParallelMetrics::source_started_at`, the origin the
   source actually paced against.

A third artifact followed the first fix: batching means every record in a batch
enters together, so measuring each against its own nominal slot reports negative
latency for all but the first. `pacing_batch_size` is public so the harness
computes the same schedule the source used.

*All three produced plausible-looking numbers. A benchmark that is wrong is worse
than no benchmark, for the same reason a green test that exercises nothing is --
it converts ignorance into confidence.*

---

### D-068 — What the numbers actually say

Measured on an 11th Gen Intel i5-1135G7 (4 cores / 8 threads, 2.4 GHz), 8 GB RAM,
WSL2 on Ubuntu 24.04, Clang 18, `release` preset (`-O3 -DNDEBUG`).

| Component | Result |
| --- | --- |
| Tumbling window assignment | ~1.1 ns/record |
| Sliding assignment, 2 / 10 / 60 windows | 5.7 / 16.1 / 104 ns -- **linear in windows per record**, as D-028 predicted |
| Key-group hash | ~12 ns |
| Serialize int64 / 64-byte string | 31 / 56 ns |
| Keyed state RMW, 1 / 100 / 10k keys | 80 / 135 / 257 ns -- the ordered map's O(log n) |
| Queue push+pop, uncontended | 22 ns |
| Queue 1P1C, capacity 8 | **173k items/s** |
| Queue 1P1C, capacity 1024 | **4.35M items/s** |

**The headline finding is the last two rows: a 25x throughput difference from
queue capacity alone.** At capacity 8 the producer and consumer ping-pong on the
condition variable and spend their time in the scheduler rather than moving data.
That is the concrete argument both for chaining adjacent operators (D-065) and
against a small default capacity -- and it is the kind of thing that is invisible
without a benchmark, which is exactly why optimisation was deferred to this stage
rather than guessed at earlier.

End-to-end, 4 subtasks, Zipf-skewed keys: ~555k records/s unpaced; at a paced
200k/s the latency distribution is p50 346us, p99 1.16ms, p99.9 1.37ms. Note that
throughput *fell* from parallelism 4 to 8 -- at parallelism 8 the job wants ten
threads on an 8-thread machine, and oversubscription costs more than the extra
width buys.

Checkpointing every 50k records cost **3.5%** throughput, with checkpoint
durations around 2ms. Recovery after a kill at the halfway point took 0.24s,
dominated by replaying the 100k records since the last checkpoint rather than by
loading state -- which is the real argument for frequent checkpoints, and the
number to trade the 3.5% against.

---

### D-069 — The demo, and the poison-timestamp hazard

Per-zone revenue in 5-minute tumbling windows, plus sessionization by vehicle
with a 30-minute inactivity gap, both on the parallel runtime with checkpointing,
followed by a kill and recovery.

The generator reproduces the properties of the real TLC data that made it worth
choosing (D-013) -- skew, out-of-order arrival, and dirty records -- while
running anywhere with no download. It is seeded.

**The hazard the dirty data exposed**, which is the most useful thing the demo
teaches: bounded out-of-orderness derives the watermark from the **maximum event
time seen** (D-024). One trip stamped in 2096 drags the watermark to 2096 minus
the bound, and every subsequent record -- all valid -- is now late. **A single
broken meter silently discards the rest of the stream.**

The watermark generator cannot defend itself: monotonicity is exactly what makes
watermarks safe, so it cannot regress. The defence must be upstream -- reject
implausible timestamps before they can influence time at all.

And the bound has to be tight enough to actually catch the bad data. The first
version of the sanitizer allowed anything before the year 3000 while the broken
meters stamp 2096, so the poison sailed through and **roughly 70% of all windows
silently never fired**. The symptom was not an error but a plausible-looking
report missing most of its rows. A real deployment validates against a
*business*-plausible range ("not more than an hour ahead of ingestion"), not
against the limits of the timestamp type.

---

### D-070 — Window state redistributes by key group too

Found by writing the test that should have existed all along: a *windowed*
parallel pipeline recovering across a rescale. It failed, and the failure was
real -- a rescaled windowed job silently restarted every window from empty and
under-reported, the same quiet failure as D-066.

D-062 had said operator state cannot be redistributed on a rescale. That is true
in general, and **false for window state specifically**, because window state is
keyed by exactly the key the shuffle partitions on. So it redistributes by key
group like backend state.

`OperatorBase::restore_state` now takes a `KeyGroupRange`, and the pipeline
offers **every** old snapshot to **every** subtask. An operator with keyed state
keeps the groups it now owns and drops the rest, additively across calls; one
with genuinely unkeyed state ignores the range and restores nothing on a rescale.
That distinction is precisely why Flink separates keyed state from operator state
with union-list and split-list redistribution.

One detail worth stating: the restored watermark is the **maximum** across
snapshots, not the last one read. Taking the last would make event-time progress
depend on the order the blobs happened to be iterated in.

**The blind spot this exposes, which is the more useful lesson:** every recovery
test used a keyed aggregate, whose state lives in the backend. Not one exercised
an operator holding its own state, so an entire category was untested while the
suite looked thorough. *A test suite that exercises one kind of state proves
nothing about the other kinds.*

---

### D-071 — Comparison against a baseline, not against Flink

**Rejected: benchmarking against Flink.** A fair comparison needs identical
hardware, workload, semantics **and durability guarantees** -- Ripple checkpoints
to memory, Flink to durable storage; Ripple is in-process, Flink serializes
across a network even locally. Any number showing Ripple "winning" would be
comparing different things and would not survive one follow-up question. An
unfair benchmark is worse than none.

**Chosen: a hand-written loop over an `unordered_map`** doing the same
aggregation with no engine at all.

| | records/sec |
| --- | --- |
| No engine (hand-written loop) | ~34,000,000 |
| Ripple, parallelism 4 | ~725,000 |

**The engine costs roughly 47x a bare loop.** That is the honest headline, and it
is the number that says where to look: the component benchmarks (D-068) sum to
about 250ns of real work per record, so the overwhelming majority of the time is
coordination -- per-record queue handoffs, allocation, and serialize/deserialize
on every state access -- rather than the aggregation itself.

The baseline provides none of event time, windowing, checkpointing, recovery,
parallelism, or backpressure. It is a lower bound on cost, not an alternative.
The 47x is what those properties cost as currently implemented, and the obvious
first optimisation is batching across queues rather than one record per handoff
-- which the 25x queue-capacity finding already pointed at.

---

### D-072 — Single benchmark runs are not measurements

Checkpoint cost measured **3.5%** on one run and **-0.1%** on the next; unpaced
throughput at parallelism 4 has ranged from 546k to 725k rec/s across runs.

Same binary, same input, same machine. The variance is the machine -- a laptop
under WSL2 with 8 threads, no CPU pinning, no isolation, thermal and scheduler
noise. Any single number from this harness is indicative, not authoritative.

Recorded rather than quietly averaged away, because quoting one run as a result
is the same species of error as quoting a mean latency without its tail: it
presents a distribution as a fact. The harness should repeat and report a
distribution over runs, which is the obvious next improvement to it.

---

## 4. Stage status

| Stage | Description | Status |
| ----- | ----------- | ------ |
| 0 | Scaffolding, sanitizers, CI | **Complete** |
| 1 | Core dataflow, type erasure | **Complete** — 21 tests, clean under dev/gcc/asan/tsan |
| 2 | Event time and watermarks | **Complete** — 32 tests, clean under dev/gcc/asan/tsan |
| 3 | Windowing | **Complete** — 53 tests, clean under dev/gcc/asan/tsan |
| 4 | Keyed state, backend, serialization | **Complete** — 84 tests, clean under dev/gcc/asan/tsan |
| 5 | Concurrency primitives | **Complete** — 106 tests, TSan-clean under contention |
| 6 | Parallelism, partitioning, backpressure | **Complete** — 115 tests, TSan-clean over repeated runs |
| 7 | Checkpointing (Chandy-Lamport ABS) | **Complete** — 125 tests, alignment verified by disabling it |
| 8 | Recovery and exactly-once | **Complete** — 139 tests, fault injection + rescaling via key groups |
| 9 | Benchmarks and demo application | **Complete** — 146 tests, micro + end-to-end benchmarks, baseline comparison, taxi demo |

All stages complete. The README is the entry point; this file is the decision
record behind it.
| 10 | README | **Complete** |
