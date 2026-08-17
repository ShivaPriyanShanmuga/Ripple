# Interview Preparation Log

Concepts covered per stage, the comprehension questions asked, and — most
importantly — the questions that were answered incompletely or wrongly. That
last section is the revision list.

---

## Stage 0 — Scaffolding, sanitizers, CI

### Concepts covered

- **Stream vs. batch processing.** Unbounded input breaks the assumptions batch
  code relies on: cannot sort, no end-of-input to trigger output, restart from
  the beginning is not viable.
- **The three hard problems** a stream engine exists to solve: time, state,
  failure.
- **Dataflow model.** Source, operator, sink, pipeline as a DAG. Push vs. pull
  execution, and why streaming pushes — the producer controls arrival, so a
  consumer-driven pull model cannot express "data showed up."
- **Event time vs. processing time vs. ingestion time.** Why processing time
  makes output a function of network conditions and therefore untestable and
  unreproducible; why event time makes output a pure function of input.
- **Watermarks.** What one asserts ("I have seen everything up to T") and what it
  does not guarantee (that nothing older will arrive). Bounded out-of-orderness
  as a statement about the *spread of event times in the arrival stream*, not
  about any timestamp being wrong or late. The latency-vs-completeness dial.
  Why watermarks travel in-band rather than from a global clock. Why an operator
  takes the minimum across input channels. The idle-source stall.
- **Windowing.** Tumbling, sliding (and its memory multiple), session (and why
  only sessions require merging). What "the window fires" means mechanically,
  including that firing is how window memory is reclaimed.
- **Keyed state and partitioning.** Why partitioning the keyspace removes the
  need for a lock, rather than making the lock faster. Key skew as the cost.
- **Serialization.** C++ has no reflection, so state serialization must be
  hand-designed — the reason this is a Stage 4 design task rather than a library
  call.
- **Backpressure.** Why bounded queues are the mechanism, how blocking on a full
  queue propagates backwards to the source with no coordination, and why
  unbounded queues convert graceful degradation into an OOM kill.
- **Checkpointing.** Why stop-the-world snapshots do not scale. Chandy-Lamport
  barriers as in-band markers defining a consistent cut. Barrier alignment at
  multi-input operators, what it buys (exactly-once) and what it costs
  (latency). Aligned vs. unaligned.
- **Exactly-once.** That it means exactly-once *effect on state*, not
  exactly-once delivery. The three requirements for the end-to-end property:
  replayable source, correct snapshots, idempotent or transactional sink — and
  why two-phase commit appears in real sinks.
- **Sanitizers.** ASan, UBSan, TSan and what each detects. Why ASan and TSan
  cannot be combined (colliding shadow-memory layouts, both interposing on the
  allocator; rejected in the Clang driver). Why UBSan composes with either
  (inline instrumentation, no shadow region). Why races cannot be found by
  running tests.
- **Build tooling.** CMake presets, FetchContent, why dependency source builds
  are a sanitizer correctness requirement rather than a convenience.
- **Data race, definition.** Two threads, same memory, at least one write, no
  synchronization ordering them — and that in C++ this is undefined behaviour,
  not merely a wrong value.

### Questions asked

1. If `-fno-sanitize-recover=all` were dropped from the `asan` preset, CI still
   runs, tests still execute, and the job reports success. What has gone wrong,
   and why is that state *more dangerous* than having no UBSan at all?
2. TSan requires every translation unit instrumented. Why does an uninstrumented
   library cause **false positives** rather than only missed detections? What can
   TSan not observe, and what does it wrongly conclude?
3. Building Stages 1–4 single-threaded gives a free correctness oracle in
   Stage 6. What is the oracle, what property makes it valid, and name a class of
   Stage 6 bug it would **not** catch.

### Struggled with — revision list

- **Q1, second half.** Answered the mechanism (abort instead of continue) but not
  the reasoning: a silently-failing safety mechanism is worse than no safety
  mechanism, because it converts acknowledged ignorance into confident
  wrongness and misdirects debugging effort. Generalizes directly to Stage 7 — a
  checkpoint that silently completes with inconsistent state is worse than one
  that fails loudly.
- **Q2, not attempted.** The term "instrumented" was unfamiliar. Core idea to
  revise: TSan does not look for wrong values, it builds a **happens-before
  graph** and reports accesses with no path between them. Uninstrumented code
  hides *synchronization*, so an edge that really exists is never recorded, and
  correctly-ordered code is reported as racing. Nuance: TSan intercepts pthread
  calls at the library boundary, so the real exposure is atomics, custom
  spinlocks, and inline assembly. Partial instrumentation permits both false
  positives and false negatives.
- **Q3, not attempted.** Two things to be able to state cold: (a) the oracle is
  valid because event-time semantics make output a pure function of input, so a
  unique correct answer exists — this collapses under processing time; (b) the
  oracle and TSan find **disjoint** bug classes. A race can produce identical
  output on ten thousand runs and still be UB, so output comparison says nothing
  about race freedom; conversely the oracle catches thread-safe logic errors
  (wrong partitioner, watermark as max instead of min) that TSan cannot see.
  Also: the oracle is a *differential* test — it proves parallel equals
  sequential, not that either is correct.

---

## Stage 1 — Core dataflow and type erasure

### Concepts covered

- **The type-erasure problem.** Storing and operating on a heterogeneous set of
  operators whose input/output types differ, in a statically typed language.
- **Four designs and their tradeoffs** — CRTP/static chaining, `std::function`
  composition, `std::variant` payloads, virtual base with typed edges. See
  D-014 in DESIGN.md; be able to give the rejection reason for each.
- **Splitting one problem into two.** Ownership/traversal needs erasure; the
  data path does not. Recognizing that they are separable is the key move.
- **Move semantics in anger.** Why `Record<T>&&` states a contract rather than
  an optimization; use-after-move; that `std::move` on a `const` object silently
  copies because `std::move` is only a cast and a const rvalue cannot bind to
  `T&&`; that an un-`noexcept` move constructor makes `std::vector` *copy* on
  reallocation to preserve the strong exception guarantee.
- **Ownership models and their costs.** Value+move vs. `shared_ptr` (atomic
  refcount contention that worsens with thread count) vs. pooling (lifetime
  complexity at thread boundaries).
- **Push vs. pull at the implementation level.** Pull requires resumable
  operators — coroutines or state machines. Push does not, but must be given
  backpressure.
- **Chrono as a type-safety tool**, not just a clock: making
  `Timestamp + Timestamp` a compile error.
- **Template argument deduction does not see through inheritance.**
  `unique_ptr<Derived>` will not deduce against `unique_ptr<Base<T>>`; the
  conversion is available only after deduction succeeds. Solved with member type
  aliases plus `static_assert`.
- **Pointer stability under vector growth.** Why the pipeline's wiring slots
  survive `push_back` — the vector holds `unique_ptr`s, so reallocation moves
  pointers and leaves objects at fixed addresses.

### Questions to be able to answer cold

These were not asked interactively (the stage-gate quiz was suspended at the
author's request); they are the checklist for the revision pass.

1. Why does a compile-time operator chain (CRTP) make Stage 6 and Stage 7
   impossible, specifically? *(No seam to insert a queue; no runtime collection
   to walk for snapshotting.)*
2. `std::function` and virtual dispatch cost roughly the same. So what is the
   actual argument against `std::function`-based operators?
3. Why is `std::variant` right for Stage 2's stream element but wrong for record
   payloads?
4. Where does the atomic contention in `shared_ptr<Record>` come from, and why
   does it get *worse* as you add threads?
5. Name two cases where the move-only ownership model must fall back to a copy,
   and say where that copy belongs.
6. A colleague marks a payload type's move constructor without `noexcept`.
   Throughput drops. Why?
7. Why does `from(std::make_unique<VectorSource<int>>(...))` fail to compile
   against a `unique_ptr<Source<Out>>` parameter?

### Struggled with — revision list

- Not yet assessed. Stage 1 was built without the comprehension gate; these
  concepts have not been tested and should be treated as **unverified** during
  the revision pass.

---

## Cross-stage themes

### Recurring theme worth naming in an interview

Two of the three questions were about the same underlying idea: **a check that
can pass while the property it protects is violated is worse than no check.**
UBSan that recovers, an oracle mistaken for a race detector, a green CI badge
that means nothing. This is the throughline of the project's correctness
methodology and is worth stating explicitly when discussing it.
