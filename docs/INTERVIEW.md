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

## Stage 2 — Event time and watermarks

### Concepts covered

- **What a watermark asserts, and what it does not.** "No record with event time
  <= T will arrive on this channel from now on" is a *heuristic produced by a
  chosen strategy*, not a guarantee. Records older than T still arrive; they are
  late, and Stage 3 decides their fate.
- **Bounded out-of-orderness, correctly understood.** The bound measures how
  jumbled the *arrival stream* is — how far behind the newest event time an
  arriving record's event time may be. It is not a claim that any timestamp is
  wrong or late. An event time is stamped when the thing happened and is never
  wrong.
- **The latency-vs-completeness dial.** A large bound gives complete results
  late; a small bound gives fast results with more records missing their window.
  There is no correct value — it is a business decision.
- **Why watermarks travel in-band.** They cannot overtake the records ahead of
  them, so watermark T arriving at an operator proves every record <= T has
  already passed through that operator. No global clock. A side-channel would
  destroy the property.
- **Minimum across input channels.** You are only as caught up as your slowest
  input. Maximum would be catastrophic, not merely wrong.
- **The idle-channel stall.** A silent input freezes the minimum, so windows stop
  firing and output stops while the job looks healthy. Expected behaviour of the
  rule, not a bug.
- **Ordering: record before watermark.** Emitting the watermark first makes a
  record late against a watermark its own arrival produced.
- **Monotonicity.** A regressing watermark re-opens a window that already fired,
  producing a contradictory duplicate result.
- **End-of-stream watermark.** Finite input must emit a maximal watermark or the
  final windows silently vanish.
- **Interface design: separate virtual vs. variant.** Why `on_watermark` with a
  default forwarding body beats a variant every operator must dispatch on — and
  why the variant is still correct at the Stage 6 transport layer.

### Questions to be able to answer cold

1. What exactly does a watermark guarantee? (Trick question — state precisely
   what it does and does not promise.)
2. Why must an operator take the minimum of its input watermarks rather than the
   maximum? Describe the concrete data loss if you took the maximum.
3. Your job's output stopped, but records are still flowing and there are no
   errors. What is your first hypothesis and why?
4. What breaks if the watermark is emitted before the record that produced it?
   Why is that bug hard to notice?
5. Why does map need no watermark-handling code at all, and what would the
   variant-based design have cost here?
6. A window operator overrides `on_watermark` to fire its windows and forgets to
   forward the watermark. What happens downstream?
7. Why does a finite job need an end-of-stream watermark, and what does the
   output look like without one?
8. Bounded out-of-orderness is set to 30 seconds. A record arrives whose event
   time is 45 seconds behind the maximum seen. What happens, and whose fault is
   it?

### Struggled with — revision list

- **Bounded out-of-orderness terminology** was a genuine point of confusion
  during the Stage 0 debrief: the question was raised of why event time would be
  "30 seconds late" when event time is stamped at occurrence and never changes.
  The resolution — the bound describes disorder in *arrival order*, not error in
  any timestamp — is worth re-deriving rather than memorising.
- **The idle-source stall** was spotted independently ("what if there is no event
  at that moment?"), which is the right instinct. Make sure the *fix* is also
  known: idleness detection marking a channel as not participating in the
  minimum, or advancing the watermark from the wall clock during quiet periods.
- Everything else in this stage is **unverified** — built without the
  comprehension gate.

---

## Stage 3 — Windowing

### Concepts covered

- **What "the window fires" means mechanically.** Records fold into
  accumulators; a watermark proves a window complete; the accumulator becomes a
  result; the state is deleted. **Firing is how a windowed job reclaims memory** —
  which is why a stalled watermark does not merely stop output, it grows the
  process until it dies.
- **Half-open intervals.** Why `[start, end)` and not inclusive ends.
- **One record in many windows.** Sliding windows place each record in
  `ceil(size/slide)` accumulators simultaneously. That multiple is the memory
  cost — a one-hour window sliding every second holds every record 3,600 times.
- **Why only sessions merge.** Tumbling/sliding boundaries are fixed by
  arithmetic; session boundaries are determined by the data, so a bridging record
  proves two sessions were never two. Merging forces the aggregator to be
  associative.
- **The adjacency/overlap boundary.** A gap of exactly the session gap must NOT
  merge; that is the definition of the session ending.
- **Incremental aggregation vs. buffering.** One accumulator per window rather
  than every record.
- **Allowed lateness and the re-fire.** The same window emitted twice, and the
  upsert requirement it imposes on sinks — the same idempotence Stage 8 needs.
- **Side output for late data.** Why counting and routing beats silent dropping.
- **Floor vs. truncating division** in window assignment, and why pre-epoch
  timestamps expose it.
- **When to erase a type and when not to.** Virtual for operators (runtime
  topology, traversal); templates for assigners and aggregators (neither
  applies). The rule is not "virtual is fine", it is "erase only where runtime
  variation is required".

### Questions to be able to answer cold

1. Walk through what happens, step by step, from a record arriving to its
   window's result being emitted and its memory freed.
2. Why must windows be half-open? Give the concrete failure with inclusive ends.
3. A one-hour sliding window with a one-second slide. How many windows does one
   record land in, and what does that do to memory?
4. Why do session windows need merging logic when tumbling and sliding do not?
5. Why does a merging assigner force the aggregator to be associative?
6. Two sessions with a 30s gap setting, separated by exactly 30s. Merge or not?
   Why is that the right answer?
7. With allowed lateness configured, a downstream sink receives the same window
   twice with different values. Is that a bug? What must the sink do?
8. A window operator fires its windows on watermark and forgets to forward the
   watermark. Describe the symptom an operator would see in production.
9. Why is a window's result stamped at `end - 1ms` rather than `end`?
10. Your windowed job's memory grows without bound but output looks correct so
    far. What is your first hypothesis?

### Struggled with — revision list

- **Unverified** — built without the comprehension gate. Questions 1, 4, 5, and 8
  are the ones most likely to come up in an interview and least likely to be
  answerable from having merely read the code.

---

## Stage 4 — Keyed state, state backend, serialization

### Concepts covered

- **Why C++ forces serialization to be designed by hand.** No reflection means
  no way to ask a type its fields. This is a design task here and a library call
  in Java.
- **Three serialization designs and their trade-offs**: trait specialisation
  (chosen), self-describing tagged formats, macro-generated visitors. See D-034.
- **The defining hand-rolled-serialization bug**: write and read disagreeing on
  field count, and why requiring full consumption converts it from silent
  corruption into an exception.
- **Wire-format stability**: fixed little-endian, one-byte bool, and why a
  round-trip test cannot catch an endianness bug on a little-endian machine.
- **Keys as bytes.** Why the backend erases the key type, and why that makes
  serialization a prerequisite for keyed state rather than a companion to
  checkpointing.
- **Determinism as a checkpointing requirement.** Ordered maps over hash maps so
  identical state produces identical bytes.
- **keyed vs. operator state.** Keyed state is partitioned and lives in the
  backend; operator state (source offsets, window contents) lives in the operator
  and is captured by `snapshot_state`.
- **Partitioning instead of locking.** Why keyed state needs no mutex at all, and
  why a shared map behind a lock does not merely run slower but fails to scale.
- **Designing for a future requirement without building it** — `snapshot_state`,
  `restore_state`, `write_snapshot`, `restore_snapshot` all exist and are tested;
  no barriers, no coordinator, no checkpointing.
- **Batching vs. write-through persistence**, and why an LSM tree is what a
  production backend uses.

### Questions to be able to answer cold

1. Why can't C++ generate serialization code the way Java can, and what does
   that force on the design?
2. You write three fields and read two. What happens, when do you find out, and
   what one design decision makes you find out immediately?
3. Why does the state backend store keys as bytes rather than being templated on
   the key type?
4. Why ordered maps rather than hash maps in the backend? What breaks with
   `unordered_map`?
5. Keyed state has no mutex anywhere. Why is that safe, and why is the
   alternative (one map behind a lock) not just slower but unscalable?
6. What is the difference between keyed state and operator state? Give an
   example of each and say where each is stored.
7. Why is `KeyedOperator::process` marked `final`? What bug does that prevent,
   and why would no type system catch it?
8. Your file-backed state backend writes through on every state update. What
   goes wrong at 100k records/sec, and what should it do instead?
9. A `ValueState<int64_t>` returns `optional`. Why not just return 0 when the key
   has never been written?
10. Why does serialization have to be designed in Stage 4 rather than Stage 7,
    where it is actually used?

### Struggled with — revision list

- **Unverified** — built without the comprehension gate. Questions 2, 5, 6 and 7
  are the highest-value ones: 5 and 6 are standard interview material, and 2 and
  7 are the kind of design-reasoning question that distinguishes having built
  something from having read it.

---

## Cross-stage themes

### Recurring theme worth naming in an interview

Two of the three questions were about the same underlying idea: **a check that
can pass while the property it protects is violated is worse than no check.**
UBSan that recovers, an oracle mistaken for a race detector, a green CI badge
that means nothing. This is the throughline of the project's correctness
methodology and is worth stating explicitly when discussing it.
