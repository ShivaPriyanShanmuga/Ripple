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

## 4. Stage status

| Stage | Description | Status |
| ----- | ----------- | ------ |
| 0 | Scaffolding, sanitizers, CI | **Complete** |
| 1 | Core dataflow, type erasure | Not started |
| 2 | Event time and watermarks | Not started |
| 3 | Windowing | Not started |
| 4 | Keyed state, backend, serialization | Not started |
| 5 | Concurrency primitives | Not started |
| 6 | Parallelism, partitioning, backpressure | Not started |
| 7 | Checkpointing (Chandy-Lamport ABS) | Not started |
| 8 | Recovery and exactly-once | Not started |
| 9 | Benchmarks and demo application | Not started |
| 10 | README | Not started |
