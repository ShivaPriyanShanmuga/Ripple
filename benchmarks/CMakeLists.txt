# Component-level microbenchmarks. Google Benchmark handles the repetition and
# statistics, which is what it is good at.
add_executable(ripple_micro_bench micro_bench.cpp)
target_link_libraries(ripple_micro_bench
    PRIVATE
        ripple::ripple
        ripple::warnings
        benchmark::benchmark
        Threads::Threads
)

# End-to-end harness. Deliberately not a Google Benchmark target: it runs a
# multi-threaded pipeline once and reports a distribution plus a few one-shot
# durations, which is a different shape from "repeat until the timing is stable".
add_executable(ripple_e2e_bench end_to_end_bench.cpp)
target_link_libraries(ripple_e2e_bench
    PRIVATE
        ripple::ripple
        ripple::warnings
        Threads::Threads
)
