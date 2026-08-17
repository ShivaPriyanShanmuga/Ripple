include(GoogleTest)

# Every test target gets the same dependencies. A function keeps that in one
# place, so adding a test in a later stage is a single line.
function(ripple_add_test name)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name}
        PRIVATE
            ripple::ripple
            ripple::warnings
            GTest::gtest_main
            Threads::Threads
    )
    gtest_discover_tests(${name} PROPERTIES TIMEOUT 60)
endfunction()

ripple_add_test(scaffolding_test)
ripple_add_test(record_test)
ripple_add_test(operators_test)
ripple_add_test(pipeline_test)
ripple_add_test(watermark_test)
ripple_add_test(window_test)
ripple_add_test(serialization_test)
ripple_add_test(state_test)
ripple_add_test(bounded_queue_test)
ripple_add_test(worker_group_test)
ripple_add_test(parallel_test)
ripple_add_test(checkpoint_test)
ripple_add_test(recovery_test)
