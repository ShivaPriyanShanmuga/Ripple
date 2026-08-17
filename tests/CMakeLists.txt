include(GoogleTest)

add_executable(scaffolding_test scaffolding_test.cpp)
target_link_libraries(scaffolding_test
    PRIVATE
        GTest::gtest_main
        ripple::warnings
        Threads::Threads
)

# gtest_discover_tests runs the binary at build time and registers each TEST()
# with CTest individually, rather than as one opaque "run the whole binary"
# entry. That means a failure names the specific test, and CTest can run them
# in parallel.
gtest_discover_tests(scaffolding_test)
