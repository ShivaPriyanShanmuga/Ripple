# The demo application. Always built: a project whose demo needs a special build
# flag to appear is a project nobody runs.
add_executable(ripple_taxi_demo taxi_demo.cpp)
target_link_libraries(ripple_taxi_demo
    PRIVATE
        ripple::ripple
        ripple::warnings
        Threads::Threads
)
