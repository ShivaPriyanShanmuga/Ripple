cmake_minimum_required(VERSION 3.20)

project(ripple
    VERSION 0.1.0
    DESCRIPTION "A mini stream-processing engine"
    LANGUAGES CXX
)

# ---------------------------------------------------------------------------
# Language standard
#
# Set at directory scope rather than per-target because it applies to
# absolutely everything we compile, including FetchContent dependencies.
# CXX_EXTENSIONS OFF gives us -std=c++20 rather than -std=gnu++20: we want to
# find out at compile time if we have accidentally leaned on a GNU extension.
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Emit compile_commands.json for clang-tidy, clangd, and editor tooling.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Put binaries somewhere predictable so sanitizer stack traces and manual runs
# do not require hunting through the build tree.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

option(RIPPLE_BUILD_TESTS      "Build the test suite"                     ON)
option(RIPPLE_BUILD_BENCHMARKS "Build benchmarks (Stage 9; downloads Google Benchmark)" OFF)
option(RIPPLE_WERROR           "Treat compiler warnings as errors"        OFF)
option(RIPPLE_ENABLE_CLANG_TIDY "Run clang-tidy during compilation"       OFF)

find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# Warning set, carried as an INTERFACE target
#
# An INTERFACE library compiles nothing. It is a bag of flags that propagates
# to anything linking it. This is the modern-CMake alternative to setting
# global CMAKE_CXX_FLAGS: global flags would also hit FetchContent
# dependencies, and we would drown in warnings from other people's code.
# ---------------------------------------------------------------------------
add_library(ripple_warnings INTERFACE)
add_library(ripple::warnings ALIAS ripple_warnings)

target_compile_options(ripple_warnings INTERFACE
    -Wall
    -Wextra
    -Wpedantic
    -Wshadow               # a local silently hiding a member is a real bug source
    -Wnon-virtual-dtor     # matters: Stage 1 gives us polymorphic operator bases
    -Woverloaded-virtual   # catches an override that actually overloads instead
    -Wold-style-cast       # forces static_cast/reinterpret_cast, which are greppable
    -Wconversion
    -Wsign-conversion      # signed/unsigned mixups in index arithmetic
    -Wnull-dereference
    -Wdouble-promotion
    -Wformat=2
    -Wimplicit-fallthrough
)

if(RIPPLE_WERROR)
    target_compile_options(ripple_warnings INTERFACE -Werror)
endif()

# ---------------------------------------------------------------------------
# Dependencies
#
# FetchContent builds dependencies from source using OUR flags. That is not a
# convenience here, it is a correctness requirement: ThreadSanitizer needs
# every translation unit in the process instrumented. Linking a system
# GoogleTest built without -fsanitize=thread produces false positives inside
# gtest and can hide real races that cross the boundary.
# ---------------------------------------------------------------------------
include(FetchContent)

if(RIPPLE_BUILD_TESTS)
    FetchContent_Declare(googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.15.2      # pinned: a moving tag makes CI failures unreproducible
        GIT_SHALLOW    TRUE
        SYSTEM                      # treat its headers as system headers -> no warnings from them
        EXCLUDE_FROM_ALL
    )
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
endif()

if(RIPPLE_BUILD_BENCHMARKS)
    # Declared inside the guard on purpose: with the option OFF, nothing is
    # downloaded and nothing is built. Turned on in Stage 9.
    FetchContent_Declare(benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG        v1.9.0
        GIT_SHALLOW    TRUE
        SYSTEM
        EXCLUDE_FROM_ALL
    )
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(benchmark)
endif()

# ---------------------------------------------------------------------------
# clang-tidy
#
# Deliberately configured AFTER FetchContent_MakeAvailable. CMAKE_CXX_CLANG_TIDY
# is read when a target is created, so setting it here means our targets are
# linted and GoogleTest's are not.
# ---------------------------------------------------------------------------
if(RIPPLE_ENABLE_CLANG_TIDY)
    find_program(RIPPLE_CLANG_TIDY_EXE NAMES clang-tidy REQUIRED)
    set(CMAKE_CXX_CLANG_TIDY "${RIPPLE_CLANG_TIDY_EXE}")
    message(STATUS "clang-tidy enabled: ${RIPPLE_CLANG_TIDY_EXE}")
endif()

# ---------------------------------------------------------------------------
# The engine
#
# One static library rather than one target per subsystem. Splitting later is
# mechanical; merging later is not, and per-stage targets would invent
# dependency-direction problems before there are dependencies.
#
# Most of the engine is templates and therefore headers. `src/` holds the
# non-template definitions, which is what makes this a compiled library rather
# than a header-only one -- worth having for the link-time checking and the
# incremental rebuild behaviour.
# ---------------------------------------------------------------------------
add_library(ripple STATIC
    src/pipeline.cpp
)
add_library(ripple::ripple ALIAS ripple)

target_include_directories(ripple PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_compile_features(ripple PUBLIC cxx_std_20)

# PRIVATE: our warning set governs how *we* compile, and must not be forced on
# anything that merely consumes the library.
target_link_libraries(ripple PRIVATE ripple::warnings)

if(RIPPLE_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
