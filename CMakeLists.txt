cmake_minimum_required (VERSION 3.10)
project (tiodb CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY
    ${CMAKE_BINARY_DIR}/bin
)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY
    ${CMAKE_BINARY_DIR}/lib
)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY
    ${CMAKE_BINARY_DIR}/bin
)

add_subdirectory(client/c)
add_subdirectory(server/tio)

# tests
add_subdirectory(tests/Integration/SubscriberTest)
add_subdirectory(tests/Integration/TioBench)

# benchmarks
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
add_subdirectory(benchmarks/Integration/InsertionsBench)

# submodules
add_subdirectory(submodules/googletest)
add_subdirectory(submodules/benchmark)