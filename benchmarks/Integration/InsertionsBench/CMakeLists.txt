cmake_minimum_required(VERSION 3.10)

project(InsertionsBench)

# allow the source files to find headers
include_directories(${CMAKE_SOURCE_DIR}/client/c)
include_directories(${CMAKE_SOURCE_DIR}/client/cpp)

include_directories(${CMAKE_SOURCE_DIR}/submodules/benchmark/include)

set(SOURCE_FILES
    insertionsbench.cpp
    ${CMAKE_SOURCE_DIR}/client/c/tioclient.c
)

add_executable(InsertionsBench ${SOURCE_FILES})

target_link_libraries(InsertionsBench
    PUBLIC
        benchmark
)