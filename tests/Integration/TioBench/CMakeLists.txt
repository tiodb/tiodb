cmake_minimum_required(VERSION 3.10)

# TODO: Divide TioBench(Bechmark, Integration Test)

project(TioBench)

# allow the source files to find headers
include_directories(${CMAKE_SOURCE_DIR}/client/c)
include_directories(${CMAKE_SOURCE_DIR}/client/cpp)

set(SOURCE_FILES
    entrypoint.cpp
    ${CMAKE_SOURCE_DIR}/client/c/tioclient.c
)

# force gtest dynamically

add_executable(TioBench ${SOURCE_FILES})

target_link_libraries(TioBench
    PUBLIC
        tioclient
)   