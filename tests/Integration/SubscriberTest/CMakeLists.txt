cmake_minimum_required(VERSION 3.10)

project(SubscribeTest)

# allow the source files to find headers
include_directories(${CMAKE_SOURCE_DIR}/client/c)
include_directories(${CMAKE_SOURCE_DIR}/client/cpp)

include_directories(${CMAKE_SOURCE_DIR}/submodules/googletest/googlemock/include)

set(SOURCE_FILES
    subscribertest.cpp
    ${CMAKE_SOURCE_DIR}/client/c/tioclient.c
)

# force gtest dynamically
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

add_executable(SubscribeTest ${SOURCE_FILES})

target_link_libraries(SubscribeTest
    PUBLIC
        tioclient
        gmock
)   