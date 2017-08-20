cmake_minimum_required(VERSION 3.8)
project(tiodb)

set(CMAKE_CXX_STANDARD 17)

find_package(Boost 1.64 COMPONENTS filesystem unit_test_framework REQUIRED)

set(SOURCE_FILES tio.cpp)
add_executable(tiodb ${SOURCE_FILES})