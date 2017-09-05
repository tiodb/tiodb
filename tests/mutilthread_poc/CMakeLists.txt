cmake_minimum_required(VERSION 3.8)
project(mutilthread_poc)

set(CMAKE_CXX_STANDARD 17)

set(SOURCE_FILES main.cpp)
add_executable(mutilthread_poc ${SOURCE_FILES})