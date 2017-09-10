cmake_minimum_required(VERSION 3.8)
project(mutilthread_poc)

set(CMAKE_CXX_STANDARD 17)

if(MSVC)
	# The last CMake I've tested doesn't know how to configure C++17 on Visual C++
	set (CMAKE_CXX_FLAGS "/std:c++latest ${CMAKE_CXX_FLAGS}")

endif()

set(SOURCE_FILES main.cpp)
add_executable(mutilthread_poc ${SOURCE_FILES})