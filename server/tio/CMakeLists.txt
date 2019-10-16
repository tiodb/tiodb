cmake_minimum_required (VERSION 3.10)

include(CMakeDependentOption)

option(DOWNLOAD_BOOST "Download and compile the boost library." ON)

if(DOWNLOAD_BOOST)
	include(LibBoost)
endif()

if(MSVC)
	include(Pch)
endif()

set(CMAKE_CXX_STANDARD 11)
set(Boost_USE_STATIC_LIBS ON)

if(DOWNLOAD_BOOST)
	set(BOOST_ROOT ${CMAKE_SOURCE_DIR}/third_party/boost/boostlib)
else()
	option(BOOST_ROOT "Path of the boost library" "")
endif()

find_package(Boost REQUIRED COMPONENTS filesystem regex program_options system date_time thread)

# allow the source files to find headers in /src
include_directories(${PROJECT_SOURCE_DIR}/src/server/tio)

file(GLOB
	SOURCE_FILES
	*.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/*.h
	${CMAKE_SOURCE_DIR}/submodules/http-parser/http_parser.c
 	${CMAKE_SOURCE_DIR}/client/c/tioclient.c
)

set(PCH_EXCLUDE_FILES 
	${CMAKE_SOURCE_DIR}/submodules/http-parser/http_parser.c
	${CMAKE_SOURCE_DIR}/client/c/tioclient.c
)

add_executable(tio ${SOURCE_FILES})

if(MSVC)
	add_pch(tio pch.h pch.cpp SOURCE_FILES PCH_EXCLUDE_FILES)
endif()

target_link_libraries(tio
	PUBLIC
		Boost::filesystem
		Boost::regex
		Boost::program_options
		Boost::system
		Boost::date_time
		Boost::thread
		tioclient
)