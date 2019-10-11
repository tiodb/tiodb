message("VS version: ${MSVC_VERSION}")

if(MSVC_VERSION EQUAL 1600) #VS2010
    set(BOOST_TOOLSET "msvc-10.0")
elseif(MSVC_VERSION EQUAL 1700) #VS2012
    set(BOOST_TOOLSET "msvc-11.0")
elseif(MSVC_VERSION EQUAL 1800) #VS2013
    set(BOOST_TOOLSET "msvc-12.0")
elseif(MSVC_VERSION EQUAL 1900) #VS2015
    set(BOOST_TOOLSET "msvc-14.0")
elseif(MSVC_VERSION GREATER 1910 AND MSVC_VERSION LESS 1920) #VS2017
    set(BOOST_TOOLSET "msvc-14.1")
elseif(MSVC_VERSION GREATER 1919) #VS2019
    set(BOOST_TOOLSET "msvc-14.2")
endif()

message("Setting boost_toolset: ${BOOST_TOOLSET}")

# Download and unpack lib boost at configure time
configure_file(${PROJECT_SOURCE_DIR}/cmake/LibBoost.cmake.in ${PROJECT_SOURCE_DIR}/third_party/boost/CMakeLists.txt @ONLY)

execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -S . -B build
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/boost/
)

if(result)
        message(FATAL_ERROR "CMake step for libboost failed: ${result}")
endif()

execute_process(
        COMMAND ${CMAKE_COMMAND} --build build
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/boost/
)


