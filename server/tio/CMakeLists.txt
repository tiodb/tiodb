cmake_minimum_required(VERSION 3.8)
project(tiodb)

set(CMAKE_CXX_STANDARD 17)
set(Boost_USE_STATIC_LIBS   ON)
set(BOOST_ROOT /Users/rodrigostrauss/Downloads/boost_1_64_0)

find_package(Boost 1.64 COMPONENTS filesystem regex program_options system date_time REQUIRED)


INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})


set(SOURCE_FILES
        ../../client/c/tioclient.c
        Command.cpp
        ContainerManager.cpp
        tiotcpclient.cpp
        TioTcpServer.cpp
        TioTcpSession.cpp
        tio.cpp
        )


add_executable(tiodb ${SOURCE_FILES})

TARGET_LINK_LIBRARIES(tiodb ${Boost_LIBRARIES})