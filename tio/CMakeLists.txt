cmake_minimum_required(VERSION 2.6)

PROJECT(tio)

#INCLUDE_DIRECTORIES(/usr/include/boost_1_34_1/)
#LINK_DIRECTORIES(/usr/local/lib)
#LINK_DIRECTORIES(/usr/include/boost_1_34_1/stage/lib)

INCLUDE_DIRECTORIES(/usr/include/python2.6)

LINK_LIBRARIES (tio debug boost_system optimized boost_system)
LINK_LIBRARIES (tio debug boost_program_options optimized boost_program_options)
LINK_LIBRARIES (tio debug boost_python optimized boost_python)
LINK_LIBRARIES (tio pthread)
LINK_LIBRARIES (tio python2.6)

#LINK_LIBRARIES (tio db_cxx-4.6)

#LINK_LIBRARIES (tio tcmalloc) # google's tcmalloc
#LINK_LIBRARIES (tio profiler) # google's cpu profiler

ADD_EXECUTABLE(tio Command.cpp ContainerManager.cpp TioTcpServer.cpp TioTcpSession.cpp TioPython.cpp pch.cpp tio.cpp)

