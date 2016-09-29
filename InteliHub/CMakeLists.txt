cmake_minimum_required(VERSION 2.6)

PROJECT(intelihub)

SET(TIO_PYTHON_PLUGIN_SUPPORT 0)

ADD_DEFINITIONS(-DTIO_PYTHON_PLUGIN_SUPPORT=${TIO_PYTHON_PLUGIN_SUPPORT})

SET (TIO_BUILD_DIR ../../../bin)

SET(EXECUTABLE_OUTPUT_PATH ${TIO_BUILD_DIR} CACHE PATH "Build directory" FORCE)
SET(LIBRARY_OUTPUT_PATH ${TIO_BUILD_DIR} CACHE PATH "Build directory" FORCE)

IF(${WIN32})
	#
	# I've tried to keep it compatible with Boost 1.44, but it crashes when calling python dll/so.
	# Probably because on 1.44 Boost Python was tested with Python 2.2.
	#
	SET(BOOST_VERSION 1_47)
	SET(BOOST_ROOT ../../../lib/boost/boost_${BOOST_VERSION})
        SET(BOOST_INCLUDEDIR ${BOOST_ROOT})
	SET(BOOST_LIBDIR ${BOOST_ROOT}/lib)

	IF(${TIO_PYTHON_PLUGIN_SUPPORT})
		SET(PYTHON_ROOT ../../../lib/Python-2.7.2)
		SET(PYTHON_INCLUDEDIR
				${PYTHON_ROOT}/Include
				${PYTHON_ROOT}/PC)
   
		SET(PYTHON_LIBDIR ${PYTHON_ROOT}/PCbuild)
	ENDIF(${TIO_PYTHON_PLUGIN_SUPPORT})
		
	#
	# Link with the static version of Visual C++ runtime, so we'll
	# have a standalone executable.
	#
	IF(NOT ${TIO_PYTHON_PLUGIN_SUPPORT})
		FOREACH(flag_var CMAKE_CXX_FLAGS_RELEASE CMAKE_CXX_FLAGS_RELWITHDEBINFO)
			IF(${flag_var} MATCHES "/MD")
				STRING(REGEX REPLACE "/MD" "/MT" ${flag_var} "${${flag_var}}")
			ENDIF(${flag_var} MATCHES "/MD")
		ENDFOREACH(flag_var)
	ENDIF(NOT ${TIO_PYTHON_PLUGIN_SUPPORT})

ELSE(${WIN32})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
    LINK_LIBRARIES (intelihub debug boost_system optimized boost_system)
    LINK_LIBRARIES (intelihub debug boost_program_options optimized boost_program_options)
    LINK_LIBRARIES (intelihub debug boost_filesystem optimized boost_filesystem)
    LINK_LIBRARIES (intelihub pthread)
    #LINK_LIBRARIES (intelihub tcmalloc) # google's tcmalloc
    #LINK_LIBRARIES (intelihub profiler) # google's cpu profiler

    IF(${TIO_PYTHON_PLUGIN_SUPPORT})
        SET(PYTHON_INCLUDEDIR /usr/include/python2.6)
        LINK_LIBRARIES (intelihub debug boost_python optimized boost_python)
        LINK_LIBRARIES (intelihub python2.6)
    ENDIF(${TIO_PYTHON_PLUGIN_SUPPORT})
	
    SET(BOOST_INCLUDEDIR /usr/local/include)
    SET(BOOST_LIBDIR /usr/local/lib)

ENDIF(${WIN32})

INCLUDE_DIRECTORIES(${BOOST_INCLUDEDIR} ${PYTHON_INCLUDEDIR})
LINK_DIRECTORIES(${BOOST_LIBDIR} ${PYTHON_LIBDIR})

ADD_EXECUTABLE(intelihub
	pch.cpp
    Command.cpp
    ContainerManager.cpp
    TioTcpServer.cpp
    TioTcpSession.cpp
    TioPython.cpp
    tio.cpp
    ../InteliHubClient/tioclient.c)
	
# I can't get it to work, help is appreciated. Since tioclient.c is C and not C++, it can't
# use the pch. /TP was supposed to force it to be compiled as C++, what is OK. But it doesn't work
#	
# precompiled header
#	IF (MSVC)
#		SET_TARGET_PROPERTIES(intelihub PROPERTIES COMPILE_FLAGS "/Yupch.h")
#		SET_SOURCE_FILES_PROPERTIES(pch.cpp PROPERTIES COMPILE_FLAGS "/Ycpch.h")
#		SET_SOURCE_FILES_PROPERTIES(tioclient.c PROPERTIES COMPILE_FLAGS "/TP")
#	endif(MSVC)
