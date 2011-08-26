cmake_minimum_required(VERSION 2.6)

PROJECT(tio)

SET(TIO_PYTHON_PLUGIN_SUPPORT 0)

ADD_DEFINITIONS(-DTIO_PYTHON_PLUGIN_SUPPORT=${TIO_PYTHON_PLUGIN_SUPPORT})

IF(${WIN32})
    SET(BOOST_INCLUDEDIR x:/boost/boost_1_47)
	SET(BOOST_LIBDIR x:/boost/boost_1_47/lib)

	IF(${TIO_PYTHON_PLUGIN_SUPPORT})
		SET(PYTHON_INCLUDEDIR
				D:/temp/Python-2.7/Include
				D:/temp/Python-2.7/PC)
   
		SET(PYTHON_LIBDIR D:/temp/Python-2.7/PCbuild)
	ENDIF(${TIO_PYTHON_PLUGIN_SUPPORT})
		
	#
	# Link with the static version of Visual C++ runtime, so we'll
	# have a standalone executable.
	#
	FOREACH(flag_var CMAKE_CXX_FLAGS_RELEASE CMAKE_CXX_FLAGS_RELWITHDEBINFO)
		IF(${flag_var} MATCHES "/MD")
			STRING(REGEX REPLACE "/MD" "/MT" ${flag_var} "${${flag_var}}")
		ENDIF(${flag_var} MATCHES "/MD")
	ENDFOREACH(flag_var)

ELSE(${WIN32})
    LINK_LIBRARIES (tio debug boost_system optimized boost_system)
    LINK_LIBRARIES (tio debug boost_program_options optimized boost_program_options)
    LINK_LIBRARIES (tio debug boost_filesystem optimized boost_filesystem)
    LINK_LIBRARIES (tio pthread)
    #LINK_LIBRARIES (tio tcmalloc) # google's tcmalloc
    #LINK_LIBRARIES (tio profiler) # google's cpu profiler

    IF(${TIO_PYTHON_PLUGIN_SUPPORT})
        SET(PYTHON_INCLUDEDIR /usr/include/python2.6)
        LINK_LIBRARIES (tio debug boost_python optimized boost_python)
        LINK_LIBRARIES (tio python2.6)
    ENDIF(${TIO_PYTHON_PLUGIN_SUPPORT})

ENDIF(${WIN32})

INCLUDE_DIRECTORIES(${BOOST_INCLUDEDIR} ${PYTHON_INCLUDEDIR})
LINK_DIRECTORIES(${BOOST_LIBDIR} ${PYTHON_LIBDIR})

ADD_EXECUTABLE(tio
	  pch.cpp
    Command.cpp
    ContainerManager.cpp
    TioTcpServer.cpp
    TioTcpSession.cpp
    TioPython.cpp
    tio.cpp
    ../tioclient/tioclient.c)
	
# I can't get it to work, help is appreciated. Since tioclient.c is C and not C++, it can't
# use the pch. /TP was supposed to force it to be compiled as C++, what is OK. But it doesn't work
#	
# precompiled header
#	IF (MSVC)
#		SET_TARGET_PROPERTIES(tio PROPERTIES COMPILE_FLAGS "/Yupch.h")
#		SET_SOURCE_FILES_PROPERTIES(pch.cpp PROPERTIES COMPILE_FLAGS "/Ycpch.h")
#		SET_SOURCE_FILES_PROPERTIES(tioclient.c PROPERTIES COMPILE_FLAGS "/TP")
#	endif(MSVC)
