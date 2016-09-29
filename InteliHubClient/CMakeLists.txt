cmake_minimum_required(VERSION 2.6)

PROJECT(intelihubclient)

SET (TIO_BUILD_DIR ../../../bin)

SET(LIBRARY_OUTPUT_PATH ${TIO_BUILD_DIR} CACHE PATH "Build directory" FORCE)

ADD_LIBRARY(intelihubclient tioclient.c)

