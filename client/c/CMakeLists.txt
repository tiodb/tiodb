cmake_minimum_required (VERSION 3.5.1)
project(tioclient C)

add_compile_definitions(WIN32 _WINDOWS _USRDLL tioclientdll_EXPORTS)

add_library(tioclient SHARED 
    tioclient.c
    tioclient.h
    tioclient_internals.h
    tioclientdll.def 
)