cmake_minimum_required(VERSION 3.26.3)
project(pthread_java C)

set(CMAKE_C_STANDARD_REQUIRED 17)

add_definitions(-D_GNU_SOURCE)

add_compile_options(-march=native -mtune=native -fomit-frame-pointer -O2 -pipe)

if (NOT DEFINED JAVA_HOME)
    message(FATAL_ERROR "JAVA_HOME is not set")
endif ()

if (NOT DEFINED JNI_HEADERS)
    message(FATAL_ERROR "JNI_HEADERS is not set")
endif ()

include_directories(${JNI_HEADERS})
include_directories("${JAVA_HOME}/include")
include_directories("${JAVA_HOME}/include/linux")

add_library(pthread_java SHARED library.c)

install(TARGETS pthread_java)
