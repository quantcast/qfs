#
# $Id$
#
# Created 2006/10/20
# Author: Sriram Rao (Kosmix Corp)
#
# Copyright 2008-2012 Quantcast Corp.
# Copyright 2006 Kosmix Corp.
#
# This file is part of Kosmos File System (KFS).
#
# Licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
#

set(CMAKE_LEGACY_CYGWIN_WIN32 0) # Remove when CMake >= 2.8.4 is required
cmake_minimum_required(VERSION 2.4.6)

project (KFS)

if (DEFINED KFS_DIR_PREFIX)
    message ("Qfs source dir prefix: ${KFS_DIR_PREFIX}")
    set(CMAKE_MODULE_PATH ${KFS_DIR_PREFIX}cmake)
else (DEFINED KFS_DIR_PREFIX)
    set(KFS_DIR_PREFIX "")
    set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
endif (DEFINED KFS_DIR_PREFIX)

# Locate Boost
# set(Boost_LIB_DIAGNOSTIC_DEFINITIONS "-DBOOST_LIB_DIAGNOSTIC")

if (NOT DEFINED Boost_USE_STATIC_LIBS)
    set(Boost_USE_STATIC_LIBS OFF)
endif (NOT DEFINED Boost_USE_STATIC_LIBS)

set(Boost_USE_MULTITHREADED ON)

IF (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    find_package(Boost COMPONENTS regex system REQUIRED)
ELSE (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    find_package(Boost COMPONENTS regex REQUIRED)
ENDIF (CMAKE_SYSTEM_NAME STREQUAL "Darwin")

message(STATUS "Boost-includes = ${Boost_INCLUDE_DIRS}")
message(STATUS "Boost-libs = ${Boost_LIBRARIES}")

# Locate the path to jni.h
find_package(JNI)

ENABLE_TESTING()

# Change this to where the install directory is located
if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX "." CACHE PATH "installation directory prefix" FORCE)
endif (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)

# Build with statically linked libraries; the value for this variable has to be defined here
# overwriting whatever is in the cache.
# When set to ON, we build with statically linked libraries; when off we
# link with dynamically linked libs

IF (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    IF (BUILD_CPU_MODE STREQUAL "32")
        message (STATUS "Building 32-bit mode on Solaris")
        # If we are asked to build 32 bit mode
        add_definitions (-D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -D_LARGE_FILES)
    ELSE (BUILD_CPU_MODE STREQUAL "32")
        # On solaris, use 64-bit mode
        set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -m64")
        set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} -m64")
        set(CMAKE_SHARED_LINKER_FLAGS  "${CMAKE_SHARED_LINKER_FLAGS} -m64")
    ENDIF (BUILD_CPU_MODE STREQUAL "32")
    # Statically linked binaries don't work on solaris
    set (USE_STATIC_LIB_LINKAGE OFF CACHE BOOL "Build binaries with statically linked libraries" FORCE)
    # Cmake does whacky relink on solaris and messes things up; avoid this
    set (CMAKE_SKIP_RPATH ON)
ELSE (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    set (USE_STATIC_LIB_LINKAGE ON CACHE BOOL "Build binaries with statically linked libraries" FORCE)
    IF (CMAKE_SIZEOF_VOID_P MATCHES "4" AND NOT CYGWIN)
        message (STATUS "Enabling largefile source flags")
        add_definitions (-D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_LARGE_FILES)
    ENDIF (CMAKE_SIZEOF_VOID_P MATCHES "4" AND NOT CYGWIN)
ENDIF (CMAKE_SYSTEM_NAME STREQUAL "SunOS")

IF (ENABLE_PROFILING)
    message (STATUS "Enabling profiling with gprof")
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -pg")
    set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} -pg")
    set(CMAKE_SHAREDBoost_USE_MULTITHREADED_LINKER_FLAGS  "${CMAKE_SHARED_LINKER_FLAGS} -pg")
    set(CMAKE_EXE_FLAGS  "${CMAKE_EXE_FLAGS} -pg")
ENDIF (ENABLE_PROFILING)

# Darwin compilers need to be told about ports
IF (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I/opt/local/include")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L/opt/local/lib")
ENDIF (CMAKE_SYSTEM_NAME STREQUAL "Darwin")


# Change the line to Release to build release binaries
# For servers, build with debugging info; for tools, build Release
#

IF (NOT CMAKE_BUILD_TYPE)
    message (STATUS "Setting build type to Debug")
    set (CMAKE_BUILD_TYPE "Debug")
ENDIF (NOT CMAKE_BUILD_TYPE)

IF (CMAKE_BUILD_TYPE STREQUAL "Release")
    message(STATUS "Enabling -D NDEBUG flag")
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -D NDEBUG -g3")
ENDIF(CMAKE_BUILD_TYPE STREQUAL "Release")

set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -DBOOST_SP_USE_QUICK_ALLOCATOR")
string(TOUPPER KFS_OS_NAME_${CMAKE_SYSTEM_NAME} KFS_OS_NAME)
add_definitions (-D${KFS_OS_NAME})

#
# Find the path to libfuse.so
#

SET(Fuse_LIBRARY_DIR "")
IF (EXISTS "/lib64/libfuse.so")
    SET(Fuse_LIBRARY_DIR "/lib64")
ELSEIF (EXISTS "/opt/local/lib/libfuse.dylib")
    SET(Fuse_LIBRARY_DIR "/opt/local/lib")
ELSEIF (EXISTS "/usr/local/lib/libfuse.dylib" OR EXISTS "/usr/local/lib/libfuse_ino64.dylib")
    SET(Fuse_LIBRARY_DIR "/usr/local/lib")
    SET(Fuse_INCLUDE_DIR "/usr/local/include/osxfuse")
ELSEIF (EXISTS "/usr/lib/libfuse.a" OR EXISTS "/usr/lib/libfuse.so")
    SET(Fuse_LIBRARY_DIR "/usr/local/lib")
ELSEIF (EXISTS "/lib/libfuse.a" OR EXISTS "/lib/libfuse.so")
    SET(Fuse_LIBRARY_DIR "/lib")
ENDIF (EXISTS "/lib64/libfuse.so")

if(COMMAND cmake_policy)
    cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

# include dirs
include_directories( ${Boost_INCLUDE_DIRS} ${KFS_DIR_PREFIX}src/cc)

# get the subdirs we want
add_subdirectory (${KFS_DIR_PREFIX}src/cc/common src/cc/common)
add_subdirectory (${KFS_DIR_PREFIX}src/cc/meta src/cc/meta)
add_subdirectory (${KFS_DIR_PREFIX}src/cc/chunk src/cc/chunk)
add_subdirectory (${KFS_DIR_PREFIX}src/cc/libclient src/cc/libclient)
add_subdirectory (${KFS_DIR_PREFIX}src/cc/kfsio src/cc/kfsio)
add_subdirectory (${KFS_DIR_PREFIX}src/cc/tools src/cc/tools)
add_subdirectory (${KFS_DIR_PREFIX}src/cc/devtools src/cc/devtools)
add_subdirectory (${KFS_DIR_PREFIX}src/cc/tests src/cc/tests)
add_subdirectory (${KFS_DIR_PREFIX}src/cc/emulator src/cc/emulator)
add_subdirectory (${KFS_DIR_PREFIX}src/test-scripts src/test-scripts)
add_subdirectory (${KFS_DIR_PREFIX}src/cc/qcdio src/cc/qcdio)
add_subdirectory (${KFS_DIR_PREFIX}src/cc/qcrs src/cc/qcrs)

add_subdirectory (${KFS_DIR_PREFIX}examples/cc examples/cc)
add_subdirectory (${KFS_DIR_PREFIX}benchmarks/mstress benchmarks/mstress)

IF (NOT ${JAVA_INCLUDE_PATH} STREQUAL "" AND NOT DEFINED JNI_FOUND)
    set(JNI_FOUND TRUE)
ENDIF  (NOT ${JAVA_INCLUDE_PATH} STREQUAL "" AND NOT DEFINED JNI_FOUND)

IF (JNI_FOUND)
    message(STATUS "JNI found: building qfs_access")
    include_directories ( ${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2} )
    add_subdirectory (${KFS_DIR_PREFIX}src/cc/access src/cc/access)
ELSE (JNI_FOUND)
    message(STATUS "JNI NOT found: NOT building qfs_access")
ENDIF (JNI_FOUND)

IF (NOT ${Fuse_LIBRARY_DIR} STREQUAL "")
    message(STATUS "Found fuse")
    include_directories ( ${Fuse_INCLUDE_DIR} )
    add_subdirectory (${KFS_DIR_PREFIX}src/cc/fuse src/cc/fuse)
ENDIF (NOT ${Fuse_LIBRARY_DIR} STREQUAL "")
