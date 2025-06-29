#
# $Id$
#
# Created 2006/10/20
# Author: Sriram Rao (Kosmix Corp)
#
# Copyright 2008-2025 Quantcast Corporation. All rights reserved.
# Copyright 2006 Kosmix Corp.
#
# This file is part of Quantcast File System.
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

cmake_minimum_required(VERSION 2.8.4...10.0 FATAL_ERROR)
cmake_policy(VERSION
    ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION})

if(NOT DEFINED KFS_DIR_PREFIX)
    set(KFS_DIR_PREFIX ${CMAKE_SOURCE_DIR})
endif()

project(QFS)
set(CMAKE_MODULE_PATH ${KFS_DIR_PREFIX}/cmake/Modules)

if(NOT DEFINED Boost_NO_BOOST_CMAKE)
    if(${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} EQUAL 8 AND
        5 LESS ${CMAKE_PATCH_VERSION})
        # Turn off by default with 2.8.5 < cmake < 2.9.0 to
        # work around cmake28 and boost libraries problem on centos 6.
        # Boost_NO_BOOST_CMAKE was introduced int cmake 2.8.6
        set(Boost_NO_BOOST_CMAKE ON)
    endif()
endif()

if(NOT DEFINED QFS_USE_STATIC_LIB_LINKAGE)
    set(QFS_USE_STATIC_LIB_LINKAGE ON)
endif()

if(NOT DEFINED Boost_USE_STATIC_LIBS)
    set(Boost_USE_STATIC_LIBS OFF)
endif()

set(Boost_USE_MULTITHREADED ON)

# Require the packages we need to build
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin" OR
    CYGWIN OR
    CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    find_package(Boost COMPONENTS regex system REQUIRED)
else()
    find_package(Boost COMPONENTS regex REQUIRED)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux" OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    set(CMAKE_INSTALL_RPATH $ORIGIN)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(CMAKE_INSTALL_RPATH @loader_path)
endif()

if(CYGWIN)
    set(CMAKE_FIND_LIBRARY_PREFIXES ${CMAKE_FIND_LIBRARY_PREFIXES} "")
    set(CMAKE_FIND_LIBRARY_SUFFIXES
        ${CMAKE_FIND_LIBRARY_SUFFIXES} ".dll.a" ".lib")

    # For now only for cygwin to get strptime, and gnu specific glob flags.
    add_definitions(-D_GNU_SOURCE)
endif()

if(QFS_OMIT_JNI)
    message(STATUS "Not building QFS JNI")
else()
    set(JAVA_AWT_LIBRARY NotNeeded)
    set(JAVA_JVM_LIBRARY NotNeeded)
    set(JAVA_AWT_INCLUDE_PATH NotNeeded)
    find_package(JNI REQUIRED)
endif()

find_package(Jerasure REQUIRED)
find_package(Krb5 REQUIRED krb5)
find_package(OpenSSL REQUIRED)
find_package(FUSE)

add_definitions(-DBOOST_BIND_GLOBAL_PLACEHOLDERS)

# BOOST_SP_USE_QUICK_ALLOCATOR and BOOST_SP_USE_STD_ALLOCATOR are deprecated in
# Boost 1.87 and later.
if(Boost_VERSION_MAJOR LESS_EQUAL 1 AND Boost_VERSION_MINOR LESS 87)
    add_definitions(-DBOOST_SP_USE_QUICK_ALLOCATOR)
endif()

# Define various options based on the library configuration we use
if(KRB5_FLAVOR)
    add_definitions(-DKFS_KRB_USE_${KRB5_FLAVOR})
endif()

if(KRB5_USE_KRB5H)
    add_definitions(-DKFS_KRB_USE_KRB5H)
endif()

if("${KRB5_FLAVOR}" STREQUAL "HEIMDAL" OR
    KRB5_HAS_krb5_get_init_creds_opt_set_out_ccache)
    add_definitions(-DKFS_KRB_USE_KRB5_GET_INIT_CREDS_OPT)
endif()

if(KRB5_HAS_krb5_unparse_name_flags_ext)
    add_definitions(-DKRB5_HAS_krb5_unparse_name_flags_ext)
endif()

if(KRB5_HAS_krb5_unparse_name_ext)
    add_definitions(-DKRB5_HAS_krb5_unparse_name_ext)
endif()

if(KRB5_HAS_krb5_free_keytab_entry_contents)
    add_definitions(-DKRB5_HAS_krb5_free_keytab_entry_contents)
endif()

if(KRB5_HAS_krb5_kt_free_entry)
    add_definitions(-DKRB5_HAS_krb5_kt_free_entry)
endif()

# Warn about old versions
if(NOT OPENSSL_VERSION OR OPENSSL_VERSION MATCHES "^0[.]")
    message(STATUS "WARNING: openssl version ${OPENSSL_VERSION}")
    message(STATUS "WARNING: QFS requires openssl version 1.0 or greater")
    message(STATUS "WARNING: QFS authentication will not work properly")
endif()

# Change this to where the install directory is located
if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX "." CACHE PATH "install directory prefix" FORCE)
endif()

# Build with statically linked libraries; the value for this variable has to be
# defined here overwriting whatever is in the cache.
# When set to ON, we build with statically linked libraries; when off we
# link with dynamically linked libs
if(NOT DEFINED QFS_USE_STATIC_LIB_LINKAGE)
    set(QFS_USE_STATIC_LIB_LINKAGE TRUE)
endif()

if(QFS_USE_STATIC_LIB_LINKAGE)
    message(STATUS "Build binaries with statically linked QFS libraries")
else()
    message(STATUS "Build binaries with dynamically linked QFS libraries")
endif()

set(USE_STATIC_LIB_LINKAGE ${QFS_USE_STATIC_LIB_LINKAGE} CACHE BOOL
    "Build binaries with statically linked libraries" FORCE)

if(ENABLE_COVERAGE)
    message(STATUS "Enabling code coverage with gcov")
    set(CMAKE_CXX_FLAGS "-coverage")
    set(CMAKE_C_FLAGS "-coverage")
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++14")
endif()

if(ENABLE_PROFILING)
    message(STATUS "Enabling profiling with gprof")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pg")
    set(CMAKE_SHAREDBoost_USE_MULTITHREADED_LINKER_FLAGS
        "${CMAKE_SHARED_LINKER_FLAGS} -pg")
    set(CMAKE_EXE_FLAGS "${CMAKE_EXE_FLAGS} -pg")
endif()

# Change the line to Release to build release binaries
# For servers, build with debugging info; for tools, build Release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D NDEBUG -g3")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
string(TOUPPER KFS_OS_NAME_${CMAKE_SYSTEM_NAME} KFS_OS_NAME)
add_definitions(-D${KFS_OS_NAME})

if(ENABLE_IO_BUFFER_DEBUG)
    add_definitions(-DDEBUG_IOBuffer)
    message(STATUS "Enabled IO buffer debug")
endif()

if(DEFINED QFS_EXTRA_CXX_OPTIONS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${QFS_EXTRA_CXX_OPTIONS}")
    message(STATUS "Extra CXX options: ${QFS_EXTRA_CXX_OPTIONS}")
endif()

if(DEFINED QFS_EXTRA_C_OPTIONS)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${QFS_EXTRA_C_OPTIONS}")
    message(STATUS "Extra C options: ${QFS_EXTRA_C_OPTIONS}")
endif()

if(FUSE_FOUND)
    message(STATUS "Building qfs_fuse")
    message(STATUS "fuse libs:         ${FUSE_LIBRARIES}")
    message(STATUS "fuse include dirs: ${FUSE_INCLUDE_DIRS}")
    message(STATUS "fuse definitions:  ${FUSE_DEFINITIONS}")
    message(STATUS "fuse version: ${FUSE_MAJOR_VERSION}.${FUSE_MINOR_VERSION}")
    add_definitions(${FUSE_DEFINITIONS})

    if(${FUSE_MAJOR_VERSION} LESS 3)
        add_definitions(-D FUSE_USE_VERSION=26)
    else()
        add_definitions(-D FUSE_USE_VERSION=30)
    endif()

    include_directories(${FUSE_INCLUDE_DIRS})
else()
    message(STATUS "Not building qfs_fuse")
endif()

# include dirs
include_directories(
    ${Boost_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
    ${KRB5_INCLUDE_DIRS}
    ${KFS_DIR_PREFIX}/src/cc
    ${JAVA_INCLUDE_PATH}
    ${JAVA_INCLUDE_PATH2}
)

# Get the sub directories we want.
if(NOT QFS_OMIT_JNI)
    add_subdirectory(${KFS_DIR_PREFIX}/src/cc/access src/cc/access)
endif()

add_subdirectory(${KFS_DIR_PREFIX}/src/cc/chunk src/cc/chunk)
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/common src/cc/common)
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/devtools src/cc/devtools)
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/emulator src/cc/emulator)
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/kfsio src/cc/kfsio)
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/krb src/cc/krb)
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/libclient src/cc/libclient)
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/meta src/cc/meta)
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/qcdio src/cc/qcdio)
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/qcrs src/cc/qcrs)
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/qfsc src/cc/qfsc)
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/s3io src/cc/s3io)
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/tools src/cc/tools)
add_subdirectory(${KFS_DIR_PREFIX}/src/test-scripts src/test-scripts)

add_subdirectory(${KFS_DIR_PREFIX}/examples/cc examples/cc)
add_subdirectory(${KFS_DIR_PREFIX}/benchmarks/mstress benchmarks/mstress)
add_subdirectory(${KFS_DIR_PREFIX}/contrib/plugins contrib/plugins)

if(FUSE_FOUND)
    add_subdirectory(${KFS_DIR_PREFIX}/src/cc/fuse src/cc/fuse)
endif()

add_custom_target(
    rat ${KFS_DIR_PREFIX}/scripts/rat.sh ${KFS_DIR_PREFIX}
    COMMENT "Running license release audit tool (rat)"
    VERBATIM
)
