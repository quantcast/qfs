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
cmake_minimum_required(VERSION 2.4.6 FATAL_ERROR)

IF (COMMAND cmake_policy)
    IF (DEFINED QFS_DONT_SET_CMAMKE_POLICY_VERISON)
        message(
            STATUS
            "Not setting version policy: ${QFS_DONT_SET_CMAMKE_POLICY_VERISON}"
        )
    ELSE (DEFINED QFS_DONT_SET_CMAMKE_POLICY_VERISON)
        cmake_policy(
            VERSION
            ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}
        )
    ENDIF (DEFINED QFS_DONT_SET_CMAMKE_POLICY_VERISON)
ENDIF (COMMAND cmake_policy)

project (QFS)

IF (DEFINED KFS_DIR_PREFIX)
    message (STATUS "Qfs source dir prefix: ${KFS_DIR_PREFIX}")
    set(CMAKE_MODULE_PATH ${KFS_DIR_PREFIX}cmake/Modules)
ELSE (DEFINED KFS_DIR_PREFIX)
    set(KFS_DIR_PREFIX "")
    set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/Modules)
ENDIF (DEFINED KFS_DIR_PREFIX)

IF (NOT DEFINED Boost_NO_BOOST_CMAKE)
    IF (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} EQUAL 8 AND
            5 LESS ${CMAKE_PATCH_VERSION})
        # Turn off by default with 2.8.5 < cmake < 2.9.0 to
        # work around cmake28 and boost libraries problem on centos 6.
        # Boost_NO_BOOST_CMAKE was introduced int cmake 2.8.6
        set(Boost_NO_BOOST_CMAKE ON)
    ENDIF (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} EQUAL 8 AND
        5 LESS ${CMAKE_PATCH_VERSION})
ENDIF (NOT DEFINED Boost_NO_BOOST_CMAKE)

# Locate Boost
# set(Boost_LIB_DIAGNOSTIC_DEFINITIONS "-DBOOST_LIB_DIAGNOSTIC")

IF (NOT DEFINED Boost_USE_STATIC_LIBS)
    set(Boost_USE_STATIC_LIBS OFF)
ENDIF (NOT DEFINED Boost_USE_STATIC_LIBS)

set(Boost_USE_MULTITHREADED ON)

IF (CMAKE_SYSTEM_NAME STREQUAL "Darwin" OR CYGWIN)
    find_package(Boost COMPONENTS regex system REQUIRED)
ELSE (CMAKE_SYSTEM_NAME STREQUAL "Darwin" OR CYGWIN)
    find_package(Boost COMPONENTS regex REQUIRED)
ENDIF (CMAKE_SYSTEM_NAME STREQUAL "Darwin" OR CYGWIN)

message(STATUS "Boost includes: ${Boost_INCLUDE_DIRS}")
message(STATUS "Boost libs:     ${Boost_LIBRARIES}")

IF (${CMAKE_MAJOR_VERSION} LESS 2)
ELSEIF (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} LESS 8)
ELSE (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} LESS 8)
    set (HAS_EXTERNAL_PROJECT ON)
ENDIF (${CMAKE_MAJOR_VERSION} LESS 2)

IF (HAS_EXTERNAL_PROJECT AND NOT QFS_OMIT_JERASURE)
    # gf-complete autoconf overrides CFGLAGS. Use CPPFLAGS to add compiler flags.
    include(ExternalProject)
    IF (KFS_DIR_PREFIX STREQUAL "")
        set (KFS_EXTERNAL_PROJECT_DIR ${CMAKE_SOURCE_DIR}/ext/)
    ELSE (KFS_DIR_PREFIX STREQUAL "")
        set (KFS_EXTERNAL_PROJECT_DIR ${KFS_DIR_PREFIX}ext/)
    ENDIF (KFS_DIR_PREFIX STREQUAL "")

    set (Gf_complete          "gf-complete")
    set (Gf_complete_CPPFLAGS
        "-I${KFS_EXTERNAL_PROJECT_DIR}${Gf_complete}/include")
    set (Gf_complete_CC        ${CMAKE_C_COMPILER})
    set (Gf_complete_PREFIX    ${CMAKE_CURRENT_BINARY_DIR}/${Gf_complete})
    ExternalProject_Add(Gf_complete_proj
        DOWNLOAD_COMMAND  ""
        SOURCE_DIR        ${KFS_EXTERNAL_PROJECT_DIR}${Gf_complete}
        CONFIGURE_COMMAND ${KFS_EXTERNAL_PROJECT_DIR}${Gf_complete}/configure
            CC=${Gf_complete_CC}
            CPPFLAGS=${Gf_complete_CPPFLAGS}
            --enable-static=yes
            --enable-shared=yes
            --prefix=${Gf_complete_PREFIX}
        BUILD_COMMAND ${MAKE}
    )
    set (Gf_complete_INCLUDE ${Gf_complete_PREFIX}/include)
    set (Gf_complete_LIB_DIR ${Gf_complete_PREFIX}/lib/)
    set(Gf_complete_STATIC_LIB
        ${Gf_complete_LIB_DIR}${CMAKE_STATIC_LIBRARY_PREFIX}gf_complete${CMAKE_STATIC_LIBRARY_SUFFIX}
    )
    set(Gf_complete_SHARED_LIB
        ${Gf_complete_LIB_DIR}${CMAKE_SHARED_LIBRARY_PREFIX}gf_complete${CMAKE_SHARED_LIBRARY_SUFFIX}
    )

    set (Jerasure          "jerasure")
    set (Jerasure_CPPFLAGS
        "-I${KFS_EXTERNAL_PROJECT_DIR}${Jerasure}/include -I${Gf_complete_INCLUDE}")
    set (Jerasure_LDFLAGS  "-L${Gf_complete_LIB_DIR}")
    set (Jerasure_CC       ${CMAKE_C_COMPILER})
    set (Jerasure_PREFIX   ${CMAKE_CURRENT_BINARY_DIR}/${Jerasure})
    ExternalProject_Add(Jerasure_proj
        DEPENDS           Gf_complete_proj
        DOWNLOAD_COMMAND  ""
        SOURCE_DIR        ${KFS_EXTERNAL_PROJECT_DIR}${Jerasure}
        CONFIGURE_COMMAND ${KFS_EXTERNAL_PROJECT_DIR}${Jerasure}/configure
            CC=${Jerasure_CC}
            CPPFLAGS=${Jerasure_CPPFLAGS}
            LDFLAGS=${Jerasure_LDFLAGS}
            --enable-static=yes
            --enable-shared=yes
            --prefix=${Jerasure_PREFIX}
        BUILD_COMMAND ${MAKE}
    )
    set (Jerasure_INCLUDE ${Jerasure_PREFIX}/include)
    set (Jerasure_LIB_DIR ${Jerasure_PREFIX}/lib/)
    set (Jerasure_STATIC_LIB
        ${Jerasure_LIB_DIR}${CMAKE_STATIC_LIBRARY_PREFIX}Jerasure${CMAKE_STATIC_LIBRARY_SUFFIX}
    )
    set (Jerasure_SHARED_LIB
        ${Jerasure_LIB_DIR}${CMAKE_SHARED_LIBRARY_PREFIX}Jerasure${CMAKE_SHARED_LIBRARY_SUFFIX}
    )

    install (FILES ${Gf_complete_STATIC_LIB} ${Jerasure_STATIC_LIB}
        DESTINATION lib/static
    )
    set (JERASURE_STATIC_LIBRARIES
        ${Jerasure_STATIC_LIB}
        ${Gf_complete_STATIC_LIB}
    )
    IF (CYGWIN AND NOT QFS_JERASURE_CYGWIN_USE_SHARED_LIBS)
        # It appears that on cygwin only static libs are built, and it is
        # possible to link client library dll against them.
        set (JERASURE_SHARED_LIBRARIES ${JERASURE_STATIC_LIBRARIES})
    ELSE (CYGWIN AND NOT QFS_JERASURE_CYGWIN_USE_SHARED_LIBS)
        # Shared library are sym linked, install both sym link and the targets
        # by using pattern. Allow version suffix that follows library suffix.
        install (DIRECTORY  ${Gf_complete_LIB_DIR} ${Jerasure_LIB_DIR}
            DESTINATION lib
            USE_SOURCE_PERMISSIONS
            FILES_MATCHING PATTERN
                "${CMAKE_SHARED_LIBRARY_PREFIX}*${CMAKE_SHARED_LIBRARY_SUFFIX}*"
        )
        set (JERASURE_SHARED_LIBRARIES
            ${Jerasure_SHARED_LIB}
            ${Gf_complete_SHARED_LIB}
        )
    ENDIF (CYGWIN AND NOT QFS_JERASURE_CYGWIN_USE_SHARED_LIBS)
ELSE (HAS_EXTERNAL_PROJECT AND NOT QFS_OMIT_JERASURE)
    IF (NOT QFS_OMIT_JERASURE)
        message(STATUS "WARNING: Jerasure requires cmake 2.8.0 or greater")
        message(STATUS "WARNING: Jerasure will not be compiled")
        set (QFS_OMIT_JERASURE ON)
    ELSE (NOT QFS_OMIT_JERASURE)
        message(STATUS "Jerasure will not be compiled")
    ENDIF (NOT QFS_OMIT_JERASURE)
    add_definitions(-DQFS_OMIT_JERASURE=1)
ENDIF (HAS_EXTERNAL_PROJECT AND NOT QFS_OMIT_JERASURE)

# Locate the path to jni.h
IF (NOT JNI_FOUND AND NOT QFS_OMIT_JNI)
    find_package(JNI)
ENDIF (NOT JNI_FOUND AND NOT QFS_OMIT_JNI)

IF (NOT DEFINED JNI_FOUND AND NOT "${JAVA_INCLUDE_PATH}" STREQUAL "")
    set(JNI_FOUND TRUE)
ENDIF (NOT DEFINED JNI_FOUND AND NOT "${JAVA_INCLUDE_PATH}" STREQUAL "")

IF (NOT OPENSSL_FOUND)
    find_package(OpenSSL REQUIRED)
ENDIF (NOT OPENSSL_FOUND)

IF (NOT KRB5_FOUND)
    find_package(Krb5 REQUIRED krb5)
    IF (NOT KRB5_FOUND)
        iF (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
            set (KRB5_INSTALL_INFO
                " Kerberos5 mac port required.")
            set (KRB5_INSTALL_INFO
                "${KRB5_INSTALL_INFO} To install: sudo port kerberos5")
        ENDIF (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
        message(FATAL_ERROR
            "Cannot find Kerberos5 libraries.${KRB5_INSTALL_INFO}")
    ENDIF (NOT KRB5_FOUND)
ENDIF (NOT KRB5_FOUND)
IF (KRB5_FLAVOR)
    add_definitions(-DKFS_KRB_USE_${KRB5_FLAVOR})
ENDIF (KRB5_FLAVOR)
IF (KRB5_USE_KRB5H)
    add_definitions(-DKFS_KRB_USE_KRB5H)
ENDIF (KRB5_USE_KRB5H)
IF ("${KRB5_FLAVOR}" STREQUAL "HEIMDAL" OR
        KRB5_HAS_krb5_get_init_creds_opt_set_out_ccache)
    add_definitions(-DKFS_KRB_USE_KRB5_GET_INIT_CREDS_OPT)
ENDIF ("${KRB5_FLAVOR}" STREQUAL "HEIMDAL" OR
        KRB5_HAS_krb5_get_init_creds_opt_set_out_ccache)
IF (KRB5_HAS_krb5_unparse_name_flags_ext)
    add_definitions(-DKRB5_HAS_krb5_unparse_name_flags_ext)
ENDIF (KRB5_HAS_krb5_unparse_name_flags_ext)

IF (NOT DEFINED OPENSSL_VERSION)
    execute_process(
        COMMAND openssl version
        ERROR_QUIET
        OUTPUT_VARIABLE OPENSSL_VERSION
    )
    string(REGEX REPLACE "^[^ ]+ +([^ ]+).*$" "\\1"
        OPENSSL_VERSION "${OPENSSL_VERSION}")
ENDIF (NOT DEFINED OPENSSL_VERSION)

IF (NOT OPENSSL_VERSION OR OPENSSL_VERSION MATCHES "^0[.]")
    message(STATUS "WARNING: Open SSL version ${OPENSSL_VERSION}")
    message(STATUS
        "WARNING: QFS authentication support requires Open SSL version 1.0 or greater")
    message(STATUS "WARNING: QFS authentication will not work")
ENDIF (NOT OPENSSL_VERSION OR OPENSSL_VERSION MATCHES "^0[.]")


ENABLE_TESTING()

# Change this to where the install directory is located
IF (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX "." CACHE PATH
        "installation directory prefix" FORCE)
ENDIF (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)

# Build with statically linked libraries; the value for this variable has to be
# defined here overwriting whatever is in the cache.
# When set to ON, we build with statically linked libraries; when off we
# link with dynamically linked libs

IF (NOT DEFINED QFS_USE_STATIC_LIB_LINKAGE)
    # Statically linked binaries don't work on solaris
    set (QFS_USE_STATIC_LIB_LINKAGE NOT CMAKE_SYSTEM_NAME STREQUAL "SunOS")
ENDIF  (NOT DEFINED QFS_USE_STATIC_LIB_LINKAGE)

IF (QFS_USE_STATIC_LIB_LINKAGE)
    message (STATUS "Build binaries with statically linked QFS libraries")
ELSE (QFS_USE_STATIC_LIB_LINKAGE)
    message (STATUS "Build binaries with dynamically linked QFS libraries")
ENDIF (QFS_USE_STATIC_LIB_LINKAGE)
set (USE_STATIC_LIB_LINKAGE ${QFS_USE_STATIC_LIB_LINKAGE} CACHE BOOL
    "Build binaries with statically linked libraries" FORCE)

IF (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    IF (BUILD_CPU_MODE STREQUAL "32")
        message (STATUS "Building 32-bit mode on Solaris")
        # If we are asked to build 32 bit mode
        add_definitions (
            -D_FILE_OFFSET_BITS=64
            -D_LARGEFILE_SOURCE
            -D_LARGE_FILES
        )
    ELSE (BUILD_CPU_MODE STREQUAL "32")
        # On solaris, use 64-bit mode
        set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -m64")
        set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} -m64")
        set(CMAKE_SHARED_LINKER_FLAGS  "${CMAKE_SHARED_LINKER_FLAGS} -m64")
    ENDIF (BUILD_CPU_MODE STREQUAL "32")
    # Cmake does whacky relink on solaris and messes things up; avoid this
    set (CMAKE_SKIP_RPATH ON)
ELSE (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    IF (CMAKE_SIZEOF_VOID_P EQUAL 4 AND NOT CYGWIN)
        message (STATUS "Enabling largefile source flags")
        add_definitions (
            -D_FILE_OFFSET_BITS=64
            -D_LARGEFILE_SOURCE
            -D_LARGEFILE64_SOURCE
            -D_LARGE_FILES
        )
    ENDIF (CMAKE_SIZEOF_VOID_P EQUAL 4 AND NOT CYGWIN)
ENDIF (CMAKE_SYSTEM_NAME STREQUAL "SunOS")

IF (ENABLE_PROFILING)
    message (STATUS "Enabling profiling with gprof")
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -pg")
    set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} -pg")
    set(CMAKE_SHAREDBoost_USE_MULTITHREADED_LINKER_FLAGS
        "${CMAKE_SHARED_LINKER_FLAGS} -pg")
    set(CMAKE_EXE_FLAGS  "${CMAKE_EXE_FLAGS} -pg")
ENDIF (ENABLE_PROFILING)

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

IF (${CMAKE_MAJOR_VERSION} LESS 2)
ELSEIF (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} LESS 8)
ELSEIF (${CMAKE_MAJOR_VERSION} EQUAL 2 AND
        ${CMAKE_MINOR_VERSION} EQUAL 8 AND ${CMAKE_PATCH_VERSION} LESS 3)
ELSE (${CMAKE_MAJOR_VERSION} EQUAL 2 AND
        ${CMAKE_MINOR_VERSION} EQUAL 8 AND ${CMAKE_PATCH_VERSION} LESS 3)
    SET(USE_FIND_FUSE_CMAKE_MODULE TRUE)
    IF (NOT QFS_OMIT_FUSE AND NOT FUSE_FOUND)
        find_package(FUSE)
    ENDIF (NOT QFS_OMIT_FUSE AND NOT FUSE_FOUND)
ENDIF (${CMAKE_MAJOR_VERSION} LESS 2)

IF (NOT USE_FIND_FUSE_CMAKE_MODULE AND NOT QFS_OMIT_FUSE AND NOT FUSE_FOUND)
    IF (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} LESS 6)
        INCLUDE(UsePkgConfig)
        PKGCONFIG("fuse"
            FUSE_INCLUDE_DIRS FUSE_LIBRARY_DIRS FUSE_LIBRARIES FUSE_DEFINITIONS)
        IF (DEFINED FUSE_LIBRARIES)
            set(FUSE_FOUND TRUE)
            STRING(REGEX REPLACE "-pthread" ""
                FUSE_LIBRARIES "${FUSE_LIBRARIES}")
            STRING(REGEX REPLACE " +-l" ";"
                FUSE_LIBRARIES "${FUSE_LIBRARIES}")
        ENDIF (DEFINED FUSE_LIBRARIES)
    ELSE (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} LESS 6)
        INCLUDE(FindPkgConfig)
        pkg_search_module(FUSE "fuse")
        set(FUSE_DEFINITIONS ${FUSE_CFLAGS} CACHE STRING INTERNAL FORCE)
    ENDIF (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} LESS 6)
    IF (FUSE_FOUND)
        set(FUSE_LIBS_LIST "")
        FOREACH(name ${FUSE_LIBRARIES})
            # Look for this library.
            find_library(FUSE_${name}_LIBRARY
                NAMES ${name}
                PATHS ${FUSE_LIBRARY_DIRS}
            )
            # If any library is not found then the whole package is not found.
            IF(NOT FUSE_${name}_LIBRARY)
                SET(FUSE_FOUND FALSE CACHE BOOL INTERNAL FORCE)
            ENDIF(NOT FUSE_${name}_LIBRARY)
            list(APPEND FUSE_LIBS_LIST "${FUSE_${name}_LIBRARY}")
        ENDFOREACH(name)
        IF (FUSE_FOUND)
            set(FUSE_LIBRARIES ${FUSE_LIBS_LIST} CACHE LIST INTERNAL FORCE)
        ELSE (FUSE_FOUND)
            set(FUSE_LIBRARIES "")
        ENDIF (FUSE_FOUND)
    ENDIF (FUSE_FOUND)
ENDIF (NOT USE_FIND_FUSE_CMAKE_MODULE AND NOT QFS_OMIT_FUSE AND NOT FUSE_FOUND)


# include dirs
include_directories(
    ${Boost_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
    ${KRB5_INCLUDE_DIRS}
    ${KFS_DIR_PREFIX}src/cc
)

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
add_subdirectory (${KFS_DIR_PREFIX}src/cc/krb src/cc/krb)
add_subdirectory (${KFS_DIR_PREFIX}src/cc/qfsc src/cc/qfsc)

add_subdirectory (${KFS_DIR_PREFIX}examples/cc examples/cc)
add_subdirectory (${KFS_DIR_PREFIX}benchmarks/mstress benchmarks/mstress)

IF (JNI_FOUND)
    message(STATUS "JNI found: building qfs_access")
    include_directories ( ${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2} )
    add_subdirectory (${KFS_DIR_PREFIX}src/cc/access src/cc/access)
ELSE (JNI_FOUND)
    message(STATUS "JNI NOT found: NOT building qfs_access")
ENDIF (JNI_FOUND)

IF (NOT QFS_OMIT_FUSE AND FUSE_FOUND)
    message(STATUS "Building qfs_fuse")
    message(STATUS "fuse libs:         ${FUSE_LIBRARIES}")
    message(STATUS "fuse include dirs: ${FUSE_INCLUDE_DIRS}")
    message(STATUS "fuse definitions:  ${FUSE_DEFINITIONS}")
    add_definitions(${FUSE_DEFINITIONS})
    add_definitions(-D FUSE_USE_VERSION=26)
    include_directories(${FUSE_INCLUDE_DIRS})
    add_subdirectory (${KFS_DIR_PREFIX}src/cc/fuse src/cc/fuse)
ELSE  (NOT QFS_OMIT_FUSE AND FUSE_FOUND)
    message(STATUS "Not building qfs_fuse")
ENDIF (NOT QFS_OMIT_FUSE AND FUSE_FOUND)
