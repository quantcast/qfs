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

cmake_minimum_required(VERSION 2.8.4 FATAL_ERROR)
cmake_policy(VERSION ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION})

IF (NOT DEFINED KFS_DIR_PREFIX)
    set (KFS_DIR_PREFIX ${CMAKE_SOURCE_DIR})
ENDIF (NOT DEFINED KFS_DIR_PREFIX)

project (QFS)
set(CMAKE_MODULE_PATH ${KFS_DIR_PREFIX}/cmake/Modules)

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

if(NOT DEFINED QFS_USE_STATIC_LIB_LINKAGE)
    set(QFS_USE_STATIC_LIB_LINKAGE ON)
endif()

if(NOT DEFINED Boost_USE_STATIC_LIBS)
    set(Boost_USE_STATIC_LIBS OFF)
endif()

set(Boost_USE_MULTITHREADED ON)

# Require the packages we need to build
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin" OR CYGWIN)
    find_package(Boost COMPONENTS regex system REQUIRED)
else()
    find_package(Boost COMPONENTS regex REQUIRED)
endif()

set(CMAKE_FIND_LIBRARY_PREFIXES ${CMAKE_FIND_LIBRARY_PREFIXES} "")
set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES} ".dll.a" ".lib")
add_definitions(-D__int64=int64_t)

find_package(Jerasure REQUIRED)
find_package(JNI REQUIRED)
find_package(Krb5 REQUIRED krb5)
find_package(OpenSSL)
find_package(FUSE)
find_package(gtest)

# Define various options based on the library configuration we use
if(KRB5_FLAVOR)
    add_definitions(-DKFS_KRB_USE_${KRB5_FLAVOR})
endif()
if(KRB5_USE_KRB5H)
    add_definitions(-DKFS_KRB_USE_KRB5H)
endif()
if("${KRB5_FLAVOR}" STREQUAL "HEIMDAL" OR KRB5_HAS_krb5_get_init_creds_opt_set_out_ccache)
    add_definitions(-DKFS_KRB_USE_KRB5_GET_INIT_CREDS_OPT)
endif()
if(KRB5_HAS_krb5_unparse_name_flags_ext)
    add_definitions(-DKRB5_HAS_krb5_unparse_name_flags_ext)
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
# When setto ON, we build with statically linked libraries; when off we
# link with dynamically linked libs

if(QFS_USE_STATIC_LIB_LINKAGE)
    message(STATUS "Build binaries with statically linked QFS libraries")
else()
    message(STATUS "Build binaries with dynamically linked QFS libraries")
endif()
set(USE_STATIC_LIB_LINKAGE ${QFS_USE_STATIC_LIB_LINKAGE} CACHE BOOL
    "Build binaries with statically linked libraries" FORCE)

if(CMAKE_SIZEOF_VOID_P EQUAL 4 AND NOT CYGWIN)
    message(STATUS "Enabling largefile source flags")
    add_definitions (
        -D_FILE_OFFSET_BITS=64
        -D_LARGEFILE_SOURCE
        -D_LARGEFILE64_SOURCE
        -D_LARGE_FILES
    )
endif()

if(ENABLE_COVERAGE)
    message(STATUS "Enabling code coverage with gcov")
    set(CMAKE_CXX_FLAGS  "-coverage")
    set(CMAKE_C_FLAGS "-coverage")
endif()

if(ENABLE_PROFILING)
    message(STATUS "Enabling profiling with gprof")
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -pg")
    set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} -pg")
    set(CMAKE_SHAREDBoost_USE_MULTITHREADED_LINKER_FLAGS
        "${CMAKE_SHARED_LINKER_FLAGS} -pg")
    set(CMAKE_EXE_FLAGS  "${CMAKE_EXE_FLAGS} -pg")
endif()

# Change the line to Release to build release binaries
# For servers, build with debugging info; for tools, build Release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -D NDEBUG -g3")
endif()

set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -DBOOST_SP_USE_QUICK_ALLOCATOR")
string(TOUPPER KFS_OS_NAME_${CMAKE_SYSTEM_NAME} KFS_OS_NAME)
add_definitions (-D${KFS_OS_NAME})

# include dirs
include_directories(
    ${Boost_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
    ${KRB5_INCLUDE_DIRS}
    ${KFS_DIR_PREFIX}/src/cc
    ${JAVA_INCLUDE_PATH}
    ${JAVA_INCLUDE_PATH2}
)

if(FUSE_FOUND)
    message(STATUS "Building qfs_fuse")
    message(STATUS "fuse libs:         ${FUSE_LIBRARIES}")
    message(STATUS "fuse include dirs: ${FUSE_INCLUDE_DIRS}")
    message(STATUS "fuse definitions:  ${FUSE_DEFINITIONS}")
    add_definitions(${FUSE_DEFINITIONS})
    add_definitions(-D FUSE_USE_VERSION=26)
    include_directories(${FUSE_INCLUDE_DIRS})
else()
    message(STATUS "Not building qfs_fuse")
endif()

# get the subdirs we want
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/access src/cc/access)
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
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/tests src/cc/tests)
add_subdirectory(${KFS_DIR_PREFIX}/src/cc/tools src/cc/tools)
add_subdirectory(${KFS_DIR_PREFIX}/src/test-scripts src/test-scripts)

add_subdirectory(${KFS_DIR_PREFIX}/examples/cc examples/cc)
add_subdirectory(${KFS_DIR_PREFIX}/benchmarks/mstress benchmarks/mstress)
add_subdirectory(${KFS_DIR_PREFIX}/contrib/plugins contrib/plugins)

if(FUSE_FOUND)
    add_subdirectory(${KFS_DIR_PREFIX}/src/cc/fuse src/cc/fuse)
endif()
