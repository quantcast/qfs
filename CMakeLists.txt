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

project (QFS)
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/Modules)

if(NOT DEFINED Boost_USE_STATIC_LIBS)
    set(Boost_USE_STATIC_LIBS OFF)
endif()

set(Boost_USE_MULTITHREADED ON)

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin" OR CYGWIN)
    find_package(Boost COMPONENTS regex system REQUIRED)
else()
    find_package(Boost COMPONENTS regex REQUIRED)
endif()

message(STATUS "Boost includes: ${Boost_INCLUDE_DIRS}")
message(STATUS "Boost libs:     ${Boost_LIBRARIES}")

if(${CMAKE_MAJOR_VERSION} LESS 2)
elseif(${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} LESS 8)
else()
    set(HAS_EXTERNAL_PROJECT ON)
endif()

if(HAS_EXTERNAL_PROJECT AND NOT QFS_OMIT_JERASURE)
    # gf-complete autoconf overrides CFGLAGS. Use CPPFLAGS to add compiler flags.
    include(ExternalProject)
    set(KFS_EXTERNAL_PROJECT_DIR ${CMAKE_SOURCE_DIR}/ext/)

    set(Gf_complete          "gf-complete")
    set(Gf_complete_CPPFLAGS
        "-I${KFS_EXTERNAL_PROJECT_DIR}${Gf_complete}/include")
    set(Gf_complete_CC        ${CMAKE_C_COMPILER})
    set(Gf_complete_PREFIX    ${CMAKE_CURRENT_BINARY_DIR}/${Gf_complete})
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
    set(Gf_complete_INCLUDE ${Gf_complete_PREFIX}/include)
    set(Gf_complete_LIB_DIR ${Gf_complete_PREFIX}/lib/)
    set(Gf_complete_STATIC_LIB
        ${Gf_complete_LIB_DIR}${CMAKE_STATIC_LIBRARY_PREFIX}gf_complete${CMAKE_STATIC_LIBRARY_SUFFIX}
    )
    set(Gf_complete_SHARED_LIB
        ${Gf_complete_LIB_DIR}${CMAKE_SHARED_LIBRARY_PREFIX}gf_complete${CMAKE_SHARED_LIBRARY_SUFFIX}
    )

    set(Jerasure          "jerasure")
    set(Jerasure_CPPFLAGS
        "-I${KFS_EXTERNAL_PROJECT_DIR}${Jerasure}/include -I${Gf_complete_INCLUDE}")
    set(Jerasure_LDFLAGS  "-L${Gf_complete_LIB_DIR}")
    set(Jerasure_CC       ${CMAKE_C_COMPILER})
    set(Jerasure_PREFIX   ${CMAKE_CURRENT_BINARY_DIR}/${Jerasure})
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
    set(Jerasure_INCLUDE ${Jerasure_PREFIX}/include)
    set(Jerasure_LIB_DIR ${Jerasure_PREFIX}/lib/)
    set(Jerasure_STATIC_LIB
        ${Jerasure_LIB_DIR}${CMAKE_STATIC_LIBRARY_PREFIX}Jerasure${CMAKE_STATIC_LIBRARY_SUFFIX}
    )
    set(Jerasure_SHARED_LIB
        ${Jerasure_LIB_DIR}${CMAKE_SHARED_LIBRARY_PREFIX}Jerasure${CMAKE_SHARED_LIBRARY_SUFFIX}
    )

    install (FILES ${Gf_complete_STATIC_LIB} ${Jerasure_STATIC_LIB}
        DESTINATION lib/static
    )
    set(JERASURE_STATIC_LIBRARIES
        ${Jerasure_STATIC_LIB}
        ${Gf_complete_STATIC_LIB}
    )
    if(CYGWIN AND NOT QFS_JERASURE_CYGWIN_USE_SHARED_LIBS)
        # It appears that on cygwin only static libs are built, and it is
        # possible to link client library dll against them.
        set(JERASURE_SHARED_LIBRARIES ${JERASURE_STATIC_LIBRARIES})
    else()
        # Shared library are sym linked, install both sym link and the targets
        # by using pattern. Allow version suffix that follows library suffix.
        install (DIRECTORY  ${Gf_complete_LIB_DIR} ${Jerasure_LIB_DIR}
            DESTINATION lib
            USE_SOURCE_PERMISSIONS
            FILES_MATCHING PATTERN
                "${CMAKE_SHARED_LIBRARY_PREFIX}*${CMAKE_SHARED_LIBRARY_SUFFIX}*"
        )
        set(JERASURE_SHARED_LIBRARIES
            ${Jerasure_SHARED_LIB}
            ${Gf_complete_SHARED_LIB}
        )
    endif()
else()
    if(NOT QFS_OMIT_JERASURE)
        message(STATUS "WARNING: Jerasure requires cmake 2.8.0 or greater")
        message(STATUS "WARNING: Jerasure will not be compiled")
        set(QFS_OMIT_JERASURE ON)
    else()
        message(STATUS "Jerasure will not be compiled")
    endif()
    add_definitions(-DQFS_OMIT_JERASURE=1)
endif()

# Locate the path to jni.h
if(NOT JNI_FOUND AND NOT QFS_OMIT_JNI)
    find_package(JNI)
endif()

if(NOT DEFINED JNI_FOUND AND NOT "${JAVA_INCLUDE_PATH}" STREQUAL "")
    set(JNI_FOUND TRUE)
endif()

if(NOT OPENSSL_FOUND)
    find_package(OpenSSL REQUIRED)
endif()

if(NOT KRB5_FOUND)
    find_package(Krb5 REQUIRED krb5)
    if(NOT KRB5_FOUND)
        iF (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
            set(KRB5_INSTALL_INFO
                " Kerberos5 mac port required.")
            set(KRB5_INSTALL_INFO
                "${KRB5_INSTALL_INFO} To install: sudo port kerberos5")
        endif()
        message(FATAL_ERROR
            "Cannot find Kerberos5 libraries.${KRB5_INSTALL_INFO}")
    endif()
endif()
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

if(NOT DEFINED OPENSSL_VERSION)
    execute_process(
        COMMAND openssl version
        ERROR_QUIET
        OUTPUT_VARIABLE OPENSSL_VERSION
    )
    string(REGEX REPLACE "^[^ ]+ +([^ ]+).*$" "\\1"
        OPENSSL_VERSION "${OPENSSL_VERSION}")
endif()

if(NOT OPENSSL_VERSION OR OPENSSL_VERSION MATCHES "^0[.]")
    message(STATUS "WARNING: Open SSL version ${OPENSSL_VERSION}")
    message(STATUS
        "WARNING: QFS authentication support requires Open SSL version 1.0 or greater")
    message(STATUS "WARNING: QFS authentication will not work")
endif()


ENABLE_TESTING()

# Change this to where the install directory is located
if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX "." CACHE PATH
        "installation directory prefix" FORCE)
endif()

# Build with statically linked libraries; the value for this variable has to be
# defined here overwriting whatever is in the cache.
# When setto ON, we build with statically linked libraries; when off we
# link with dynamically linked libs

if(NOT DEFINED QFS_USE_STATIC_LIB_LINKAGE)
    # Statically linked binaries don't work on solaris
    set(QFS_USE_STATIC_LIB_LINKAGE NOT CMAKE_SYSTEM_NAME STREQUAL "SunOS")
endif()

if(QFS_USE_STATIC_LIB_LINKAGE)
    message(STATUS "Build binaries with statically linked QFS libraries")
else()
    message(STATUS "Build binaries with dynamically linked QFS libraries")
endif()
set(USE_STATIC_LIB_LINKAGE ${QFS_USE_STATIC_LIB_LINKAGE} CACHE BOOL
    "Build binaries with statically linked libraries" FORCE)

if(CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    if(BUILD_CPU_MODE STREQUAL "32")
        message(STATUS "Building 32-bit mode on Solaris")
        # If we are asked to build 32 bit mode
        add_definitions (
            -D_FILE_OFFSET_BITS=64
            -D_LARGEFILE_SOURCE
            -D_LARGE_FILES
        )
    else()
        # On solaris, use 64-bit mode
        set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -m64")
        set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} -m64")
        set(CMAKE_SHARED_LINKER_FLAGS  "${CMAKE_SHARED_LINKER_FLAGS} -m64")
    endif()
    # Cmake does whacky relink on solaris and messes things up; avoid this
    set(CMAKE_SKIP_RPATH ON)
else()
    if(CMAKE_SIZEOF_VOID_P EQUAL 4 AND NOT CYGWIN)
        message(STATUS "Enabling largefile source flags")
        add_definitions (
            -D_FILE_OFFSET_BITS=64
            -D_LARGEFILE_SOURCE
            -D_LARGEFILE64_SOURCE
            -D_LARGE_FILES
        )
    endif()
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
#

if(NOT CMAKE_BUILD_TYPE)
    message(STATUS "Setting build type to Debug")
    set(CMAKE_BUILD_TYPE "Debug")
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    message(STATUS "Enabling -D NDEBUG flag")
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -D NDEBUG -g3")
endif()

set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -DBOOST_SP_USE_QUICK_ALLOCATOR")
string(TOUPPER KFS_OS_NAME_${CMAKE_SYSTEM_NAME} KFS_OS_NAME)
add_definitions (-D${KFS_OS_NAME})

#
# Find the path to libfuse.so
#

if(${CMAKE_MAJOR_VERSION} LESS 2)
elseif(${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} LESS 8)
elseif(${CMAKE_MAJOR_VERSION} EQUAL 2 AND
        ${CMAKE_MINOR_VERSION} EQUAL 8 AND ${CMAKE_PATCH_VERSION} LESS 3)
else()
    set(USE_FIND_FUSE_CMAKE_MODULE TRUE)
    if(NOT QFS_OMIT_FUSE AND NOT FUSE_FOUND)
        find_package(FUSE)
    endif()
endif()

if(NOT USE_FIND_FUSE_CMAKE_MODULE AND NOT QFS_OMIT_FUSE AND NOT FUSE_FOUND)
    if(${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} LESS 6)
        INCLUDE(UsePkgConfig)
        PKGCONFIG("fuse"
            FUSE_INCLUDE_DIRS FUSE_LIBRARY_DIRS FUSE_LIBRARIES FUSE_DEFINITIONS)
        if(DEFINED FUSE_LIBRARIES)
            set(FUSE_FOUND TRUE)
            STRING(REGEX REPLACE "-pthread" ""
                FUSE_LIBRARIES "${FUSE_LIBRARIES}")
            STRING(REGEX REPLACE " +-l" ";"
                FUSE_LIBRARIES "${FUSE_LIBRARIES}")
        endif()
    else()
        INCLUDE(FindPkgConfig)
        pkg_search_module(FUSE "fuse")
        set(FUSE_DEFINITIONS ${FUSE_CFLAGS} CACHE STRING INTERNAL FORCE)
    endif()
    if(FUSE_FOUND)
        set(FUSE_LIBS_LIST "")
        foreach(name ${FUSE_LIBRARIES})
            # Look for this library.
            find_library(FUSE_${name}_LIBRARY
                NAMES ${name}
                PATHS ${FUSE_LIBRARY_DIRS}
            )
            # If any library is not found then the whole package is not found.
            IF(NOT FUSE_${name}_LIBRARY)
                set(FUSE_FOUND FALSE CACHE BOOL INTERNAL FORCE)
            endif()
            list(APPEND FUSE_LIBS_LIST "${FUSE_${name}_LIBRARY}")
        ENDFOREACH(name)
        if(FUSE_FOUND)
            set(FUSE_LIBRARIES ${FUSE_LIBS_LIST} CACHE LIST INTERNAL FORCE)
        else()
            set(FUSE_LIBRARIES "")
        endif()
    endif()
endif()

# include dirs
include_directories(
    ${Boost_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
    ${KRB5_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/src/cc
)

if(JNI_FOUND)
    message(STATUS "JNI found: building qfs_access")
    include_directories ( ${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2} )
else()
    message(STATUS "JNI NOT found: NOT building qfs_access")
endif()

if(NOT QFS_OMIT_FUSE AND FUSE_FOUND)
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
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/chunk src/cc/chunk)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/common src/cc/common)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/devtools src/cc/devtools)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/emulator src/cc/emulator)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/kfsio src/cc/kfsio)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/krb src/cc/krb)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/libclient src/cc/libclient)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/meta src/cc/meta)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/qcdio src/cc/qcdio)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/qcrs src/cc/qcrs)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/qfsc src/cc/qfsc)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/s3io src/cc/s3io)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/tests src/cc/tests)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/tools src/cc/tools)
add_subdirectory(${CMAKE_SOURCE_DIR}/src/test-scripts src/test-scripts)

add_subdirectory(${CMAKE_SOURCE_DIR}/examples/cc examples/cc)
add_subdirectory(${CMAKE_SOURCE_DIR}/benchmarks/mstress benchmarks/mstress)
add_subdirectory(${CMAKE_SOURCE_DIR}/contrib/plugins contrib/plugins)

if(NOT QFS_OMIT_FUSE AND FUSE_FOUND)
    add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/fuse src/cc/fuse)
endif()

if(JNI_FOUND)
    add_subdirectory(${CMAKE_SOURCE_DIR}/src/cc/access src/cc/access)
endif()
