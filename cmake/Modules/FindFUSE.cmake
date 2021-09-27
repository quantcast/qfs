#
# $Id$
#
# Created 2016/03/31
#
# Copyright 2016-2017 Quantcast Corporation. All rights reserved.
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

# This module can find FUSE Library
#
# Requirements:
# - CMake >= 2.8.3
#
# The following variables will be defined for your use:
#   - FUSE_FOUND         : was FUSE found?
#   - FUSE_INCLUDE_DIRS  : FUSE include directory
#   - FUSE_LIBRARIES     : FUSE library
#   - FUSE_DEFINITIONS   : FUSE cflags
#   - FUSE_VERSION       : complete version of FUSE (major.minor)
#   - FUSE_MAJOR_VERSION : major version of FUSE
#   - FUSE_MINOR_VERSION : minor version of FUSE
#
# Example Usage:
#
#   1. Copy this file in the root of your project source directory
#   2. Then, tell CMake to search this non-standard module in your project directory by adding to your CMakeLists.txt:
#     set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR})
#   3. Finally call find_package() once, here are some examples to pick from
#
#   Require FUSE 2.6 or later
#     find_package(FUSE 2.6 REQUIRED)
#
#   if(FUSE_FOUND)
#      add_definitions(${FUSE_DEFINITIONS})
#      include_directories(${FUSE_INCLUDE_DIRS})
#      add_executable(myapp myapp.c)
#      target_link_libraries(myapp ${FUSE_LIBRARIES})
#   endif()

#=============================================================================
# Copyright (c) 2012, julp
#
# Distributed under the OSI-approved BSD License
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#=============================================================================

cmake_minimum_required(VERSION 2.8.3...10.0)

########## Private ##########
function(fusedebug _varname)
    if(FUSE_DEBUG)
        message("${_varname} = ${${_varname}}")
    endif(FUSE_DEBUG)
endfunction(fusedebug)

########## Public ##########
set(FUSE_FOUND TRUE)
set(FUSE_LIBRARIES )
set(FUSE_DEFINITIONS )
set(FUSE_INCLUDE_DIRS )

find_package(PkgConfig)

set(PC_FUSE_INCLUDE_DIRS )
set(PC_FUSE_LIBRARY_DIRS )
if(PKG_CONFIG_FOUND)
    pkg_check_modules(PC_FUSE "fuse" QUIET)
    if(PC_FUSE_FOUND)
        fusedebug(PC_FUSE_LIBRARIES)
        fusedebug(PC_FUSE_LIBRARY_DIRS)
        fusedebug(PC_FUSE_LDFLAGS)
        fusedebug(PC_FUSE_LDFLAGS_OTHER)
        fusedebug(PC_FUSE_INCLUDE_DIRS)
        fusedebug(PC_FUSE_CFLAGS)
        fusedebug(PC_FUSE_CFLAGS_OTHER)
        set(FUSE_DEFINITIONS "${PC_FUSE_CFLAGS_OTHER}")
    endif(PC_FUSE_FOUND)
endif(PKG_CONFIG_FOUND)

if ("${PC_FUSE_INCLUDE_DIRS}" STREQUAL "")
    find_path(
        FUSE_INCLUDE_DIRS
        NAMES fuse.h
        PATHS "${PC_FUSE_INCLUDE_DIRS}"
        DOC "Include directories for FUSE"
    )
else()
    find_path(
        FUSE_INCLUDE_DIRS
        NAMES fuse.h
        NO_DEFAULT_PATH
        PATHS "${PC_FUSE_INCLUDE_DIRS}"
        DOC "Include directories for FUSE"
    )
endif()

if(NOT FUSE_INCLUDE_DIRS)
    set(FUSE_FOUND FALSE)
endif(NOT FUSE_INCLUDE_DIRS)

find_library(
    FUSE_LIBRARIES
    NAMES "libosxfuse.dylib" "fuse"
    PATHS "${PC_FUSE_LIBRARY_DIRS}"
    DOC "Libraries for FUSE"
)
fusedebug(FUSE_LIBRARIES)

if(NOT FUSE_LIBRARIES)
    set(FUSE_FOUND FALSE)
endif(NOT FUSE_LIBRARIES)

if(FUSE_FOUND)
    FOREACH(FUSE_VERSION_INCLUDE
            "${FUSE_INCLUDE_DIRS}/osxfuse/fuse/fuse_common.h"
            "${FUSE_INCLUDE_DIRS}/fuse/fuse_common.h"
            "${FUSE_INCLUDE_DIRS}/fuse_common.h")
        if (EXISTS "${FUSE_VERSION_INCLUDE}")
            file(READ "${FUSE_VERSION_INCLUDE}" _contents)
            string(REGEX REPLACE ".*# *define *FUSE_MAJOR_VERSION *([0-9]+).*" "\\1" FUSE_MAJOR_VERSION "${_contents}")
            string(REGEX REPLACE ".*# *define *FUSE_MINOR_VERSION *([0-9]+).*" "\\1" FUSE_MINOR_VERSION "${_contents}")
            set(FUSE_VERSION "${FUSE_MAJOR_VERSION}.${FUSE_MINOR_VERSION}")
        endif()
    endforeach()

    include(CheckCSourceCompiles)
    # Backup CMAKE_REQUIRED_*
    set(OLD_CMAKE_REQUIRED_INCLUDES "${CMAKE_REQUIRED_INCLUDES}")
    set(OLD_CMAKE_REQUIRED_LIBRARIES "${CMAKE_REQUIRED_LIBRARIES}")
    set(OLD_CMAKE_REQUIRED_DEFINITIONS "${CMAKE_REQUIRED_DEFINITIONS}")
    # Add FUSE compilation flags
    set(CMAKE_REQUIRED_INCLUDES "${CMAKE_REQUIRED_INCLUDES}" "${FUSE_INCLUDE_DIRS}")
    set(CMAKE_REQUIRED_LIBRARIES "${CMAKE_REQUIRED_LIBRARIES}" "${FUSE_LIBRARIES}")
    set(CMAKE_REQUIRED_DEFINITIONS "${CMAKE_REQUIRED_DEFINITIONS}" "${FUSE_DEFINITIONS}")
    check_c_source_compiles("#include <stdlib.h>
#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

int main(void) {
    return 0;
}" FUSE_CFLAGS_CHECK)
    if(NOT FUSE_CFLAGS_CHECK)
        set(FUSE_DEFINITIONS "-D_FILE_OFFSET_BITS=64")
        # Should we run again previous test to assume the failure was due to missing definition -D_FILE_OFFSET_BITS=64?
    endif(NOT FUSE_CFLAGS_CHECK)
    # Restore CMAKE_REQUIRED_*
    set(CMAKE_REQUIRED_INCLUDES "${OLD_CMAKE_REQUIRED_INCLUDES}")
    set(CMAKE_REQUIRED_LIBRARIES "${OLD_CMAKE_REQUIRED_LIBRARIES}")
    set(CMAKE_REQUIRED_DEFINITIONS "${OLD_CMAKE_REQUIRED_DEFINITIONS}")
endif(FUSE_FOUND)

if(FUSE_INCLUDE_DIRS)
    include(FindPackageHandleStandardArgs)
    if(FUSE_FIND_REQUIRED AND NOT FUSE_FIND_QUIETLY)
        find_package_handle_standard_args(FUSE REQUIRED_VARS FUSE_LIBRARIES FUSE_INCLUDE_DIRS VERSION_VAR FUSE_VERSION)
    else()
        find_package_handle_standard_args(FUSE "FUSE not found" FUSE_LIBRARIES FUSE_INCLUDE_DIRS)
    endif()
else(FUSE_INCLUDE_DIRS)
    if(FUSE_FIND_REQUIRED AND NOT FUSE_FIND_QUIETLY)
        message(FATAL_ERROR "Could not find FUSE include directory")
    endif()
endif(FUSE_INCLUDE_DIRS)

mark_as_advanced(
    FUSE_INCLUDE_DIRS
    FUSE_LIBRARIES
)

# IN (args)
fusedebug("FUSE_FIND_COMPONENTS")
fusedebug("FUSE_FIND_REQUIRED")
fusedebug("FUSE_FIND_QUIETLY")
fusedebug("FUSE_FIND_VERSION")
# OUT
# Found
fusedebug("FUSE_FOUND")
# Definitions
fusedebug("FUSE_DEFINITIONS")
# Linking
fusedebug("FUSE_INCLUDE_DIRS")
fusedebug("FUSE_LIBRARIES")
# Version
fusedebug("FUSE_MAJOR_VERSION")
fusedebug("FUSE_MINOR_VERSION")
fusedebug("FUSE_VERSION")

if(NOT FUSE_FOUND)
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
        # Suppress warnings, from FindPkgConfig
        # see https://cmake.org/cmake/help/latest/module/FindPackageHandleStandardArgs.html
        set(FPHSA_NAME_MISMATCHED 1)
        INCLUDE(FindPkgConfig)
        unset(FPHSA_NAME_MISMATCHED)
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
