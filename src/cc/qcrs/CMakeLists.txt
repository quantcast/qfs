#
# $Id$
#
# Created 2010/08/05
# Author: Mike Ovsiannikov
#
# Copyright 2010-2011 Quantcast Corp.
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

include(CheckCCompilerFlag)

set (sources
decode.c
encode.c
rs_table.c
)

add_library (kfsrs STATIC ${sources})
add_library (kfsrs-shared SHARED ${sources})
set_target_properties (kfsrs PROPERTIES OUTPUT_NAME "qfs_qcrs")
set_target_properties (kfsrs-shared PROPERTIES OUTPUT_NAME "qfs_qcrs")

#
# Since the objects have to be built twice, set this up so they don't
# clobber each other.

set_target_properties (kfsrs PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kfsrs-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

# If vector mode is not defined, attempt to detect it
if (NOT DEFINED vectormode)
    message(STATUS "System name: ${CMAKE_SYSTEM_NAME}")
    message(STATUS "System processor: ${CMAKE_SYSTEM_PROCESSOR}")
    # OS X uses sysctl instead of /proc/cpuinfo
    if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
        execute_process(
            COMMAND sh -c "sysctl machdep.cpu.features | grep -wi ssse3"
            OUTPUT_QUIET
            ERROR_QUIET
            RESULT_VARIABLE MY_SSSE3_SUPPORTED_RET
        )
        if (MY_SSSE3_SUPPORTED_RET EQUAL 0)
            set(vectormode ssse3)
        else (MY_SSSE3_SUPPORTED_RET EQUAL 0)
            execute_process(
                COMMAND sh -c "sysctl machdep.cpu.features | grep -wi sse2"
                OUTPUT_QUIET
                ERROR_QUIET
                RESULT_VARIABLE MY_SSE2_SUPPORTED_RET
            )
            if (MY_SSE2_SUPPORTED_RET EQUAL 0)
                set(vectormode sse2)
            endif (MY_SSE2_SUPPORTED_RET EQUAL 0)
        endif (MY_SSSE3_SUPPORTED_RET EQUAL 0)
    elseif (CMAKE_SYSTEM_PROCESSOR MATCHES ^arm)
        if (EXISTS /proc/cpuinfo)
            # Check for the NEON feature in /proc/cpuinfo on ARM processors
            execute_process(
                COMMAND sh -c "grep -w Features /proc/cpuinfo | grep -w neon"
                OUTPUT_QUIET
                ERROR_QUIET
                RESULT_VARIABLE MY_NEON_SUPPORTED_RET
            )
            if (MY_NEON_SUPPORTED_RET EQUAL 0)
                set(vectormode neon)
            endif (MY_NEON_SUPPORTED_RET EQUAL 0)
        endif (EXISTS /proc/cpuinfo)
    elseif (EXISTS /proc/cpuinfo OR CYGWIN) # EXISTS check fails on Cygwin
        # Check for SSE2 and SSSE3 in /proc/cpuinfo if it exists, since some
        # environments like Cygwin report "unknown" for processor
        # (Note that SSSE3 is not the same as SSE3, and not all 64-bit x86
        # processors have SSSE3, especially non-Intel ones)
        execute_process(
            COMMAND sh -c "grep -w flags /proc/cpuinfo | grep -w ssse3"
            OUTPUT_QUIET
            ERROR_QUIET
            RESULT_VARIABLE MY_SSSE3_SUPPORTED_RET
        )
        if (MY_SSSE3_SUPPORTED_RET EQUAL 0)
            set(vectormode ssse3)
        else (MY_SSSE3_SUPPORTED_RET EQUAL 0)
            execute_process(
                COMMAND sh -c "grep -w flags /proc/cpuinfo | grep -w sse2"
                OUTPUT_QUIET
                ERROR_QUIET
                RESULT_VARIABLE MY_SSE2_SUPPORTED_RET
            )
            if (MY_SSE2_SUPPORTED_RET EQUAL 0)
                message(STATUS "qcrs: no ssse3 flag present in /proc/cpuinfo, assuming no ssse3 support.")
                message(STATUS "qcrs: Ssse3 can be forced with cmake -D vecormode=ssse3 if the target cpu has ssse3 support.")
                set(vectormode sse2)
            endif (MY_SSE2_SUPPORTED_RET EQUAL 0)
        endif (MY_SSSE3_SUPPORTED_RET EQUAL 0)
    elseif (CMAKE_SYSTEM_PROCESSOR MATCHES x86_64 OR
        CMAKE_SYSTEM_PROCESSOR MATCHES amd64 OR
        CMAKE_SYSTEM_PROCESSOR MATCHES i686)
        # Assume that SSE2 is always available on x86
        set(vectormode sse2)
    endif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
endif (NOT DEFINED vectormode)

if (NOT DEFINED vectormode)
    message(STATUS "qcrs: not using cpu vector extensions performance can be up 100 times lower")
endif (NOT DEFINED vectormode)

# Vector intrinsics are only available on GCC
if (DEFINED vectormode)
    if (CMAKE_COMPILER_IS_GNUCC OR "${CMAKE_C_COMPILER_ID}" STREQUAL "Clang")
        if (vectormode STREQUAL ssse3)
            message(STATUS "qcrs: enabling ssse3")
            add_definitions(-mssse3 -DLIBRS_USE_SSSE3)
        elseif (vectormode STREQUAL sse2)
            message(STATUS "qcrs: enabling sse2")
            add_definitions(-msse2 -DLIBRS_USE_SSE2)
        elseif (vectormode STREQUAL neon)
            message(STATUS "qcrs: enabling neon")
            add_definitions(-mfpu=neon -DLIBRS_USE_NEON)
        endif (vectormode STREQUAL ssse3)
        if (vectormode STREQUAL ssse3 OR vectormode STREQUAL sse2)
            CHECK_C_COMPILER_FLAG(-flax-vector-conversions MY_LAXVEC_CONV)
            if (MY_LAXVEC_CONV)
                add_definitions(-flax-vector-conversions)
            endif (MY_LAXVEC_CONV)
        endif (vectormode STREQUAL ssse3 OR vectormode STREQUAL sse2)
    endif (CMAKE_COMPILER_IS_GNUCC OR "${CMAKE_C_COMPILER_ID}" STREQUAL "Clang")
endif (DEFINED vectormode)
if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "qcrs: enabling -O3 flag")
    add_definitions(-O3)
endif (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")

set(rstestbin rstest)
set(rsmktablebin rsmktable)
add_executable (${rstestbin} rs_test_main.c)
add_executable (${rsmktablebin} mktable_main.c)

target_link_libraries (${rstestbin} kfsrs)
add_dependencies (${rstestbin} kfsrs)
add_dependencies (${rsmktablebin} kfsrs)

install (TARGETS kfsrs kfsrs-shared
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)
