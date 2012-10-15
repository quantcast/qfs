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

if (CMAKE_SYSTEM_PROCESSOR MATCHES x86_64)
    add_definitions(-mssse3 -flax-vector-conversions)
else (CMAKE_SYSTEM_PROCESSOR MATCHES x86_64)
    # On 32 bit platform enable ssse3 if /proc/cpuinfo does not exists or cpu
    # has ssse3 flag.
    execute_process(
        COMMAND sh -c "test -f /proc/cpuinfo || exit 0; grep -w flags /proc/cpuinfo | grep -w ssse3"
        OUTPUT_QUIET
        ERROR_QUIET
        RESULT_VARIABLE MY_SSSE3_SUPPORTED_RET
    )
    if (MY_SSSE3_SUPPORTED_RET EQUAL 0)
        message(STATUS "qcrs: enabling ssse3")
        add_definitions(-mssse3)
    else (MY_SSSE3_SUPPORTED_RET EQUAL 0)
		if(CMAKE_SYSTEM_PROCESSOR MATCHES arm)
			message(STATUS "qcrs: disabling ssse")
			add_definitions(-flax-vector-conversions -DLIBRS_USE_NONSSSE -DLIBRS_USE_SSE2)
		else (CMAKE_SYSTEM_PROCESSOR MATCHES arm)
			message(STATUS "qcrs: disabling ssse3")
			add_definitions(-msse2 -DLIBRS_USE_SSE2)
		endif (CMAKE_SYSTEM_PROCESSOR MATCHES arm)
    endif (MY_SSSE3_SUPPORTED_RET EQUAL 0)
endif (CMAKE_SYSTEM_PROCESSOR MATCHES x86_64)

if (CYGWIN)
    add_definitions(-flax-vector-conversions)
endif (CYGWIN)

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
