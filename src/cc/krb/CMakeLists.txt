#
# $Id$
#
# Created 2013/05/19
# Author: Mike Ovsiannikov
#
# Copyright 2013 Quantcast Corp.
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
KrbService.cc
KrbClient.cc
)

#
# Build a static and a dynamically linked libraries.  Both libraries
# should have the same root name, but installed in different places
#
add_library (qfskrb STATIC ${sources})
add_library (qfskrb-shared SHARED ${sources})
set_target_properties (qfskrb-shared PROPERTIES OUTPUT_NAME "qfskrb")
set_target_properties (qfskrb PROPERTIES OUTPUT_NAME "qfskrb")

#
# Since the objects have to be built twice, set this up so they don't
# clobber each other.

set_target_properties (qfskrb PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (qfskrb-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

target_link_libraries (qfskrb        ${KRB5_LIBRARIES})
target_link_libraries (qfskrb-shared ${KRB5_LIBRARIES})

add_executable (qfskrbtest krbtest_main.cc)

set (exe_files qfskrbtest)

foreach (exe_file ${exe_files})
    if (USE_STATIC_LIB_LINKAGE)
        target_link_libraries (${exe_file}
            qfskrb
            qcdio
        )
        add_dependencies (${exe_file}
            qfskrb
            qcdio
        )
    else (USE_STATIC_LIB_LINKAGE)
        target_link_libraries (${exe_file}
            qfskrb-shared
            qcdio-shared
        )
        add_dependencies (${exe_file}
            qfskrb-shared
            qcdio-shared
        )
    endif (USE_STATIC_LIB_LINKAGE)
endforeach (exe_file)

install (TARGETS qfskrb qfskrb-shared
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib/static)

