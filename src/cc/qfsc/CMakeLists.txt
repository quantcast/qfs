#
# $Id$
#
# Created 2013/06/17
# Author: Stephen J Day
#
# Copyright 2013-2014 Quantcast Corp.
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

add_library (qfsc STATIC qfsc.cc)
add_library (qfsc-shared SHARED qfsc.cc)
set_target_properties (qfsc PROPERTIES OUTPUT_NAME "qfsc")
set_target_properties (qfsc-shared PROPERTIES OUTPUT_NAME "qfsc")
set_target_properties (qfsc PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (qfsc-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (qfsc
    kfsClient
)
add_dependencies (qfsc-shared
    kfsClient-shared
)
target_link_libraries (qfsc
    kfsClient
)
target_link_libraries (qfsc-shared
    kfsClient-shared
)

add_executable (test-qfsc test-qfsc.c)
set_target_properties (test-qfsc PROPERTIES LINKER_LANGUAGE CXX)

if (USE_STATIC_LIB_LINKAGE)
    add_dependencies (test-qfsc qfsc)
    target_link_libraries (test-qfsc qfsc)
else (USE_STATIC_LIB_LINKAGE)
    add_dependencies (test-qfsc qfsc-shared)
    target_link_libraries (test-qfsc qfsc-shared)
endif (USE_STATIC_LIB_LINKAGE)

install (TARGETS qfsc qfsc-shared
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib/static)
install (FILES qfs.h DESTINATION include/kfs/c)

