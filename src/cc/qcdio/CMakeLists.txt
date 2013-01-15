#
# $Id$
#
# Created 2008/12/11
# Author: Mike Ovsiannikov
#
# Copyright 2008-2009 Quantcast Corp.
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
QCDiskQueue.cc
QCFdPoll.cc
QCIoBufferPool.cc
QCMutex.cc
QCThread.cc
QCUtils.cc
)

string(TOUPPER QC_OS_NAME_${CMAKE_SYSTEM_NAME} QC_OS_NAME)
add_definitions (-D_GNU_SOURCE -D${QC_OS_NAME} -DQC_USE_BOOST)

#
# Build a static and a dynamically linked libraries.  Both libraries
# should have the same root name, but installed in different places
#
add_library (qcdio STATIC ${sources})
add_library (qcdio-shared SHARED ${sources})
set_target_properties (qcdio-shared PROPERTIES OUTPUT_NAME "qfs_qcdio")
set_target_properties (qcdio PROPERTIES OUTPUT_NAME "qfs_qcdio")

#
# Since the objects have to be built twice, set this up so they don't
# clobber each other.

set_target_properties (qcdio PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (qcdio-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)
target_link_libraries (qcdio ${Boost_LIBRARIES})

if (NOT Boost_USE_STATIC_LIBS)
    target_link_libraries (qcdio-shared ${Boost_LIBRARIES})
endif (NOT Boost_USE_STATIC_LIBS)

install (TARGETS qcdio qcdio-shared
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

