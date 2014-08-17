#
# $Id$
#
# Created 2006
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

# Take all the .cc files and build a library out of them
set (sources
    KfsOps.cc
    FileOpener.cc
    KfsClient.cc
    KfsNetClient.cc
    KfsProtocolWorker.cc
    KfsRead.cc
    KfsWrite.cc
    RSStriper.cc
    Reader.cc
    Path.cc
    utils.cc
    WriteAppender.cc
    Writer.cc
    kfsglob.cc
    KfsAttr.cc
    ECMethod.cc
    QCECMethod.cc
    ECMethodJerasure.cc
)

#
# Build a static and a dynamically linked libraries.  Both libraries
# should have the same root name, but installed in different places
#
add_library (kfsClient STATIC ${sources})
add_library (kfsClient-shared SHARED ${sources})
set_target_properties (kfsClient PROPERTIES OUTPUT_NAME "qfs_client")
set_target_properties (kfsClient-shared PROPERTIES OUTPUT_NAME "qfs_client")

#
# Since the objects have to be built twice, set this up so they don't
# clobber each other.

set_target_properties (kfsClient PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kfsClient-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (kfsClient
    kfsIO
    kfsrs
)
add_dependencies (kfsClient-shared
    kfsIO-shared
    kfsrs-shared
)
if (NOT QFS_OMIT_JERASURE)
    include_directories (
        ${Gf_complete_INCLUDE}
        ${Jerasure_INCLUDE}
        ${Jerasure_INCLUDE}/jerasure
    )
    add_dependencies(kfsClient        Jerasure_proj)
    add_dependencies(kfsClient-shared Jerasure_proj)
endif (NOT QFS_OMIT_JERASURE)

# get everthing into one
target_link_libraries (kfsClient
    kfsIO
    kfsrs
    ${JERASURE_STATIC_LIBRARIES}
)
target_link_libraries (kfsClient-shared
    kfsIO-shared
    kfsrs-shared
    ${JERASURE_SHARED_LIBRARIES}
)

install (TARGETS kfsClient kfsClient-shared
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib/static)

install (FILES KfsAttr.h KfsClient.h DESTINATION include/kfs)
