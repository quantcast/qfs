#
# $Id$
#
# Created 2006
# Author: Sriram Rao (Kosmix Corp)
#
# Copyright 2009-2012 Quantcast Corp.
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

add_executable (chunkserver
    chunkserver_main.cc
    AtomicRecordAppender.cc
    BufferManager.cc
    ChunkManager.cc
    ChunkServer.cc
    ClientManager.cc
    ClientSM.cc
    DiskIo.cc
    KfsOps.cc
    LeaseClerk.cc
    Logger.cc
    MetaServerSM.cc
    RemoteSyncSM.cc
    Replicator.cc
    utils.cc
    DirChecker.cc
    Chunk.cc
    ClientThread.cc
)
add_executable (chunkscrubber chunkscrubber_main.cc)

set (exe_files chunkserver chunkscrubber)

foreach (exe_file ${exe_files})
    if (USE_STATIC_LIB_LINKAGE)
        target_link_libraries (${exe_file}
            kfsClient
        )
        add_dependencies (${exe_file}
            kfsClient
        )
    else (USE_STATIC_LIB_LINKAGE)
        target_link_libraries (${exe_file}
            kfsClient-shared
        )
        add_dependencies (${exe_file}
            kfsClient-shared
        )
    endif (USE_STATIC_LIB_LINKAGE)
endforeach (exe_file)

if (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    target_link_libraries(chunkserver umem)
endif (CMAKE_SYSTEM_NAME STREQUAL "SunOS")

#
# Install them
#
install (TARGETS ${exe_files}
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib/static)
