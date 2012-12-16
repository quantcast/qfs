#
# $Id$
#
# Created 2006
# Author: Sriram Rao (Kosmix Corp)
#
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

#
# For the library take everything except the *_main.cc files
#
set (lib_srcs
AuditLog.cc
Checkpoint.cc
ChunkServer.cc
ChildProcessTracker.cc
ClientSM.cc
DiskEntry.cc
kfsops.cc
kfstree.cc
LayoutManager.cc
Logger.cc
meta.cc
MetaRequest.cc
NetDispatch.cc
Replay.cc
Restorer.cc
util.cc
)

add_library (kfsMeta STATIC ${lib_srcs})
set_target_properties (kfsMeta PROPERTIES OUTPUT_NAME "qfs_meta")
set_target_properties (kfsMeta PROPERTIES CLEAN_DIRECT_OUTPUT 1)

if (NOT USE_STATIC_LIB_LINKAGE)
    add_library (kfsMeta-shared SHARED ${lib_srcs} layoutmanager_instance.cc)
    set_target_properties (kfsMeta-shared PROPERTIES OUTPUT_NAME "qfs_meta")
    set_target_properties (kfsMeta-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)
    if (NOT Boost_USE_STATIC_LIBS)
      target_link_libraries(kfsMeta-shared kfsCommon-shared kfsIO-shared crypto)
    endif (NOT Boost_USE_STATIC_LIBS)
    install (kfsMeta-shared RUNTIME DESTINATION bin LIBRARY DESTINATION lib)
endif (NOT USE_STATIC_LIB_LINKAGE)

set (exe_files metaserver logcompactor filelister qfsfsck)
foreach (exe_file ${exe_files})
        add_executable (${exe_file} ${exe_file}_main.cc layoutmanager_instance.cc)
        if (USE_STATIC_LIB_LINKAGE)
           target_link_libraries (${exe_file} kfsMeta kfsIO kfsCommon qcdio pthread crypto)
           add_dependencies (${exe_file} kfsCommon kfsIO kfsMeta qcdio)
        else (USE_STATIC_LIB_LINKAGE)
           target_link_libraries (${exe_file} kfsMeta-shared kfsIO-shared kfsCommon-shared qcdio-shared pthread crypto)
           add_dependencies (${exe_file} kfsCommon-shared kfsIO-shared kfsMeta-shared qcdio-shared)
        endif (USE_STATIC_LIB_LINKAGE)
endforeach (exe_file)

if (NOT APPLE)
  target_link_libraries(kfsMeta rt)
  target_link_libraries(metaserver rt)
endif (NOT APPLE)

if (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
   # mtmalloc seemed to worsen metaserver startup time; it took
   # 4 mins for fsck to load checkpoint from WORM, where as 30 for metaserver.
   # So, switch to umem
   target_link_libraries(kfsMeta umem)
   target_link_libraries(metaserver umem)
endif (CMAKE_SYSTEM_NAME STREQUAL "SunOS")

#
# Install them
#
install (TARGETS ${exe_files} kfsMeta
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

