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

#
# For the library take everything except the *_main.cc files
#
set (lib_srcs
ChunkServerEmulator.cc
LayoutEmulator.cc
emulator_setup.cc
)

add_library (kfsEmulator STATIC ${lib_srcs})
set_target_properties (kfsEmulator PROPERTIES OUTPUT_NAME "qfs_emulator")
set_target_properties (kfsEmulator PROPERTIES CLEAN_DIRECT_OUTPUT 1)

if (NOT USE_STATIC_LIB_LINKAGE)
    add_library (kfsEmulator-shared SHARED ${lib_srcs})
    set_target_properties (kfsEmulator-shared PROPERTIES OUTPUT_NAME "qfs_emulator")
    set_target_properties (kfsEmulator-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)
    install (kfsEmulator-shared
            RUNTIME DESTINATION bin/emulator
            LIBRARY DESTINATION lib)
    if (NOT Boost_USE_STATIC_LIBS)
        target_link_libraries(kfsEmulator-shared kfsMeta-shared kfsCommon-shared kfsIO-shared)
    endif (NOT Boost_USE_STATIC_LIBS)
endif (NOT USE_STATIC_LIB_LINKAGE)

set (exe_files rebalanceplanner rebalanceexecutor replicachecker)
foreach (exe_file ${exe_files})
        add_executable (${exe_file} ${exe_file}_main.cc)
        if (USE_STATIC_LIB_LINKAGE)
           target_link_libraries (${exe_file} kfsEmulator kfsMeta kfsIO kfsCommon qcdio pthread crypto)
           add_dependencies (${exe_file} kfsEmulator kfsCommon kfsIO qcdio kfsMeta)
        else (USE_STATIC_LIB_LINKAGE)
           target_link_libraries (${exe_file} kfsEmulator-shared kfsMeta-shared kfsIO-shared kfsCommon-shared qcdio-shared pthread crypto)
           add_dependencies (${exe_file} kfsEmulator-shared kfsCommon-shared kfsIO-shared kfsMeta-shared qcdio-shared)
        endif (USE_STATIC_LIB_LINKAGE)
endforeach (exe_file)

if (NOT APPLE)
  target_link_libraries(kfsEmulator rt)
endif (NOT APPLE)

if (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
   target_link_libraries(kfsEmulator mtmalloc)
endif (CMAKE_SYSTEM_NAME STREQUAL "SunOS")

#
# Install them
#
install (TARGETS ${exe_files} kfsEmulator
        RUNTIME DESTINATION bin/emulator
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

