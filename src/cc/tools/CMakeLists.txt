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

# Build the tools library
set (lib_srcs
    chunkfileinfo.cc
    kfsappend.cc
    kfscd.cc
    kfschangereplication.cc
    kfschgrp.cc
    kfschmod.cc
    kfschown.cc
    kfscp.cc
    kfsfstat.cc
    kfsls.cc
    kfsmkdirs.cc
    kfsmv.cc
    kfsrm.cc
    kfsrmdir.cc
    kfspwd.cc
    monutils.cc
    utils.cc
)

add_library (tools STATIC ${lib_srcs})
add_library (tools-shared SHARED ${lib_srcs})
set_target_properties (tools PROPERTIES OUTPUT_NAME "kfs_tools")
set_target_properties (tools-shared PROPERTIES OUTPUT_NAME "kfs_tools")
set_target_properties (tools PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (tools-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (tools kfsClient)
add_dependencies (tools-shared kfsClient-shared)

set (exe_files
cptokfs
cpfromkfs
kfsshell
kfstoggleworm
kfsput
kfsstats
kfsfileenum
kfsdataverify
kfsretire
kfscat
kfsping
kfstool
)

if (APPLE OR CYGWIN)
   target_link_libraries(tools-shared kfsClient-shared kfsCommon-shared)
endif (APPLE OR CYGWIN)

#
# Every executable depends on its namesake source with _main.cc
#
foreach (exe_file ${exe_files})
        add_executable (${exe_file} ${exe_file}_main.cc)
        if (USE_STATIC_LIB_LINKAGE)
                add_dependencies (${exe_file} tools)
                target_link_libraries (${exe_file} tools kfsClient qcdio pthread)
        else (USE_STATIC_LIB_LINKAGE)
                add_dependencies (${exe_file} tools-shared)
                target_link_libraries (${exe_file} tools-shared kfsClient-shared qcdio-shared pthread)
        endif (USE_STATIC_LIB_LINKAGE)
endforeach (exe_file)

#
install (TARGETS ${exe_files} tools tools-shared
        RUNTIME DESTINATION bin/tools
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)
