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
set (lib_src
    BufferedLogWriter.cc
    hsieh_hash.cc
    MsgLogger.cc
    Properties.cc
    time.cc
    kfsatomic.cc
    MemLock.cc
    RequestParser.cc
    rusage.cc
    nofilelimit.cc
    kfserrno.cc
)

# for the version file
set (VERSION_CC ${CMAKE_CURRENT_SOURCE_DIR}/Version.cc)
set_source_files_properties(${VERSION_CC} PROPERTIES GENERATED ON)

#
# Build both static/dynamic libraries.  Force the linking of all apps
# with a statically linked library.  Since kfsCommon is the symbol
# used everywhere, associate that symbol with the STATIC library.
#

add_custom_target (
    version ALL COMMAND sh ${CMAKE_CURRENT_SOURCE_DIR}/buildversgit.sh
        ${CMAKE_BUILD_TYPE}
        ${CMAKE_SOURCE_DIR}
        ${VERSION_CC}
        "boost: ${Boost_INCLUDE_DIRS} ${Boost_LIBRARIES}"
        "source dir: ${CMAKE_SOURCE_DIR}"
    VERBATIM
)

add_library (kfsCommon-shared SHARED ${lib_src} ${VERSION_CC})
add_library (kfsCommon STATIC ${lib_src} ${VERSION_CC})
add_dependencies (kfsCommon-shared version)
add_dependencies (kfsCommon version)
set_target_properties (kfsCommon PROPERTIES OUTPUT_NAME "qfs_common")
set_target_properties (kfsCommon-shared PROPERTIES OUTPUT_NAME "qfs_common")
set_target_properties (kfsCommon PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kfsCommon-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

target_link_libraries (kfsCommon qcdio pthread)
target_link_libraries (kfsCommon-shared qcdio-shared pthread)

#
install (TARGETS kfsCommon-shared kfsCommon
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)

install (FILES kfstypes.h kfsdecls.h DESTINATION include/kfs/common)
