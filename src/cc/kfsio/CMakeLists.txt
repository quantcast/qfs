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
    Acceptor.cc
    checksum.cc
    Globals.cc
    IOBuffer.cc
    NetConnection.cc
    NetErrorSimulator.cc
    NetManager.cc
    TcpSocket.cc
    ZlibInflate.cc
    KfsCallbackObj.cc
    SslFilter.cc
    ClientAuthContext.cc
    DelegationToken.cc
    Base64.cc
    CryptoKeys.cc
    ChunkAccessToken.cc
    PrngIsaac64.cc
)

add_library (kfsIO STATIC ${sources})
add_library (kfsIO-shared SHARED ${sources})
set_target_properties (kfsIO PROPERTIES OUTPUT_NAME "qfs_io")
set_target_properties (kfsIO-shared PROPERTIES OUTPUT_NAME "qfs_io")

set_target_properties (kfsIO PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (kfsIO-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (kfsIO
    qcdio
    kfsCommon
    qfskrb
)
add_dependencies (kfsIO-shared
    qcdio-shared
    kfsCommon-shared
    qfskrb-shared
)
target_link_libraries (kfsIO
    qcdio
    kfsCommon
    qfskrb
    z
    ${OPENSSL_LIBRARIES}
)
target_link_libraries (kfsIO-shared
    qcdio-shared
    kfsCommon-shared
    qfskrb-shared
    z
    ${OPENSSL_LIBRARIES}
)

if (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    target_link_libraries (kfsIO nsl socket resolv)
    target_link_libraries (kfsIO-shared nsl socket resolv)
endif (CMAKE_SYSTEM_NAME STREQUAL "SunOS")

install (TARGETS kfsIO kfsIO-shared
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)
