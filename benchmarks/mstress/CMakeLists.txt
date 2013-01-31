# $Id$
#
# Copyright 2012 Quantcast Corp.
#
# This file is part of Quantcast File System (QFS).
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
# Build the C++ mstress benchmark
#

add_executable (mstress_client mstress_client.cc)
IF (USE_STATIC_LIB_LINKAGE)
    add_dependencies (mstress_client kfsClient)
    target_link_libraries (mstress_client kfsClient qcdio pthread)
ELSE (USE_STATIC_LIB_LINKAGE)
    add_dependencies (mstress_client kfsClient-shared)
    target_link_libraries (mstress_client kfsClient-shared qcdio-shared pthread)
ENDIF (USE_STATIC_LIB_LINKAGE)

IF (NOT APPLE)
    target_link_libraries (rt)
ENDIF (NOT APPLE)

install (TARGETS mstress_client
        RUNTIME DESTINATION bin/benchmarks)
