#
# $Id$
#
# Author: Thilee Subramaniam
#
# Copyright 2012 Quantcast Corp.
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
# Build the C++ example binary
#

add_executable (kfssample kfssample_main.cc)

if (USE_STATIC_LIB_LINKAGE)
    target_link_libraries (kfssample kfsClient qcdio pthread)
    add_dependencies (kfssample kfsClient qcdio)
else (USE_STATIC_LIB_LINKAGE)
    target_link_libraries (kfssample kfsClient-shared qcdio-shared pthread)
    add_dependencies (kfssample kfsClient-shared qcdio-shared)
endif (USE_STATIC_LIB_LINKAGE)

if (NOT APPLE)
    target_link_libraries(kfssample rt)
endif (NOT APPLE)

install (TARGETS kfssample
        RUNTIME DESTINATION bin/examples)
