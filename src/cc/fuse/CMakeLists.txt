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
# Build the fuse binary; build it only on demand
#

include_directories(${FUSE_INCLUDE_DIRS})
add_executable (qfs_fuse kfs_fuse_main.cc)
IF(DEFINED FUSE_LIBRARY_DIR)
    link_directories(${FUSE_LIBRARY_DIR})
ENDIF(DEFINED FUSE_LIBRARY_DIR)
target_link_libraries (qfs_fuse kfsClient qcdio pthread ${FUSE_LIBRARIES})

#
install (TARGETS qfs_fuse
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib-static)
