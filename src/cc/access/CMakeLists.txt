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
add_library (qfs_access SHARED qfs_access_jni.cc)
add_dependencies (qfs_access kfsClient-shared)
target_link_libraries (qfs_access kfsClient-shared)
if (NOT APPLE)
  target_link_libraries(qfs_access rt)
endif (NOT APPLE)

IF (CMAKE_SYSTEM_NAME STREQUAL "SunOS") 
   # On Solaris, cmake uses gcc to link rather than g++.  This
   #  prevents linking with libstdc++ which prevents Java from loading
   #  qfs_access.  To fix, set the compiler to be C++
   set (CMAKE_C_COMPILER "${CMAKE_CXX_COMPILER}")
ENDIF (CMAKE_SYSTEM_NAME STREQUAL "SunOS")

#
install (TARGETS qfs_access
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib-static)
