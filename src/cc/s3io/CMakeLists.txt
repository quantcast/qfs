#
# $Id$
#
# Created 2015/09/09
# Author: Mike Ovsiannikov
#
# Copyright 2015,2016 Quantcast Corporation. All rights reserved.
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

set (sources
s3ion.cc
)

#
# Build a static and a dynamically linked libraries.  Both libraries
# should have the same root name, but installed in different places
#
add_library (qfss3io STATIC ${sources})
add_library (qfss3io-shared SHARED ${sources})
set_target_properties (qfss3io-shared PROPERTIES OUTPUT_NAME "qfss3io")
set_target_properties (qfss3io PROPERTIES OUTPUT_NAME "qfss3io")

#
# Since the objects have to be built twice, set this up so they don't
# clobber each other.
set_target_properties (qfss3io PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (qfss3io-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

target_link_libraries (qfss3io
    kfsIO
)
target_link_libraries (qfss3io-shared
    kfsIO-shared
)

install (TARGETS qfss3io qfss3io-shared
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib/static)
