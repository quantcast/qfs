#
# $Id$
#
# Created 2016/03/31
#
# Copyright 2016-2017 Quantcast Corporation. All rights reserved.
#
# This file is part of Quantcast File System.
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

# gf-complete autoconf overrides CFGLAGS. Use CPPFLAGS to add compiler flags.
include(ExternalProject)
set(KFS_EXTERNAL_PROJECT_DIR ${KFS_DIR_PREFIX}/ext/)

if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set (Jfe_CONFIGURE_C_COMPILER cc)
else()
    set (Jfe_CONFIGURE_C_COMPILER ${CMAKE_C_COMPILER})
endif()

set(Gf_complete          "gf-complete")
set(Gf_complete_CPPFLAGS
    "-I${KFS_EXTERNAL_PROJECT_DIR}${Gf_complete}/include")
set(Gf_complete_CC        ${CMAKE_C_COMPILER})
set(Gf_complete_PREFIX    ${CMAKE_CURRENT_BINARY_DIR}/${Gf_complete})
ExternalProject_Add(Gf_complete_proj
    DOWNLOAD_COMMAND  ""
    SOURCE_DIR        ${KFS_EXTERNAL_PROJECT_DIR}${Gf_complete}
    CONFIGURE_COMMAND ${KFS_EXTERNAL_PROJECT_DIR}${Gf_complete}/configure
        CC=${Jfe_CONFIGURE_C_COMPILER}
        CPPFLAGS=${Gf_complete_CPPFLAGS}
        --enable-static=yes
        --enable-shared=yes
        --prefix=${Gf_complete_PREFIX}
    BUILD_COMMAND ${MAKE}
)
set(Gf_complete_INCLUDE ${Gf_complete_PREFIX}/include)
set(Gf_complete_LIB_DIR ${Gf_complete_PREFIX}/lib/)
set(Gf_complete_STATIC_LIB
    ${Gf_complete_LIB_DIR}${CMAKE_STATIC_LIBRARY_PREFIX}gf_complete${CMAKE_STATIC_LIBRARY_SUFFIX}
)
set(Gf_complete_SHARED_LIB
    ${Gf_complete_LIB_DIR}${CMAKE_SHARED_LIBRARY_PREFIX}gf_complete${CMAKE_SHARED_LIBRARY_SUFFIX}
)

set(Jerasure          "jerasure")
set(Jerasure_CPPFLAGS
    "-I${KFS_EXTERNAL_PROJECT_DIR}${Jerasure}/include -I${Gf_complete_INCLUDE}")
set(Jerasure_LDFLAGS  "-L${Gf_complete_LIB_DIR}")
set(Jerasure_PREFIX   ${CMAKE_CURRENT_BINARY_DIR}/${Jerasure})
ExternalProject_Add(Jerasure_proj
    DEPENDS           Gf_complete_proj
    DOWNLOAD_COMMAND  ""
    SOURCE_DIR        ${KFS_EXTERNAL_PROJECT_DIR}${Jerasure}
    CONFIGURE_COMMAND ${KFS_EXTERNAL_PROJECT_DIR}${Jerasure}/configure
        CC=${Jfe_CONFIGURE_C_COMPILER}
        CPPFLAGS=${Jerasure_CPPFLAGS}
        LDFLAGS=${Jerasure_LDFLAGS}
        --enable-static=yes
        --enable-shared=yes
        --prefix=${Jerasure_PREFIX}
    BUILD_COMMAND ${MAKE}
)
set(Jerasure_INCLUDE ${Jerasure_PREFIX}/include)
set(Jerasure_LIB_DIR ${Jerasure_PREFIX}/lib/)
set(Jerasure_STATIC_LIB
    ${Jerasure_LIB_DIR}${CMAKE_STATIC_LIBRARY_PREFIX}Jerasure${CMAKE_STATIC_LIBRARY_SUFFIX}
)
set(Jerasure_SHARED_LIB
    ${Jerasure_LIB_DIR}${CMAKE_SHARED_LIBRARY_PREFIX}Jerasure${CMAKE_SHARED_LIBRARY_SUFFIX}
)

install (FILES ${Gf_complete_STATIC_LIB} ${Jerasure_STATIC_LIB}
    DESTINATION lib/static
)
set(JERASURE_STATIC_LIBRARIES
    ${Jerasure_STATIC_LIB}
    ${Gf_complete_STATIC_LIB}
)
set(Jerasure_STATIC_LIB_SYSTEMS Darwin)
if(CMAKE_SYSTEM_NAME IN_LIST Jerasure_STATIC_LIB_SYSTEMS OR
        (CYGWIN AND NOT QFS_JERASURE_CYGWIN_USE_SHARED_LIBS))
    # For now do not use shared libs as libtool sets absolute library path in
    # these, and makes it difficult to use with $ORIGIN on Linux and possibly
    # BSD and/or @loader_path on Darvin, and on cygwin only static libs are
    # built.
    # The libraries objects are build with -fPIC and -DPIC flags and the same
    # object are used for both static and shared libs, threfore linking with
    # other shared library (qfs_client) should work.
    set(JERASURE_SHARED_LIBRARIES ${JERASURE_STATIC_LIBRARIES})
else()
    # Shared library are sym linked, install both sym link and the targets
    # by using pattern. Allow version suffix that follows library suffix.
    install (DIRECTORY  ${Gf_complete_LIB_DIR} ${Jerasure_LIB_DIR}
        LIBRARY DESTINATION lib
        USE_SOURCE_PERMISSIONS
        FILES_MATCHING PATTERN
            "${CMAKE_SHARED_LIBRARY_PREFIX}*${CMAKE_SHARED_LIBRARY_SUFFIX}*"
    )
    set(JERASURE_SHARED_LIBRARIES
        ${Jerasure_SHARED_LIB}
        ${Gf_complete_SHARED_LIB}
    )
endif()
