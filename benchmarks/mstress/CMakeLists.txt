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

add_executable (mstress_client EXCLUDE_FROM_ALL mstress_client.cc)

IF (USE_STATIC_LIB_LINKAGE)
    add_dependencies (mstress_client kfsClient)
    target_link_libraries (mstress_client kfsClient qcdio pthread)
ELSE (USE_STATIC_LIB_LINKAGE)
    add_dependencies (mstress_client kfsClient-shared)
    target_link_libraries (mstress_client kfsClient-shared qcdio-shared pthread)
ENDIF (USE_STATIC_LIB_LINKAGE)

IF (NOT APPLE)
    target_link_libraries (mstress_client rt)
ENDIF (NOT APPLE)

add_custom_command (
    OUTPUT mstress.jar
    COMMAND ant -f ${CMAKE_CURRENT_SOURCE_DIR}/build.xml
    DEPENDS MStress_Client.java
    COMMENT The HDFS mstress client bundled as a jar.
    VERBATIM
)

add_custom_target (mstress DEPENDS mstress_client mstress.jar)

set (mstress_scripts
    mstress_plan.py
    mstress_run.py
    mstress.py
    mstress_cleanup.py
    mstress_install.sh
)

foreach (script ${mstress_scripts})
    add_custom_command (
        OUTPUT ${script}
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/${script} ${CMAKE_CURRENT_BINARY_DIR}/
    )
endforeach(script)

add_custom_target (
    mstress-scripts
    DEPENDS ${mstress_scripts}
    COMMENT Copy mstress scripts to build directory.
)

add_custom_target (
    mstress-tarball
    COMMAND cd .. && ${CMAKE_COMMAND} -E tar czvf mstress.tgz
                mstress/mstress_client
                mstress/mstress.jar
                mstress/*.py
                mstress/*.sh
    DEPENDS mstress_client mstress.jar
    COMMENT Bundle mstress files in a tar archive.
)
add_dependencies(mstress-tarball mstress-scripts)

get_property (metaserver_location TARGET metaserver PROPERTY LOCATION)
get_property (chunkserver_location TARGET chunkserver PROPERTY LOCATION)
add_custom_target (
    mstress-bootstrap
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/bin/
    COMMAND ${CMAKE_COMMAND} -E copy ${metaserver_location} ${CMAKE_CURRENT_BINARY_DIR}/bin/
    COMMAND ${CMAKE_COMMAND} -E copy ${chunkserver_location} ${CMAKE_CURRENT_BINARY_DIR}/bin/
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/webui ${CMAKE_CURRENT_BINARY_DIR}/webui
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/examples/sampleservers/sample_setup.py ${CMAKE_CURRENT_BINARY_DIR}/setup.py
    COMMAND cd .. && ${CMAKE_COMMAND} -E tar czvf mstress-bootstrap.tgz
                mstress/mstress_client
                mstress/mstress.jar
                mstress/*.py
                mstress/*.sh
                mstress/bin/metaserver
                mstress/bin/chunkserver
                mstress/webui
    DEPENDS mstress_client mstress.jar metaserver chunkserver
    COMMENT Bundle mstress files along with QFS binaries.
)
add_dependencies(mstress-bootstrap mstress-scripts)

set_directory_properties (PROPERTIES
    ADDITIONAL_MAKE_CLEAN_FILES "../mstress.tgz;mstress.jar;${mstress_scripts}"
)
