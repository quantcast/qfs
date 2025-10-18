# $Id$
#
# Copyright 2012-2025 Quantcast Corporation. All rights reserved.
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

add_executable(mstress_client EXCLUDE_FROM_ALL mstress_client.cc)

if(USE_STATIC_LIB_LINKAGE)
    add_dependencies(mstress_client kfsClient)
    target_link_libraries(mstress_client kfsClient)
else(USE_STATIC_LIB_LINKAGE)
    add_dependencies(mstress_client kfsClient-shared)
    target_link_libraries(mstress_client kfsClient-shared)
endif()

if(NOT DEFINED MSTRESS_HADOOP_VERSION)
    if(DEFINED ENV{MSTRESS_HADOOP_VERSION})
        set(MSTRESS_HADOOP_VERSION $ENV{MSTRESS_HADOOP_VERSION})
    else()
        set(MSTRESS_HADOOP_VERSION 3.4.1)
    endif()
endif()

add_custom_command(
    OUTPUT mstress.jar mstress-jar-with-dependencies.jar
    COMMAND sh ${CMAKE_CURRENT_SOURCE_DIR}/../../src/java/javabuild.sh
    -d ${CMAKE_CURRENT_SOURCE_DIR}
    -v ${CMAKE_CURRENT_SOURCE_DIR}/../../src/cc/common/buildversgit.sh
    --
    -DbuildDirectory=${CMAKE_CURRENT_BINARY_DIR}
    -Dhadoop.release.version=${MSTRESS_HADOOP_VERSION}
    package
    DEPENDS src/main/java/com/quantcast/qfs/mstress/MStress_Client.java pom.xml
    COMMENT "The HDFS mstress client bundled as a jar."
    VERBATIM
)

add_custom_target(mstress DEPENDS
    mstress_client mstress.jar mstress-jar-with-dependencies.jar)

set(mstress_scripts
    mstress_plan.py
    mstress_run.py
    mstress.py
    mstress_cleanup.py
    mstress_install.sh
)

foreach(script ${mstress_scripts})
    add_custom_command(
        OUTPUT ${script}
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/${script}
        ${CMAKE_CURRENT_BINARY_DIR}/
    )
endforeach(script)

add_custom_target(
    mstress-scripts
    DEPENDS ${mstress_scripts}
    COMMENT "Copy mstress scripts to build directory."
)

add_custom_target(
    mstress-tarball
    COMMAND cd .. && ${CMAKE_COMMAND} -E tar czvf mstress.tgz
    mstress/mstress_client
    mstress/mstress.jar
    mstress/mstress-jar-with-dependencies.jar
    mstress/*.py
    mstress/*.sh
    DEPENDS mstress
    COMMENT "Bundle mstress files in a tar archive."
)
add_dependencies(mstress-tarball mstress-scripts)

if(${CMAKE_MAJOR_VERSION} LESS 2)
elseif(${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} LESS 8)
elseif(${CMAKE_MAJOR_VERSION} EQUAL 2 AND
    ${CMAKE_MINOR_VERSION} EQUAL 8 AND ${CMAKE_PATCH_VERSION} LESS 12)
    set(cmake_use_target_property ON)
endif(${CMAKE_MAJOR_VERSION} LESS 2)

if(${cmake_use_target_property})
    get_target_property(metaserver_location metaserver LOCATION)
    get_target_property(chunkserver_location chunkserver LOCATION)
else(${cmake_use_target_property})
    set(metaserver_location "$<TARGET_FILE:metaserver>")
    set(chunkserver_location "$<TARGET_FILE:chunkserver>")
endif(${cmake_use_target_property})

add_custom_target(
    mstress-bootstrap
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/bin/
    COMMAND ${CMAKE_COMMAND} -E copy ${metaserver_location}
    ${CMAKE_CURRENT_BINARY_DIR}/bin/
    COMMAND ${CMAKE_COMMAND} -E copy ${chunkserver_location}
    ${CMAKE_CURRENT_BINARY_DIR}/bin/
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${KFS_DIR_PREFIX}/webui
    ${CMAKE_CURRENT_BINARY_DIR}/webui
    COMMAND ${CMAKE_COMMAND} -E copy
    ${KFS_DIR_PREFIX}/examples/sampleservers/sample_setup.py
    ${CMAKE_CURRENT_BINARY_DIR}/setup.py
    COMMAND cd .. && ${CMAKE_COMMAND} -E tar czvf mstress-bootstrap.tgz
    mstress/mstress_client
    mstress/mstress.jar
    mstress/mstress-jar-with-dependencies.jar
    mstress/*.py
    mstress/*.sh
    mstress/bin/metaserver
    mstress/bin/chunkserver
    mstress/webui
    DEPENDS mstress metaserver chunkserver
    COMMENT "Bundle mstress files along with QFS binaries."
)
add_dependencies(mstress-bootstrap mstress-scripts)

set(additional_files_to_clean
    ../mstress.tgz
    ../mstress-bootstrap.tgz
    mstress.jar
    mstress-jar-with-dependencies.jar
    classes
    bin
    webui
    setup.py
    ${mstress_scripts}
)
set_directory_properties(PROPERTIES
    ADDITIONAL_MAKE_CLEAN_FILES "${additional_files_to_clean}"
)
