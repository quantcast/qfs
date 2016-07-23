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

# Include all tests in their respective subdirectories into libqfstest
set(test_sources
    integtest.cc
    integtest_main.cc

    environments/MetaserverEnvironment.cc
    environments/ChunkserverEnvironment.cc

    common/Test_T.cc
)

set(test_binary test.t)
add_executable(${test_binary} ${test_sources})
target_link_libraries(${test_binary} libgtest)

if(USE_STATIC_LIB_LINKAGE)
    add_dependencies(${test_binary} kfsCommon)
    target_link_libraries(${test_binary} kfsCommon)
else()
    add_dependencies(${test_binary} kfsCommon-shared)
    target_link_libraries(${test_binary} kfsCommon-shared)
endif()

# cmake and centos <= 6 try to use the libc pthreads and set
# CMAKE_THREAD_LIBS_INIT to be empty. This doesn't work with the libgtest
# library which requires pthread at link time. Add it here directly. This isn't
# great and we need to find a better way to do it in the future but this is good
# enough for now, unfortunately.
target_link_libraries(${test_binary} pthread)

install(TARGETS ${test_binary}
    RUNTIME DESTINATION bin/tests
)
