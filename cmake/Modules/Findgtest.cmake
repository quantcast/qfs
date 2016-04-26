# pull in gtest as an external project from the internet -- debian based
# systems don't package libgtest (static libraries) any longer and only have
# the header files.
find_package(Threads REQUIRED)

include(ExternalProject)
ExternalProject_Add(
    gtest
    # we provide our own command here, since default curl binary used by Cmake might not support SSL.
    DOWNLOAD_COMMAND /bin/sh -c "wget https://github.com/google/googletest/archive/release-1.7.0.zip && unzip release-1.7.0.zip &&  mv googletest-release-1.7.0/* gtest/ && rm -rf googletest-release-1.7.0"
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/gtest
    INSTALL_COMMAND "" # Disable install step
)

add_library(libgtest IMPORTED STATIC GLOBAL)
add_dependencies(libgtest gtest)

# Set gtest properties
ExternalProject_Get_Property(gtest source_dir binary_dir)
set_target_properties(libgtest PROPERTIES
    "IMPORTED_LOCATION" "${binary_dir}/libgtest.a"
    "INTERFACE_LINK_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
)

include_directories("${source_dir}/include")
