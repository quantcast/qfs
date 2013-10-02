add_library (qfsc STATIC qfsc.cc)
add_library (qfsc-shared SHARED qfsc.cc)
set_target_properties (qfsc PROPERTIES OUTPUT_NAME "qfsc")
set_target_properties (qfsc-shared PROPERTIES OUTPUT_NAME "qfsc")

add_dependencies (qfsc kfsClient)
add_dependencies (qfsc-shared kfsClient-shared)

target_link_libraries(qfsc kfsClient kfsCommon kfsIO qcdio crypto kfsrs)
target_link_libraries(qfsc-shared kfsClient-shared)

if (NOT APPLE)
  target_link_libraries(qfsc rt)
endif (NOT APPLE)

install (TARGETS qfsc qfsc-shared
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib/static)
install (FILES qfs.h DESTINATION include/kfs/c)

add_executable (test-qfsc test-qfsc.c)
set_target_properties (test-qfsc PROPERTIES LINKER_LANGUAGE CXX)
if (USE_STATIC_LIB_LINKAGE)
        add_dependencies (test-qfsc qfsc)
        target_link_libraries (test-qfsc qfsc kfsClient qcdio pthread)
else (USE_STATIC_LIB_LINKAGE)
        add_dependencies (test-qfsc qfsc-shared)
        target_link_libraries (test-qfsc qfsc-shared kfsClient-shared qcdio-shared pthread)
endif (USE_STATIC_LIB_LINKAGE)
install (TARGETS test-qfsc
        RUNTIME DESTINATION bin/tests)
