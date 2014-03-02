add_library (qfsc STATIC qfsc.cc)
add_library (qfsc-shared SHARED qfsc.cc)
set_target_properties (qfsc PROPERTIES OUTPUT_NAME "qfsc")
set_target_properties (qfsc-shared PROPERTIES OUTPUT_NAME "qfsc")
set_target_properties (qfsc PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (qfsc-shared PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_dependencies (qfsc
    kfsClient kfsCommon kfsIO qcdio kfsrs)
add_dependencies (qfsc-shared
    kfsClient-shared kfsCommon-shared kfsIO-shared qcdio-shared kfsrs-shared)

target_link_libraries (qfsc
    kfsClient kfsCommon kfsIO qcdio ${OPENSSL_LIBRARIES} kfsrs)
target_link_libraries (qfsc-shared
    kfsClient-shared kfsCommon-shared kfsIO-shared
    qcdio-shared kfsrs-shared ${OPENSSL_LIBRARIES})

if (NOT APPLE)
  target_link_libraries(qfsc rt)
endif (NOT APPLE)

add_executable (test-qfsc test-qfsc.c)
set_target_properties (test-qfsc PROPERTIES LINKER_LANGUAGE CXX)

if (USE_STATIC_LIB_LINKAGE)
    add_dependencies (test-qfsc
        qfsc kfsClient kfsCommon kfsIO qcdio qfsrs qfskrb)
    target_link_libraries (test-qfsc
        qfsc kfsClient kfsCommon kfsIO qcdio qfskrb ${OPENSSL_LIBRARIES} kfsrs pthread)
else (USE_STATIC_LIB_LINKAGE)
    add_dependencies (test-qfsc
        qfsc-shared kfsClient-shared kfsCommon-shared
        kfsIO-shared qcdio-shared kfsrs-shared qfskrb-shared)
    target_link_libraries (test-qfsc
        qfsc-shared kfsClient-shared kfsCommon-shared
        kfsIO-shared qcdio-shared kfsrs-shared qfskrb-shared ${OPENSSL_LIBRARIES} pthread)
endif (USE_STATIC_LIB_LINKAGE)

install (TARGETS qfsc qfsc-shared
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib/static)
install (FILES qfs.h DESTINATION include/kfs/c)

