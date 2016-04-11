# gf-complete autoconf overrides CFGLAGS. Use CPPFLAGS to add compiler flags.
include(ExternalProject)
set(KFS_EXTERNAL_PROJECT_DIR ${KFS_DIR_PREFIX}/ext/)

set(Gf_complete          "gf-complete")
set(Gf_complete_CPPFLAGS
    "-I${KFS_EXTERNAL_PROJECT_DIR}${Gf_complete}/include")
set(Gf_complete_CC        ${CMAKE_C_COMPILER})
set(Gf_complete_PREFIX    ${CMAKE_CURRENT_BINARY_DIR}/${Gf_complete})
ExternalProject_Add(Gf_complete_proj
    DOWNLOAD_COMMAND  ""
    SOURCE_DIR        ${KFS_EXTERNAL_PROJECT_DIR}${Gf_complete}
    CONFIGURE_COMMAND ${KFS_EXTERNAL_PROJECT_DIR}${Gf_complete}/configure
        CC=${Gf_complete_CC}
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
set(Jerasure_CC       ${CMAKE_C_COMPILER})
set(Jerasure_PREFIX   ${CMAKE_CURRENT_BINARY_DIR}/${Jerasure})
ExternalProject_Add(Jerasure_proj
    DEPENDS           Gf_complete_proj
    DOWNLOAD_COMMAND  ""
    SOURCE_DIR        ${KFS_EXTERNAL_PROJECT_DIR}${Jerasure}
    CONFIGURE_COMMAND ${KFS_EXTERNAL_PROJECT_DIR}${Jerasure}/configure
        CC=${Jerasure_CC}
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
if(CYGWIN AND NOT QFS_JERASURE_CYGWIN_USE_SHARED_LIBS)
    # It appears that on cygwin only static libs are built, and it is
    # possible to link client library dll against them.
    set(JERASURE_SHARED_LIBRARIES ${JERASURE_STATIC_LIBRARIES})
else()
    # Shared library are sym linked, install both sym link and the targets
    # by using pattern. Allow version suffix that follows library suffix.
    install (DIRECTORY  ${Gf_complete_LIB_DIR} ${Jerasure_LIB_DIR}
        DESTINATION lib
        USE_SOURCE_PERMISSIONS
        FILES_MATCHING PATTERN
            "${CMAKE_SHARED_LIBRARY_PREFIX}*${CMAKE_SHARED_LIBRARY_SUFFIX}*"
    )
    set(JERASURE_SHARED_LIBRARIES
        ${Jerasure_SHARED_LIB}
        ${Gf_complete_SHARED_LIB}
    )
endif()
