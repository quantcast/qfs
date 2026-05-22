#
# $Id$
#
# Created 2026/05/22
#
# Copyright 2026 Quantcast Corporation. All rights reserved.
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

# Find OpenSSL headers and libraries.
#
# This project still supports old CMake releases. CMake 3.5 cannot parse the
# OpenSSL 3 version macros in opensslv.h, which can make find_package(OpenSSL)
# fail even when OPENSSL_ROOT_DIR points to a valid OpenSSL 3 installation.
#
# Result variables:
#   OPENSSL_FOUND
#   OPENSSL_INCLUDE_DIR
#   OPENSSL_INCLUDE_DIRS
#   OPENSSL_SSL_LIBRARY
#   OPENSSL_CRYPTO_LIBRARY
#   OPENSSL_LIBRARIES
#   OPENSSL_VERSION

if(OPENSSL_ROOT_DIR)
    find_path(OPENSSL_INCLUDE_DIR openssl/ssl.h
        PATHS "${OPENSSL_ROOT_DIR}"
        PATH_SUFFIXES include
        NO_DEFAULT_PATH
    )
    find_library(OPENSSL_SSL_LIBRARY NAMES ssl ssleay32 ssleay32MD
        PATHS "${OPENSSL_ROOT_DIR}"
        PATH_SUFFIXES lib64 lib
        NO_DEFAULT_PATH
    )
    find_library(OPENSSL_CRYPTO_LIBRARY NAMES crypto libeay32 libeay32MD
        PATHS "${OPENSSL_ROOT_DIR}"
        PATH_SUFFIXES lib64 lib
        NO_DEFAULT_PATH
    )
endif()

find_path(OPENSSL_INCLUDE_DIR openssl/ssl.h
    PATH_SUFFIXES include
)
find_library(OPENSSL_SSL_LIBRARY NAMES ssl ssleay32 ssleay32MD
    PATH_SUFFIXES lib64 lib
)
find_library(OPENSSL_CRYPTO_LIBRARY NAMES crypto libeay32 libeay32MD
    PATH_SUFFIXES lib64 lib
)

set(OPENSSL_LIBRARIES ${OPENSSL_SSL_LIBRARY} ${OPENSSL_CRYPTO_LIBRARY})
set(OPENSSL_INCLUDE_DIRS ${OPENSSL_INCLUDE_DIR})

if(OPENSSL_INCLUDE_DIR AND EXISTS "${OPENSSL_INCLUDE_DIR}/openssl/opensslv.h")
    file(STRINGS "${OPENSSL_INCLUDE_DIR}/openssl/opensslv.h"
        _OPENSSL_VERSION_MAJOR_STR
        REGEX "^#[\t ]*define[\t ]+OPENSSL_VERSION_MAJOR[\t ]+[0-9]+")
    if(_OPENSSL_VERSION_MAJOR_STR)
        file(STRINGS "${OPENSSL_INCLUDE_DIR}/openssl/opensslv.h"
            _OPENSSL_VERSION_MINOR_STR
            REGEX "^#[\t ]*define[\t ]+OPENSSL_VERSION_MINOR[\t ]+[0-9]+")
        file(STRINGS "${OPENSSL_INCLUDE_DIR}/openssl/opensslv.h"
            _OPENSSL_VERSION_PATCH_STR
            REGEX "^#[\t ]*define[\t ]+OPENSSL_VERSION_PATCH[\t ]+[0-9]+")
        string(REGEX REPLACE "^.*OPENSSL_VERSION_MAJOR[\t ]+([0-9]+).*$"
            "\\1" _OPENSSL_VERSION_MAJOR "${_OPENSSL_VERSION_MAJOR_STR}")
        string(REGEX REPLACE "^.*OPENSSL_VERSION_MINOR[\t ]+([0-9]+).*$"
            "\\1" _OPENSSL_VERSION_MINOR "${_OPENSSL_VERSION_MINOR_STR}")
        string(REGEX REPLACE "^.*OPENSSL_VERSION_PATCH[\t ]+([0-9]+).*$"
            "\\1" _OPENSSL_VERSION_PATCH "${_OPENSSL_VERSION_PATCH_STR}")
        set(OPENSSL_VERSION
            "${_OPENSSL_VERSION_MAJOR}.${_OPENSSL_VERSION_MINOR}.${_OPENSSL_VERSION_PATCH}")
    else()
        file(STRINGS "${OPENSSL_INCLUDE_DIR}/openssl/opensslv.h"
            _OPENSSL_VERSION_NUMBER_STR
            REGEX "^#[\t ]*define[\t ]+OPENSSL_VERSION_NUMBER[\t ]+0x[0-9a-fA-F]+.*")
        if(_OPENSSL_VERSION_NUMBER_STR)
            string(REGEX REPLACE
                "^.*OPENSSL_VERSION_NUMBER[\t ]+0x([0-9a-fA-F])([0-9a-fA-F][0-9a-fA-F])([0-9a-fA-F][0-9a-fA-F]).*$"
                "\\1;0x\\2;0x\\3" _OPENSSL_VERSION_LIST
                "${_OPENSSL_VERSION_NUMBER_STR}")
            list(GET _OPENSSL_VERSION_LIST 0 _OPENSSL_VERSION_MAJOR)
            list(GET _OPENSSL_VERSION_LIST 1 _OPENSSL_VERSION_MINOR_HEX)
            list(GET _OPENSSL_VERSION_LIST 2 _OPENSSL_VERSION_FIX_HEX)
            math(EXPR _OPENSSL_VERSION_MINOR "${_OPENSSL_VERSION_MINOR_HEX}")
            math(EXPR _OPENSSL_VERSION_FIX "${_OPENSSL_VERSION_FIX_HEX}")
            set(OPENSSL_VERSION
                "${_OPENSSL_VERSION_MAJOR}.${_OPENSSL_VERSION_MINOR}.${_OPENSSL_VERSION_FIX}")
        endif()
    endif()
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(OpenSSL
    REQUIRED_VARS OPENSSL_SSL_LIBRARY OPENSSL_CRYPTO_LIBRARY OPENSSL_INCLUDE_DIR
    VERSION_VAR OPENSSL_VERSION
)

mark_as_advanced(
    OPENSSL_INCLUDE_DIR
    OPENSSL_SSL_LIBRARY
    OPENSSL_CRYPTO_LIBRARY
)
