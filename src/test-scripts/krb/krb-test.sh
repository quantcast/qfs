#!/bin/sh
#
# $Id$
#
# Created 2025
# Author: Mike Ovsiannikov
#
# Copyright 2025 Quantcast Corporation. All rights reserved.
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

# Kerberos setup and test script for QFS Kerberos authentication test.

krb5_test() {
    local build=0
    local test_dir=$PWD/qfstest/krb-test
    local test_program=$PWD/src/cc/krb/qfskrbtest
    local krb5_config=$test_dir/krb5.conf
	local krb_env_file=$test_dir/krb.env
    local openssl_config=$test_dir/openssl.conf
    local stop_file=$test_dir/stop
    local start_file=$test_dir/start
    local log_file=$test_dir/log
    local krb5_realm=QFS.TEST
    local ker5_port= # random port
    local my_dir=$(dirname -- "$0") || return 1

    while [ $# -gt 0 ]; do
        case "$1" in
        --build | -b)
            build=1
            shift
            ;;
        --)
            shift
            break
            ;;
        -h | --help)
            cat <<EOF
Usage: $0 [--build| -b] [--] <cmake arguments>
        --build| -b: build QFS Kerberos test program
        --: pass remaining arguments to cmake
        -h|--help: show this help message
To build QFS with Heimdal Kerberos support on Mac OS set the KRB5_PREFIX
cmake argument to the path to the Heimdal Kerberos installation.
For example:
$0 --build -- -D KRB5_PREFIX=/usr/local/opt/heimdal/bin
or to build with Krb5 Kerberos support on Mac OS set the
$0 --build -- -D KRB5_PREFIX=/usr/local/opt/krb5/bin
or put the Kerberos binary directory in the PATH environment variable.
For example:
PATH=/usr/local/opt/heimdal/bin:\$PATH $0 --build
EOF
            return 0
            ;;
        *)
            echo "unsupported option: $1"
            return 1
            ;;
        esac
    done

    set -e

    if [ $build -ne 0 -o ! -x "$test_program" ]; then
        cmake --fresh ${1:+"$@"} "$my_dir/../../.."
        cmake --build . --parallel --clean-first \
            --target "$(basename -- "$test_program")"
    fi

    local container_name=qfs-krb-test-$(
        awk 'BEGIN { srand(); print int(rand() * 1e10) int(rand() * 1e10); }'
    )
    # Build the test container:
    docker build -t "$container_name" -f "$my_dir/Dockerfile.krbtest" "$my_dir"
    CONTAINER_NAME=$container_name
    trap '
        set +e
        docker rm -v --force -- "$CONTAINER_NAME" >/dev/null 2>&1
        docker rmi --force -- "$CONTAINER_NAME" >/dev/null 2>&1
    ' EXIT INT TERM QUIT HUP
    # Create test directory and files:
    mkdir -p "$test_dir"
    rm -f "$stop_file" "$log_file" "$start_file"

    # Run the test container:
    docker run -d --rm --name "$container_name" \
        -e "REALM=$krb5_realm" \
        -v "$test_dir:/test" \
        -p "127.0.0.1:$ker5_port:88/tcp" \
        -p "127.0.0.1:$ker5_port:88/udp" \
        "$container_name"

    local krb5_kdc_tcp="kdc = $(docker port "$container_name" 88/tcp)"
    local krb5_kdc_udp="kdc = $(docker port "$container_name" 88/udp)"
    if [ x"$krb5_kdc_tcp" = x"$krb5_kdc_udp" ]; then
        krb5_kdc_udp=
    fi

    cat >"$krb5_config" <<EOF
[libdefaults]
    default_realm = $krb5_realm
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    default_tkt_enctypes = AES256-CTS-HMAC-SHA1-96 AES128-CTS-HMAC-SHA1-96
    default_tgs_enctypes = AES256-CTS-HMAC-SHA1-96 AES128-CTS-HMAC-SHA1-96
    permitted_enctypes = AES256-CTS-HMAC-SHA1-96 AES128-CTS-HMAC-SHA1-96

[realms]
    $krb5_realm = {
        $krb5_kdc_udp
        $krb5_kdc_tcp
    }

[domain_realm]
    .localhost = $krb5_realm
    localhost = $krb5_realm
EOF

    cat >"$openssl_config" <<EOF
# This enables legacy cipher suites like RC4 to make older Kerberos version
# initialization work.
# Without this one would get an error like this:
# * rc4 8: EVP_CipherInit_ex einit
# The workaround is to enable legacy cipher suites by setting environment
# variable OPENSSL_CONF to this file.
# https://github.com/heimdal/heimdal/issues/1224

openssl_conf = openssl_init

[openssl_init]
providers = provider_sect

[provider_sect]
default = default_sect
legacy = legacy_sect

[default_sect]
activate = 1

[legacy_sect]
activate = 1
EOF

    echo "Waiting for QFS Kerberos Test container to start..."
    local rem_retries=60 # 60 seconds
    while ! [ -f "$start_file" ]; do
        rem_retries=$(expr $rem_retries - 1)
        if [ $rem_retries -le 0 ]; then
            echo "Waiting for QFS Kerberos Test container to start timed out"
            if [ -f "$log_file" ]; then
                cat "$log_file"
            fi
            return 1
        fi
        sleep 0.5
        if [ x"$(docker ps -f "name=$container_name" \
            -f "status=running" -q)" = x ]; then
            echo "QFS Kerberos Test container failed to start"
            if [ -f "$log_file" ]; then
                cat "$log_file"
            fi
            return 1
        fi
    done
    echo "QFS Kerberos Test container started, running tests..."

    export KRB5_CONFIG=$krb5_config
    export OPENSSL_CONF=$openssl_config
    . "$krb_env_file"
    # Service name from QFS_META_PRINCIPAL (service/host@realm)
    krb_meta_service=${QFS_META_PRINCIPAL%%/*}
    # Create a Kerberos ticket:
    kdestroy
    if kinit -h 2>&1 | grep -- --password-file >/dev/null; then
        kinit --password-file="$start_file" "${QFS_CLIENT_PRINCIPAL}"
    else
        kinit "${QFS_CLIENT_PRINCIPAL}" <"$start_file"
    fi
    klist
    # Run the test program (service host, service name, keytab path)
    "$test_program" localhost "$krb_meta_service" "$test_dir/test.keytab" dMRr2
    kdestroy
    # Stop the test container:
    touch "$stop_file"
}

krb5_test ${1:+"$@"}
