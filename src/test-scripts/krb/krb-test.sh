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

krb5cc_export_file_name() {
	local test_dir=$1
	local krb5cc_name=FILE:$test_dir/krb5cc-$2
	export KRB5CCNAME=$krb5cc_name
}

krb_init_creds() {
	# Initialize Kerberos credentials from environment variables.
	# Arguments:
	# 1. Test directory
	# 2. Kerberos password and principal pairs
	#   (e.g. TEST_PASSWORD TEST_PRINCIPAL).
	local test_dir=$1
	shift
	local krb_var
	local krb_val
	local test_password_file=
	local kinit_passwd_file_arg=0
	local krb5cc_name=

	if kinit -h 2>&1 | grep -- --password-file >/dev/null; then
		kinit_passwd_file_arg=1
	fi
	while [ $# -gt 0 ]; do
		krb_var=$1
		shift
		eval "krb_val=\$$krb_var"
		if [ x"$test_password_file" = x ]; then
			test_password_file=$test_dir/$krb_var
			krb5cc_export_file_name "$test_dir" "$krb_var"
			echo "$krb_val" > "$test_password_file"
			continue
		fi
		kdestroy
		if [ $kinit_passwd_file_arg -ne 0 ]; then
			kinit --password-file="$test_password_file" "${krb_val}"
		else
			kinit "${krb_val}" <"$test_password_file"
		fi
		rm -f "$test_password_file"
		test_password_file=
		klist
	done
}

run_in_new_process_group() {
	local test_program=$1
	shift
	# Disable errexit so we can return the test program's status.
	set +e
    # Run test program in its own process group so signals/cleanup can target
    # the whole group (e.g. kill -- -PGID). Enabling job control (set -m) makes
    # the shell place the background pipeline in a new process group; this is
    # portable across Linux and macOS /bin/sh. We then wait and propagate
    # status.
    set -m
    "$test_program" ${1+"$@"} &
    local test_program_pid=$!
    set +m
    wait "$test_program_pid"
	local ret=$?
	set -e # Restore errexit.
	return $ret
}

krb5_test() {
    local build=0
    local run=0
    local qfs_test=0
    local test_dir=$PWD/qfstest-krb
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

	# Honor client user if set -- the same logic as in qfstest.sh.
	# With Kerberos auth, qfstest.sh sets it clientuser client principal by
	# dropping realm.
	if [ x"${clientuser-}" = x ]; then
		clientuser=$(id -un) || return 1
	fi

    while [ $# -gt 0 ]; do
        case "$1" in
        --build | -b)
            build=1
            shift
            ;;
        --run | -r)
            if [ $# -le 1 ]; then
                echo "test program not specified"
                return 1
            fi
            run=1
            test_program=$1
            shift
            break
            ;;
        --qfs-test | -q)
            qfs_test=1
            shift
            break
            ;;
        --)
            shift
            break
            ;;
        -h | --help)
            cat <<EOF
Usage: $0 [--build| -b] [--run | -r] [--qfs-test | -q] [--] <cmake arguments>
        --build| -b: build QFS Kerberos test program
        --run | -r: run the specified test program -- the remaining arguments \
are treated as test program arguments
        --qfs-test | -q: run qfstest.sh passing the remaining arguments as \
arguments to qfstest.sh
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

    if [ $(expr $run + $build + $qfs_test) -gt 1 ]; then
        echo "cannot build and run specified test program or run qfstest.sh" \
            " at the same time" 1>&2
        return 1
    fi
    if [ $build -ne 0 -o ! -x "$test_program" ]; then
        cmake --fresh ${1+"$@"} "$my_dir/../../.."
        cmake --build . --parallel --clean-first \
            --target "$(basename -- "$test_program")"
    fi

    local container_name=qfs-krb-test
    local image_label=qfs.krbtest.content_hash
    # Compute a content hash of the build context so the image can be
    # reused across runs and rebuilt only when inputs change.
    local sha_cmd
    if command -v sha256sum >/dev/null 2>&1; then
        sha_cmd='sha256sum'
    else
        sha_cmd='shasum -a 256'
    fi
    local content_hash
    content_hash=$(
        cd "$my_dir" && find . -type f \! -name '.*' -print0 |
            LC_ALL=C sort -z |
            xargs -0 $sha_cmd |
            $sha_cmd |
            awk '{print $1}'
    ) || return 1
    # Look up the hash recorded on the existing image (if any).
    local prev_hash=
    prev_hash=$(docker image inspect \
        --format "{{ index .Config.Labels \"$image_label\" }}" \
        "$container_name" 2>/dev/null) || prev_hash=
    if [ x"$prev_hash" = x"$content_hash" ] && [ x"$content_hash" != x ]; then
        echo "Reusing existing $container_name image (content hash matches)."
    else
        # Remove any prior, out-of-date image with this tag so it does not
        # accumulate as a dangling image after the rebuild retags the name.
        if docker image inspect "$container_name" >/dev/null 2>&1; then
            echo "Removing out-of-date $container_name image."
            docker rmi --force -- "$container_name" >/dev/null 2>&1 || :
        fi
        echo "Building $container_name image (content hash changed)."
        docker build \
            --label "$image_label=$content_hash" \
            -t "$container_name" \
            -f "$my_dir/Dockerfile.krbtest" \
            "$my_dir"
    fi
    CONTAINER_NAME=$container_name
    # Only remove the running container on exit; keep the image for reuse.
    QFS_STOP_FILE=$stop_file
    # Stop the test container when the script exits:
    trap '
        set +e
        docker rm -v --force -- "$CONTAINER_NAME" >/dev/null 2>&1
		touch "$QFS_STOP_FILE" 2>/dev/null
    ' EXIT INT TERM QUIT HUP
    # Create test directory and files:
    mkdir -p "$test_dir"
    rm -f "$stop_file" "$log_file" "$start_file"

    # Run the test container:
    docker run -d --rm --name "$container_name" \
        -e "REALM=$krb5_realm" \
        -e "TEST_DIR=$test_dir" \
        -e "QFS_CLIENT_USER=${clientuser}" \
        -v "$test_dir:/$test_dir" \
        -p "127.0.0.1:$ker5_port:88/tcp" \
        -p "127.0.0.1:$ker5_port:88/udp" \
        "$container_name"

    # docker port often prints 0.0.0.0:PORT; libkrb5 cannot use 0.0.0.0 as a KDC
    # destination (MIT Kerberos fails with "Cannot find KDC for realm").
    local kdc_tcp_hostport kdc_udp_hostport
    kdc_tcp_hostport=$(docker port "$container_name" 88/tcp |
        head -n1 | tr -d '\r')
    kdc_udp_hostport=$(docker port "$container_name" 88/udp |
        head -n1 | tr -d '\r')
    case "$kdc_tcp_hostport" in
    0.0.0.0:*) kdc_tcp_hostport="127.0.0.1:${kdc_tcp_hostport#0.0.0.0:}" ;;
    esac
    case "$kdc_udp_hostport" in
    0.0.0.0:*) kdc_udp_hostport="127.0.0.1:${kdc_udp_hostport#0.0.0.0:}" ;;
    esac
    local krb5_kdc_tcp="kdc = $kdc_tcp_hostport"
    local krb5_kdc_udp="kdc = $kdc_udp_hostport"
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
    if [ $run -ne 0 ]; then
        run_in_new_process_group "$test_program" ${1+"$@"}
    elif [ $qfs_test -ne 0 ]; then
        run_in_new_process_group \
			"$my_dir/../qfstest.sh" -kerberos "$krb_env_file" ${1+"$@"}
    else
		# Create Kerberos credentials for the test program, and run for each
		# credential.
		krb_init_creds "$test_dir" \
			TEST_PASSWORD QFS_CLIENT_PRINCIPAL \
			TEST_ADMIN_PASSWORD QFS_ADMIN_PRINCIPAL
		# Service name from QFS_META_PRINCIPAL (service/host@realm)
		local krb_meta_service=${QFS_META_PRINCIPAL%%/*}
		local cc_var
		local ret=0
		for cc_var in TEST_PASSWORD TEST_ADMIN_PASSWORD; do
			krb5cc_export_file_name "$test_dir" "$cc_var"
			echo "Credential cache for $cc_var: $KRB5CCNAME"
			run_in_new_process_group \
				"$test_program" localhost "$krb_meta_service" \
					"${QFS_META_KEYTAB}" "dMRr2" ${1+"$@"} || ret=$?
			kdestroy
		done
		return $ret
    fi
}

krb5_test ${1+"$@"}
