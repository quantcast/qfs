#!/bin/sh
#
# $Id$
#
# Created 2013/08/26
# Author: Mike Ovsiannikov
#
# Copyright 2013 Quantcast Corp.
#
# This file is part of Kosmos File System (KFS).
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
#
# A script to create x509 certs / keys signed by the common certificate
# authority.

set -e

showcerts=0
if [ $# -gt 1 -a x"$1" = x"-v" ]; then
    showcerts=1
    shift
fi

workdir="${1-qfscerts}"
shift
if [ $# = 0 ]; then
    certs='qfs_test_meta qfs_test_chunk1 qfs_test_chunk2 qfs_test_client'
fi

subjectprefix=${subjectprefix-'/C=US/ST=California/L=San Francisco'}
cacommonname=${cacommonname-'qfs_test_ca'}
cadir="$workdir/qfs_ca"
caconf="$cadir/qfs_ca.conf"
if [ -f "$cadir/private/cakey.pem" ]; then
    echo "using CA: $cadir"
else
    echo "creating CA: $cadir"
    mkdir -p "$cadir/private" "$cadir/newcerts"
    openssl req \
        -days 3650 \
        -subj "$subjectprefix/CN=$cacommonname" \
        -newkey rsa:2048 \
        -new \
        -nodes \
        -x509 \
        -keyout "$cadir/private/cakey.pem" \
        -out "$cadir/cacert.pem" || exit
    [ $showcerts -ne 0 ] && openssl x509 -in "$cadir/cacert.pem" -text
    cat > "$caconf" << EOF
    [ ca ]
    default_ca      = CA_default            # The default ca section

    [ CA_default ]

    dir            = $cadir                 # top dir
    database       = \$dir/index.txt        # index file.
    new_certs_dir  = \$dir/newcerts         # new certs dir

    certificate    = \$dir/cacert.pem       # The CA cert
    serial         = \$dir/serial           # serial no file
    private_key    = \$dir/private/cakey.pem# CA private key
    RANDFILE       = \$dir/private/.rand    # random number file

    default_days     = 3650                 # how long to certify for
    default_crl_days = 30                   # how long before next CRL
    default_md       = sha1                 # md to use

    policy           = policy_any           # default policy
    email_in_dn      = no                   # Don't add the email into cert DN

    name_opt         = ca_default           # Subject name display option
    cert_opt         = ca_default           # Certificate display option
    copy_extensions  = none                 # Don't copy extensions from request

    [ policy_any ]
    countryName            = supplied
    stateOrProvinceName    = optional
    organizationName       = optional
    organizationalUnitName = optional
    commonName             = supplied
    emailAddress           = optional
EOF
    touch "$cadir/index.txt"
    echo 01 >> "$cadir/serial"
fi

for n in ${certs-"$@"}; do
    cn="$n"
    n="$workdir/$cn"
    if [ -f "$n.key" ]; then
        true
    else
        openssl req \
            -days 3650 \
            -subj "$subjectprefix/CN=$cn" \
            -nodes \
            -newkey rsa:1024 \
            -keyout "$n.key" \
            -out "$n.req"
        openssl ca -batch -config "$caconf" -out "$n.crt" -infiles "$n.req"
        rm "$n.req"
    fi
    [ $showcerts -ne 0 ] && openssl x509 -in "$n.crt" -text
    openssl verify -CAfile "$cadir/cacert.pem" "$n.crt" || exit
done
