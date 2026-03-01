#!/bin/bash
set -e

REALM="${REALM:-QFS.TEST}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin123}"
TEST_PASSWORD="${TEST_PASSWORD:-test123}"
HOSTNAME=$(hostname -f)
KEYTAB_FILE="${KEYTAB_FILE:-/test/test.keytab}"
KRB_ENV_FILE="${KRB_ENV_FILE:-/test/krb.env}"

# QFS principals (service/host@realm or user@realm); overridable for custom
# realms/hosts
QFS_META_PRINCIPAL="${QFS_META_PRINCIPAL:-qfsmeta/localhost@${REALM}}"
QFS_CHUNK_PRINCIPAL="${QFS_CHUNK_PRINCIPAL:-qfschunk/localhost@${REALM}}"
QFS_CLIENT_PRINCIPAL="${QFS_CLIENT_PRINCIPAL:-testclient@${REALM}}"

echo "Setting up Kerberos realm: $REALM"

# Create krb5.conf
cat >/etc/krb5.conf <<EOF
[libdefaults]
    default_realm = ${REALM}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    default_tgs_enctypes = aes256-cts-hmac-sha1-96
    default_tkt_enctypes = aes256-cts-hmac-sha1-96
    permitted_enctypes = aes256-cts-hmac-sha1-96

[realms]
    ${REALM} = {
        kdc = localhost
        admin_server = localhost
    }

[domain_realm]
    .${HOSTNAME} = ${REALM}
    ${HOSTNAME} = ${REALM}

[logging]
    kdc = FILE:/var/log/krb5kdc.log
    admin_server = FILE:/var/log/kadmin.log
    default = FILE:/var/log/krb5lib.log
EOF

# Create KDC database
kdb5_util create -s -P "$ADMIN_PASSWORD" -r "$REALM"

# Create ACL file
mkdir -p /etc/krb5kdc
echo "*/admin@${REALM} *" >/etc/krb5kdc/kadm5.acl

wait_for_port() {
    local port="$1"
    local timeout="${2:-10}"
    local i=0
    while ! nc -z localhost "$port" 2>/dev/null && [ $i -lt $timeout ]; do
        sleep 0.5
        i=$((i + 1))
    done
    nc -z localhost "$port" 2>/dev/null
}

# Start KDC
krb5kdc
wait_for_port 88 || {
    echo "KDC failed to start on port 88"
    exit 1
}

kadmind
wait_for_port 749 || {
    echo "kadmind failed to start on port 749"
    exit 1
}

rm -f "$KEYTAB_FILE"

# Derive meta/chunk service and host for principal creation (support localhost + HOSTNAME)
meta_service="${QFS_META_PRINCIPAL%%/*}"
meta_host="${QFS_META_PRINCIPAL#*/}"
meta_host="${meta_host%%@*}"
chunk_service="${QFS_CHUNK_PRINCIPAL%%/*}"
chunk_host="${QFS_CHUNK_PRINCIPAL#*/}"
chunk_host="${chunk_host%%@*}"

# Create principals
kadmin.local -q "addprinc -pw $ADMIN_PASSWORD admin/admin@${REALM}"
kadmin.local -q "addprinc -pw $TEST_PASSWORD ${QFS_CLIENT_PRINCIPAL}"
kadmin.local -q "addprinc -randkey ${QFS_META_PRINCIPAL}"
kadmin.local -q "addprinc -randkey ${meta_service}/${HOSTNAME}@${REALM}"
kadmin.local -q "addprinc -randkey ${QFS_CHUNK_PRINCIPAL}"
kadmin.local -q "addprinc -randkey ${chunk_service}/${HOSTNAME}@${REALM}"

# Create keytab (meta and chunk principals; client uses password/kinit)
mkdir -p "$(dirname -- "$KEYTAB_FILE")"
kadmin.local -q "ktadd -k $KEYTAB_FILE ${QFS_META_PRINCIPAL}"
kadmin.local -q "ktadd -k $KEYTAB_FILE ${meta_service}/${HOSTNAME}@${REALM}"
kadmin.local -q "ktadd -k $KEYTAB_FILE ${QFS_CHUNK_PRINCIPAL}"
kadmin.local -q "ktadd -k $KEYTAB_FILE ${chunk_service}/${HOSTNAME}@${REALM}"

chmod 644 "$KEYTAB_FILE"

# Create env file for sourcing
mkdir -p "$(dirname -- "$KRB_ENV_FILE")"
cat >"$KRB_ENV_FILE" <<ENVEOF
# Kerberos test env - source with: . $KRB_ENV_FILE
export REALM='${REALM}'
export ADMIN_PASSWORD='${ADMIN_PASSWORD}'
export TEST_PASSWORD='${TEST_PASSWORD}'
export HOSTNAME='${HOSTNAME}'
export KEYTAB_FILE='${KEYTAB_FILE}'
export QFS_META_PRINCIPAL='${QFS_META_PRINCIPAL}'
export QFS_CHUNK_PRINCIPAL='${QFS_CHUNK_PRINCIPAL}'
export QFS_CLIENT_PRINCIPAL='${QFS_CLIENT_PRINCIPAL}'
ENVEOF

echo ""
echo "=========================================="
echo "Kerberos Setup Complete!"
echo "=========================================="
echo "Realm: $REALM"
echo "QFS meta server principal: $QFS_META_PRINCIPAL"
echo "QFS chunk server principal: $QFS_CHUNK_PRINCIPAL"
echo "QFS client principal: $QFS_CLIENT_PRINCIPAL"
echo "Keytab: $KEYTAB_FILE"
echo ""
echo "To get a ticket:"
echo "  kinit $QFS_CLIENT_PRINCIPAL"
echo "  (password: $TEST_PASSWORD)"
echo ""
echo "To source env vars (REALM, KEYTAB_FILE, QFS_*_PRINCIPAL, etc.):"
echo "  . $KRB_ENV_FILE"
echo ""
