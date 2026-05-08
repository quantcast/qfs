#!/bin/bash
set -e

REALM="${REALM:-QFS.TEST}"
# Used to bootstrap the KDC database master key and the kadmin
# admin/admin principal.
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin123}"
# Passwords for the QFS test client and admin principals. The
# principals are also added to per-role keytabs (see below); these
# passwords are kept so they can be kinit'd into the default credential
# cache interactively if/when needed (e.g. for ad hoc debugging).
TEST_PASSWORD="${TEST_PASSWORD:-test123}"
TEST_ADMIN_PASSWORD="${TEST_ADMIN_PASSWORD:-admin123}"
HOSTNAME=$(hostname -f)
TEST_DIR="${TEST_DIR:-/test}"
# Per-principal keytab files. QFS pins each role's identity to its own
# keytab so there is no cross-talk through the default credential cache.
QFS_META_KEYTAB="${QFS_META_KEYTAB:-${TEST_DIR}/qfsmeta.keytab}"
QFS_CHUNK_KEYTAB="${QFS_CHUNK_KEYTAB:-${TEST_DIR}/qfschunk.keytab}"
QFS_CLIENT_KEYTAB="${QFS_CLIENT_KEYTAB:-${TEST_DIR}/qfsclient.keytab}"
QFS_ADMIN_KEYTAB="${QFS_ADMIN_KEYTAB:-${TEST_DIR}/qfsadmin.keytab}"
KRB_ENV_FILE="${KRB_ENV_FILE:-${TEST_DIR}/krb.env}"

# QFS principals (service/host@realm or user@realm); overridable for custom
# realms/hosts
QFS_CLIENT_USER="${QFS_CLIENT_USER:-$USER}"
QFS_META_PRINCIPAL="${QFS_META_PRINCIPAL:-qfsmeta/localhost@${REALM}}"
QFS_CHUNK_PRINCIPAL="${QFS_CHUNK_PRINCIPAL:-qfschunk/localhost@${REALM}}"
QFS_CLIENT_PRINCIPAL="${QFS_CLIENT_PRINCIPAL:-$QFS_CLIENT_USER@${REALM}}"
QFS_ADMIN_PRINCIPAL="${QFS_ADMIN_PRINCIPAL:-admin@${REALM}}"

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

rm -f \
    "$QFS_META_KEYTAB" \
    "$QFS_CHUNK_KEYTAB" \
    "$QFS_CLIENT_KEYTAB" \
    "$QFS_ADMIN_KEYTAB"

# Derive meta/chunk service and host for principal creation (support localhost + HOSTNAME)
meta_service="${QFS_META_PRINCIPAL%%/*}"
meta_host="${QFS_META_PRINCIPAL#*/}"
meta_host="${meta_host%%@*}"
chunk_service="${QFS_CHUNK_PRINCIPAL%%/*}"
chunk_host="${QFS_CHUNK_PRINCIPAL#*/}"
chunk_host="${chunk_host%%@*}"

# Create principals.
# admin/admin and the QFS client / admin client principals keep
# passwords so they can be kinit'd interactively (default credential
# cache); meta and chunk service principals use random keys.
# The client/admin principals are also added to keytabs below using
# "ktadd -norandkey" so the keytab entries match the password-derived
# keys and password-based kinit keeps working.
kadmin.local -q "addprinc -pw $ADMIN_PASSWORD admin/admin@${REALM}"
kadmin.local -q "addprinc -pw $TEST_PASSWORD ${QFS_CLIENT_PRINCIPAL}"
kadmin.local -q "addprinc -pw $TEST_ADMIN_PASSWORD ${QFS_ADMIN_PRINCIPAL}"
kadmin.local -q "addprinc -randkey ${QFS_META_PRINCIPAL}"
kadmin.local -q "addprinc -randkey ${meta_service}/${HOSTNAME}@${REALM}"
kadmin.local -q "addprinc -randkey ${QFS_CHUNK_PRINCIPAL}"
kadmin.local -q "addprinc -randkey ${chunk_service}/${HOSTNAME}@${REALM}"

# Create per-role keytabs. Each QFS role (meta server, chunk server,
# test client, test admin client) gets its own keytab so identities
# cannot leak across roles via a shared credential source.
mkdir -p \
    "$(dirname -- "$QFS_META_KEYTAB")" \
    "$(dirname -- "$QFS_CHUNK_KEYTAB")" \
    "$(dirname -- "$QFS_CLIENT_KEYTAB")" \
    "$(dirname -- "$QFS_ADMIN_KEYTAB")"

# Meta server keytab: meta service principal(s).
kadmin.local -q "ktadd -k $QFS_META_KEYTAB ${QFS_META_PRINCIPAL}"
kadmin.local -q "ktadd -k $QFS_META_KEYTAB ${meta_service}/${HOSTNAME}@${REALM}"

# Chunk server keytab: chunk service principal(s).
kadmin.local -q "ktadd -k $QFS_CHUNK_KEYTAB ${QFS_CHUNK_PRINCIPAL}"
kadmin.local -q "ktadd -k $QFS_CHUNK_KEYTAB ${chunk_service}/${HOSTNAME}@${REALM}"

# Test client keytab: regular test user principal. Use -norandkey so
# the existing password-derived keys are preserved and password-based
# kinit (against TEST_PASSWORD) keeps working alongside the keytab.
kadmin.local -q "ktadd -norandkey -k $QFS_CLIENT_KEYTAB ${QFS_CLIENT_PRINCIPAL}"

# Test admin client keytab: admin (root-mapped) test user principal.
# -norandkey for the same reason as above (TEST_ADMIN_PASSWORD).
kadmin.local -q "ktadd -norandkey -k $QFS_ADMIN_KEYTAB ${QFS_ADMIN_PRINCIPAL}"

chmod 644 \
    "$QFS_META_KEYTAB" \
    "$QFS_CHUNK_KEYTAB" \
    "$QFS_CLIENT_KEYTAB" \
    "$QFS_ADMIN_KEYTAB"

# Create env file for sourcing (%q so values with quotes/spaces/etc. are safe)
mkdir -p "$(dirname -- "$KRB_ENV_FILE")"
{
    printf '# Kerberos test env - source with: . %s\n' "$KRB_ENV_FILE"
    printf 'export REALM=%q\n' "$REALM"
    printf 'export ADMIN_PASSWORD=%q\n' "$ADMIN_PASSWORD"
    printf 'export TEST_PASSWORD=%q\n' "$TEST_PASSWORD"
    printf 'export TEST_ADMIN_PASSWORD=%q\n' "$TEST_ADMIN_PASSWORD"
    printf 'export HOSTNAME=%q\n' "$HOSTNAME"
    printf 'export QFS_META_PRINCIPAL=%q\n' "$QFS_META_PRINCIPAL"
    printf 'export QFS_CHUNK_PRINCIPAL=%q\n' "$QFS_CHUNK_PRINCIPAL"
    printf 'export QFS_CLIENT_PRINCIPAL=%q\n' "$QFS_CLIENT_PRINCIPAL"
    printf 'export QFS_ADMIN_PRINCIPAL=%q\n' "$QFS_ADMIN_PRINCIPAL"
    printf 'export QFS_META_KEYTAB=%q\n' "$QFS_META_KEYTAB"
    printf 'export QFS_CHUNK_KEYTAB=%q\n' "$QFS_CHUNK_KEYTAB"
    printf 'export QFS_CLIENT_KEYTAB=%q\n' "$QFS_CLIENT_KEYTAB"
    printf 'export QFS_ADMIN_KEYTAB=%q\n' "$QFS_ADMIN_KEYTAB"
} >"$KRB_ENV_FILE"

echo ""
echo "=========================================="
echo "Kerberos Setup Complete!"
echo "=========================================="
echo "Realm: $REALM"
echo "QFS meta server principal:  $QFS_META_PRINCIPAL  (keytab: $QFS_META_KEYTAB)"
echo "QFS chunk server principal: $QFS_CHUNK_PRINCIPAL (keytab: $QFS_CHUNK_KEYTAB)"
echo "QFS client principal:       $QFS_CLIENT_PRINCIPAL (keytab: $QFS_CLIENT_KEYTAB)"
echo "QFS admin principal:        $QFS_ADMIN_PRINCIPAL (keytab: $QFS_ADMIN_KEYTAB)"
echo ""
echo "To get a ticket from a keytab:"
echo "  kinit -kt $QFS_CLIENT_KEYTAB $QFS_CLIENT_PRINCIPAL"
echo "Or interactively with the password (TEST_PASSWORD / TEST_ADMIN_PASSWORD):"
echo "  kinit $QFS_CLIENT_PRINCIPAL"
echo ""
echo "To source env vars (REALM, QFS_*_PRINCIPAL, QFS_*_KEYTAB, etc.):"
echo "  . $KRB_ENV_FILE"
echo ""
