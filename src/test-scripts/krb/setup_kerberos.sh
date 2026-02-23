#!/bin/bash
set -e

REALM="${REALM:-QFS.TEST}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin123}"
TEST_PASSWORD="${TEST_PASSWORD:-test123}"
HOSTNAME=$(hostname -f)
KEYTAB_FILE="${KEYTAB_FILE:-/test/test.keytab}"
KRB_ENV_FILE="${KRB_ENV_FILE:-/test/krb.env}"

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

# Create principals
kadmin.local -q "addprinc -pw $ADMIN_PASSWORD admin/admin@${REALM}"
kadmin.local -q "addprinc -pw $TEST_PASSWORD testclient@${REALM}"
kadmin.local -q "addprinc -randkey test/localhost@${REALM}"
kadmin.local -q "addprinc -randkey test/${HOSTNAME}@${REALM}"

# Create keytab
mkdir -p "$(dirname -- "$KEYTAB_FILE")"
kadmin.local -q "ktadd -k "$KEYTAB_FILE" test/localhost@${REALM}"
kadmin.local -q "ktadd -k "$KEYTAB_FILE" test/${HOSTNAME}@${REALM}"

chmod 644 "$KEYTAB_FILE"

# Create env file for sourcing (REALM, ADMIN_PASSWORD, TEST_PASSWORD, HOSTNAME, KEYTAB_FILE)
mkdir -p "$(dirname -- "$KRB_ENV_FILE")"
cat >"$KRB_ENV_FILE" <<ENVEOF
# Kerberos test env - source with: . $KRB_ENV_FILE
export REALM='${REALM}'
export ADMIN_PASSWORD='${ADMIN_PASSWORD}'
export TEST_PASSWORD='${TEST_PASSWORD}'
export HOSTNAME='${HOSTNAME}'
export KEYTAB_FILE='${KEYTAB_FILE}'
ENVEOF

echo ""
echo "=========================================="
echo "Kerberos Setup Complete!"
echo "=========================================="
echo "Realm: $REALM"
echo "Test client: testclient@${REALM}"
echo "Service: test/localhost@${REALM}"
echo "Keytab: $KEYTAB_FILE"
echo ""
echo "To get a ticket:"
echo "  kinit testclient@${REALM}"
echo "  (password: $TEST_PASSWORD)"
echo ""
echo "To source env vars (REALM, ADMIN_PASSWORD, TEST_PASSWORD, HOSTNAME," \
    "KEYTAB_FILE):"
echo "  . $KRB_ENV_FILE"
echo ""
