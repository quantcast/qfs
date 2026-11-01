# QFS Version 2.2.9 Release Notes

## Major Features and Improvements

### Kerberos Authentication Hardening

- **Private credential cache for keytab clients**: Keytab-based clients no
  longer use the default (`KRB5CCNAME`) credential cache. That cache is shared
  with other processes and is clobbered when several QFS roles use different
  keytabs. The client now uses a private, process-local cache. It tries an
  in-memory (`MEMORY:`) cache first, and falls back to a `FILE:` cache under
  `TMPDIR` or `/tmp`, created with `mkstemp` and mode `0600`.
- **File cache lifetime**: The descriptor from `mkstemp` is closed before the
  Kerberos library opens the path, and the file is not removed at that point.
  Removing it earlier races the library's open. The client destructor unlinks
  the file. If the process is killed, the file can remain. A warning is logged
  when the in-memory cache is unavailable and the file cache is used.
- **Heimdal**: Heimdal builds always initialize the credential cache with
  `krb5_cc_initialize`.
- **Session key fallback**: If `krb5_auth_con_getkey` succeeds but returns no
  keyblock, the client copies `krb5_creds.keyblock` when that field exists
  (`KRB5_HAS_krb5_creds_keyblock`).
- **Principal names**: Names copied into Kerberos buffers include the
  terminating NUL, using `krb5_data_copy`.
- **Unescape**: Principal unescaping accepts only the Kerberos escapes
  (backslash, `@`, `/`, and NUL). It no longer treats whitespace and other
  characters as escapes.
- **Weak session keys**: With Kerberos authentication the session key is the
  TLS-PSK, so its strength is the negotiated enctype. The meta server and the
  client log a warning when that key is shorter than 128 bits (single-DES
  class). AES-128 and AES-256 produce 16 and 32 byte keys. The annotated meta
  server, chunk server, and client configuration files describe restricting
  `permitted_enctypes` in `krb5.conf` to strong AES ciphers and avoiding
  single-DES and RC4 keys in the keytab.

### Kerberos Authentication Test

- A Docker harness starts a KDC and kadmin, creates a keytab per QFS role
  (meta server, chunk server, test client, and admin client), and can run
  either the Kerberos unit program or the QFS test suite.
- `qfstest.sh` accepts that environment. It strips `@REALM` from the
  authenticated principal so the name matches a local user, allows `root` and
  the test client on the meta server client port, and maps the chunk server
  and admin service principals to `root` on that port. Chunk servers use a
  client connection for chunk recovery, and the admin client needs root access.
- The latest Linux release jobs run this test: Ubuntu 24.04 and 22.04, Debian
  13, and Rocky Linux 10, on x86_64 and arm64 where those jobs exist.
- `make krbtest` runs the suite when `KRB_ENV_FILE` points at the generated
  environment file. `make mintest` includes that target and skips the Kerberos
  run when the file is not set.

### OpenSSL 4.0

- Certificate `notAfter` parsing no longer reads `ASN1_TIME` fields directly.
  OpenSSL 1.1 and later expose those through `ASN1_STRING_type`,
  `ASN1_STRING_get0_data`, and `ASN1_STRING_length`.
- Common-name and expiry reads use the const-qualified X509 accessors from
  OpenSSL 4.0, with a local `X509_get0_notAfter` shim for OpenSSL before 1.1.

## Component-Specific Changes

### Kerberos Client and CMake

- `FindKrb5.cmake` detects MIT versus Heimdal from
  `KRB5_HAS_krb5_data_alloc` (the previous check referenced the wrong
  variable), looks for `krb5.h` before `krb5/krb5.h`, and checks whether
  `krb5_creds` has a `keyblock` member.
- `KrbClient.cc` includes `stdint.h` so the client builds on older Linux
  headers.

### Meta Server

- Authentication logs the weak session-key warning described above.
- The annotated object-store parameter for the CA directory is
  `chunkServer.diskQueue.<object-store-directory-prefix>ssl.CADir`. The
  previous name omitted `ssl.`.

### Client Library

- Client authentication logs the same weak session-key warning, and logs when
  a keytab client has to use an on-disk credential cache.

### C Client Test

- `test-qfsc` initializes `qfs_attr` to zero before use, which removes the new
  macOS uninitialized-field warnings.

### Java Build

- The Java build asks the JDK that Maven reports for the oldest `--release` it
  still accepts. `javac` on `PATH` can be a different JDK. Using that one can
  select a source and target release the Maven JDK rejects, for example
  release 7 with JDK 20 or later. If Maven's Java home cannot be read from
  `mvn --version`, the script falls back to `javac` on `PATH`.

### Build System and CI

- The CI Maven download is 3.9.16 (2.2.8 used 3.9.11). `curl` follows redirects
  and fails on HTTP errors when fetching that tarball.
- Apache RAT is 0.18. `sorttable.js` is listed in `.ratignore`.
- The Debian 11 build is removed. Debian 11 is end of life.

## Platform and Dependency Updates

### Supported Platforms

- **Removed**: Debian 11 (end of life). The 2.2.9 binary table has no Debian
  11 download.
- **Unchanged**: Ubuntu 14.04, 16.04, 18.04, 20.04, 22.04, and 24.04 (x86_64,
  and arm64 for 24.04); Debian 12 and 13 (x86_64 and arm64); CentOS 6, 7, and
  8; Rocky Linux 9 and 10 (x86_64 and arm64); Amazon Linux 2023 (x86_64 and
  arm64); Mac OS X x86_64 and arm64.

### Dependencies

- Maven 3.9.16 for the CI install.
- Apache RAT 0.18.
- Builds with OpenSSL 4.0.

## Documentation

- Annotated Kerberos sections in `MetaServer.prp`, `ChunkServer.prp`, and
  `QfsClient.prp` describe the private credential cache and the enctype
  restriction.
- The developer documentation links to the release publishing checklist.
