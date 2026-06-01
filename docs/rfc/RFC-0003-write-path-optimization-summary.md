# RFC-0003: QFS Write Path Optimization Summary

## Summary

This document summarizes the write-path optimization work on branch
`lock-opt` and the proposed plan for preparing upstream pull requests.

The overall direction is to reduce small-file create/write latency by:

- reducing metaserver namespace lock contention,
- removing synchronous chunkserver pre-create from the hot allocation path,
- reusing chunkserver connections,
- replacing chain replication in the client write path with client-side fanout,
- avoiding duplicate checksum scans on every chunkserver replica,
- reducing avoidable buffer copies in fanout.

The current benchmark focus is replicated 1 MB file creation with three
chunkservers.

## Implemented Optimizations

### Namespace / Metaserver

- Added the NamespaceV2 implementation and tests.
- Reworked metadata operations toward finer-grained locking instead of a single
  coarse global namespace lock.
- Added write transaction / WAL work needed by the v2 namespace path.
- Added batch apply / commit optimizations for high-frequency small
  transactions.
- Added recovery validation tests for NamespaceV2 WAL replay.

### HDFS-like Write Allocation

- Added an optional HDFS-like allocation path:
  - `metaServer.writeFlow.hdfsLikeAllocate`
  - `chunkServer.writeFlow.lazyCreateOnWrite`
- The metaserver can allocate chunk metadata and return the lease to the client
  without synchronously sending `ALLOCATE_CHUNK` to chunkservers.
- The chunkserver lazily creates the chunk on `WRITE_ID_ALLOC` when enabled.
- The client passes file id and lease id to `WRITE_ID_ALLOC`.
- Crash/restart validation found the expected incomplete-write gap and added a
  recovery direction: truncate EOF to the last recoverable chunk instead of
  trusting a namespace size that points past available stable replicas.

### Client Chunkserver Connection Reuse

- Added a chunkserver client pool for the write path.
- `mstress_client` now honors `QFS_CLIENT_CONFIG`, so benchmark runs can use the
  same client config as normal tools.
- This removed connection churn from short-file write tests:
  - `ChunkServer.Pool.Connect=3`
  - `ChunkServer.Pool.OpsQueued=9000` for 1000 files with three fanout RPC
    stages.

### Parallel Replica Write Fanout

- Added `client.parallelReplicaWrite` and enabled it by default in the test
  configuration.
- Added `No-forward` / `NF` support for:
  - `WRITE_ID_ALLOC`
  - `WRITE_PREPARE`
  - `WRITE_SYNC`
  - `CLOSE`
- Client now sends write-id allocation, write prepare, and close RPCs directly
  to all replicas instead of relying on chunkserver-to-chunkserver forwarding.
- The request still carries the full replica list so each chunkserver can derive
  its own replica position and write id.

### Fanout Buffer Sharing

- Verified that `IOBufferData` already uses a ref-counted data block.
- Added `IOBuffer::AppendShared()` so fanout requests attach shared buffer
  references directly.
- `Writer` now uses `AppendShared()` instead of creating a temporary cloned
  `IOBuffer` for each replica fanout request.
- Payload bytes are not copied for fanout; each replica request holds a shared
  reference and the data is released when the last reference drops.

### Checksum Hot Path

- Client now sends the 64 KB checksum vector in `WRITE_PREPARE` reply mode.
- Chunkserver can reuse the client-provided checksum vector for chunk metadata.
- With `chunkServer.skipWritePrepareChecksumVerify=1`, chunkserver skips the
  duplicate payload scan in the write hot path and trusts the client-provided
  checksum vector.
- Short RPC checksum vector output was fixed to preserve hex formatting for
  subsequent short-format fields.

This matches the HDFS-style tradeoff more closely: clients provide packet /
chunk checksums, datanodes store them, and later reads or scrubs verify stored
data against those checksums.

## Latest Benchmark Snapshot

Environment:

- three local chunkservers,
- client and chunkservers use `202.168.115.34` instead of `localhost` to force
  traffic through the network path,
- three replicas,
- 1 MB files,
- `client.parallelReplicaWrite=1`,
- `chunkServer.skipWritePrepareChecksumVerify=1`.

### Single Client

Plan: 1 client, 1000 files, 1 MB per file.

```text
1000 files created in 3058 ms

open  avg: 240 us
write avg: 134 us
close avg: 2681 us

Write.ChunkWriteUsec: 1735099 us
Write.CloseUsec:      2655539 us
Write.WriteIdAlloc:   225061 us
Write.ChunkClose:     114009 us

ChunkServer.Pool.BytesSent: 3147979791
ChunkServer.Pool.Connect:   3
ChunkServer.Pool.OpsQueued: 9000
```

Approximate throughput:

- logical write throughput: about 327 MB/s,
- actual client network send: about 1.03 GB/s because every 1 MB file is sent
  to three replicas.

### Two Clients

Plan: 2 clients, 1000 files per client, 1 MB per file.

Before checksum-vector / skip-verify optimization:

```text
proc_00: 4353 ms, Write.ChunkWriteUsec=2865775
proc_01: 4349 ms, Write.ChunkWriteUsec=2863192
```

After checksum-vector / skip-verify optimization:

```text
proc_00: 4171 ms, Write.ChunkWriteUsec=2414194
proc_01: 4209 ms, Write.ChunkWriteUsec=2435554
```

`ChunkWriteUsec` dropped by about 14-16%. Total time dropped by about 3-4%.
The remaining cost is dominated by three-replica network fanout and chunk file
write / close work.

## Correctness Notes

- `chunkServer.skipWritePrepareChecksumVerify=1` changes write-time checksum
  semantics: the chunkserver trusts the client-provided checksum vector instead
  of recomputing checksums over the received payload. This is closer to the
  HDFS write-path tradeoff, but it should be treated as a deliberate
  configuration choice.
- The HDFS-like lazy-create path needs careful recovery semantics for killed
  clients. The current direction is to truncate or repair namespace EOF to the
  last recoverable stable chunk after restart.
- Append, striped files, object store files, and authenticated / tokenized
  synchronous replication paths need separate review before enabling the new
  write flow broadly.

## Upstream PR Plan

The current branch contains several related but separable changes. For upstream
review, split into smaller PRs:

1. **Infrastructure / tests**
   - NamespaceV2 tests and WAL replay tests.
   - Benchmark client config loading through `QFS_CLIENT_CONFIG`.
   - Minimal scripts or docs only if acceptable upstream.

2. **NamespaceV2 / lock optimization**
   - Finer-grained metadata locking.
   - WAL / transaction correctness tests.
   - Keep performance changes separate from protocol changes where possible.

3. **HDFS-like lazy create**
   - Config switches.
   - Metaserver allocate bypass.
   - Chunkserver lazy chunk creation on `WRITE_ID_ALLOC`.
   - Recovery behavior must be completed before this is proposed as
     production-ready.

4. **Write connection reuse and fanout**
   - Client chunkserver pool.
   - `client.parallelReplicaWrite`.
   - `No-forward` protocol support.
   - Parallel `WRITE_ID_ALLOC`, `WRITE_PREPARE`, and `CLOSE`.

5. **Checksum-vector hot-path optimization**
   - Client sends block checksum vector in write-prepare reply mode.
   - Chunkserver reuses the vector.
   - Optional `chunkServer.skipWritePrepareChecksumVerify`.

6. **Buffer sharing cleanup**
   - `IOBuffer::AppendShared()`.
   - Writer fanout uses shared buffer references instead of temporary clone
     buffers.

## Remaining Work

- Add chunkserver-side detailed timing counters for:
  - request parse,
  - checksum handling,
  - disk queue submit,
  - disk completion latency.
- Finish crash/restart recovery for killed writers under lazy create.
- Re-run larger 100k-file tests after recovery semantics are finalized.
- Run compatibility tests with short RPC disabled and enabled.
- Run tests with `chunkServer.skipWritePrepareChecksumVerify=0` and `1` to make
  the correctness/performance tradeoff explicit.
