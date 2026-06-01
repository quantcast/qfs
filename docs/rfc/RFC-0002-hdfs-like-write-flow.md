# RFC-0002: HDFS-like Write Flow for QFS

## Summary

This plan introduces an optional HDFS-like write allocation path for QFS.
The goal is to reduce small-file write latency by removing the synchronous
metaserver-to-chunkserver pre-create step from the normal replicated write
allocation path.

The existing QFS write flow is preserved by default. The new path is enabled
only when both metaserver and chunkserver switches are turned on.

## Current QFS Write Flow

For a normal replicated write, the current QFS path is:

1. Client creates or opens a file and obtains a file id.
2. Client write enters `Writer`.
3. `Writer` sends `ALLOCATE` to metaserver when it needs a chunk.
4. Metaserver selects chunkservers, creates metadata, grants a write lease.
5. Metaserver sends `ALLOCATE_CHUNK` to chunkserver before replying to client.
6. `ALLOCATE_CHUNK` is logged as an in-flight metaserver-to-chunkserver op.
7. Chunkserver creates the local chunk and registers the lease.
8. Client receives allocation result.
9. Client sends `WRITE_ID_ALLOC` to chunkserver.
10. Client sends `WRITE_PREPARE` / `WRITE_SYNC` data RPCs.
11. Client close / sync waits for pending writes and sends `CLOSE_CHUNK`.

The hot part for small-file writes is step 5-7. In benchmark investigation,
the pre-RPC in-flight logging before `ALLOCATE_CHUNK` dominated allocation
latency.

## Target HDFS-like Compatible Flow

With the new optional path:

1. Client creates or opens a file and obtains a file id.
2. Client write enters `Writer`.
3. `Writer` sends `ALLOCATE` to metaserver when it needs a chunk.
4. Metaserver selects chunkservers, creates metadata, grants a write lease.
5. Metaserver replies to client without sending `ALLOCATE_CHUNK`.
6. Client sends `WRITE_ID_ALLOC` with `File-handle` and `Lease-id`.
7. Chunkserver lazily creates the chunk if it does not exist.
8. Chunkserver registers the write lease locally.
9. Client sends `WRITE_PREPARE` / `WRITE_SYNC` data RPCs.
10. Client close / sync waits for pending writes and sends `CLOSE_CHUNK`.

This keeps the existing QFS client write protocol mostly intact while moving
chunk creation from metaserver-driven pre-create to client-write-driven lazy
creation.

## Config Switches

Both switches must be enabled:

```properties
metaServer.writeFlow.hdfsLikeAllocate = 1
chunkServer.writeFlow.lazyCreateOnWrite = 1
```

Default behavior remains the original QFS path:

```properties
metaServer.writeFlow.hdfsLikeAllocate = 0
chunkServer.writeFlow.lazyCreateOnWrite = 0
```

## Implemented Changes

Implemented in this branch:

- Metaserver:
  - Added `metaServer.writeFlow.hdfsLikeAllocate`.
  - For normal replicated non-append, non-striped, non-object-store allocation,
    metaserver can skip chunkserver pre-create.
  - The allocation request is completed immediately through `LayoutDone()`.
  - `MetaAllocate` response now includes `Lease-id` when available.

- Client:
  - `AllocateOp` now parses `Lease-id`.
  - `WRITE_ID_ALLOC` now carries optional `File-handle` and `Lease-id`.
  - `Writer` passes file id and lease id to `WRITE_ID_ALLOC`.
  - `WriteAppender` also passes these fields, but append remains excluded from
    the HDFS-like metaserver bypass path.

- Chunkserver:
  - Added `chunkServer.writeFlow.lazyCreateOnWrite`.
  - `WRITE_ID_ALLOC` parses optional `File-handle` and `Lease-id`.
  - If lazy-create is enabled and a normal write targets a missing chunk,
    chunkserver creates the chunk in `AllocateWriteId()`.
  - The write lease is registered locally from the lease id carried by the
    client before the normal lease validation.

## Not Fully Done

This is not yet a complete production-grade HDFS write-flow replacement.

- Client-CS auth and synchronous replication token semantics are not fully
  reworked for lazy creation.
- Append, object-store, and striped files intentionally remain on the original
  QFS allocation path.
- Crash-recovery behavior has not yet been validated with kill/restart tests.
- The current implementation still includes prior timing instrumentation in
  the write path; decide later whether to keep or clean it up.
- The nested `MetaLogChunkAllocate` path now explicitly schedules a log flush
  after enqueue; without this, HDFS-like allocate skipped chunkserver wait but
  still waited for the metaserver log writer timeout cadence.

## Test Plan

Minimum validation before treating this as stable:

1. Build:
   - `cmake --build bld --target metaserver chunkserver qfsput mstress_client -j8`
2. Unit test:
   - `./bld/output/bin/devtools/namespacev2test`
3. Functional write test:
   - Enable both switches.
   - Clean cluster and restart metaserver/chunkservers.
   - Write a 1MB file with `qfsput`.
   - Verify `qfs -ls` reports the expected size.
   - Read the file back and verify byte count/content.
4. Benchmark:
   - Run small-file write benchmark with old path.
   - Run the same benchmark with HDFS-like path enabled.
   - Compare total time, `Write.AllocateUsec`, close latency, and chunkserver
     lazy-create failures.
5. Failure tests:
   - Kill client after metaserver allocation but before write.
   - Kill chunkserver after lazy create but before close.
   - Restart and verify `HELLO` / `AVAILABLE_CHUNK` convergence.

## Current Verification Status

Completed:

- Build passed for `metaserver`, `chunkserver`, `qfsput`, and `mstress_client`.
- `namespacev2test` passed.
- `git diff --check` passed.
- Clean-cluster functional 1MB `qfsput` passed: `qfs -ls` reported 1048576 bytes.
- HDFS-like lazy-create path was verified in chunkserver logs.
- 50 x 1MB write probe after fixing nested log flush:
  - Total time: 366 ms.
  - Previous HDFS-like run before the flush fix: 50454 ms.
  - `Write.AllocateUsec`: 7587 usec total for 49 allocations, down from about 49048728 usec.
  - Client close average: 6668 usec, down from about 1008198 usec.
- 1000 x 1MB write probe after the flush fix:
  - Total time: 18265 ms.
  - `Write.AllocateUsec`: 159286 usec total for 999 allocations.
  - `Write.ChunkWriteUsec`: 16222757 usec total; the remaining dominant cost is chunk write/close, not metaserver allocate.

Crash/restart validation:

- Completed-file full restart passed: wrote `/recovery/ok_8m`, restarted metaserver and all chunkservers without cleaning logs or chunk dirs, then `qfscat` readback matched the original sha256.
- Interrupted writer restart exposed a correctness gap: killing a large `qfsput` left `/recovery/killed_stream` with a namespace size beyond a chunk that was lazy-created and written but not made stable. After restart, chunkservers deleted that dirty chunk as stale, while metaserver still had the chunk mapping; reading failed at offset 536870912 with `no replicas available chunk: 131084`.
- A simple attempt to create lazy chunks as initially stable was rejected by the existing write path (`WRITE_ID_ALLOC` returned `chunk stable`), so this needs a real recovery design rather than a shortcut.

Pending:

- Add proper client-crash recovery semantics for HDFS-like lazy-created chunks. Candidate fixes: lease recovery that makes the last dirty chunk stable, or metaserver-side truncation/mapping cleanup for chunks that never become stable.
- Full 100k-file benchmark if needed; the short and medium probes already verify the 1s allocate stall is fixed.
