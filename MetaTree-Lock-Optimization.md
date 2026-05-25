# MetaTree Lock Optimization Plan

## Background

The create-file benchmark shows poor scaling when increasing
`metaServer.clientThreadCount`. In the measured runs, `INFO + 4` client threads
was faster than `INFO + 20`, which points to lock contention inside the meta
server rather than chunkserver or data path bottlenecks.

For empty-file create, the hot path is:

1. `ClientThread::DispatchStart()`
2. `submit_request()`
3. `MetaRequest::SubmitBegin()`
4. `LogWriter::Enqueue()`
5. `MetaCreate::start()`
6. `MetaCreate::handle()`
7. `Tree::create()`
8. `LogWriter::ScheduleFlush()`

`Tree::create()` mutates the global metadata tree by doing lookup, optional
remove, fid allocation, dentry/fattr insertion, and count updates.

## Current Locking Problem

Before this optimization, client threads held the global net dispatch mutex while
processing pending metadata requests:

```text
dispatch mutex
  submit_request()
    MetaRequest::Submit()
      LogWriter::Enqueue()
      MetaCreate::handle()
        metatree.create()
  LogWriter::ScheduleFlush()
```

This makes `metaServer.clientThreadCount` scale poorly. More client threads
mainly increase contention on the same dispatch mutex.

## Important Constraint

The metadata tree is not currently safe for simple per-parent-directory locking.
It is implemented as a single global B-tree. Even creates in different parent
directories can modify shared B-tree nodes, split internal nodes, update the
root, or touch shared indexes.

Therefore, this is unsafe as a direct first step:

```text
lock(parent_dir)
  Tree::create(parent_dir, name)
```

That would protect directory-level semantics but not the global B-tree data
structure.

## Implemented First Step

The first step separates the broad dispatch lock from metadata mutation and log
writer state.

### 1. Add a dedicated metadata request mutex

`submit_request()` now takes a dedicated metadata processing mutex before calling
`MetaRequest::Submit()`.

This preserves existing metatree safety while removing metadata processing from
the net dispatch mutex.

```text
meta request mutex
  MetaRequest::Submit()
    MetaCreate::handle()
      metatree.create()
```

### 2. Shrink dispatch mutex scope

`ClientThread::DispatchStart()` now keeps the dispatch mutex only around fork
coordination and auth context update. It does not hold the dispatch mutex while
processing the request batch.

This changes the lock shape to:

```text
dispatch mutex
  PrepareToFork()
  auth context update
  ForkDone()

meta request mutex
  submit_request()
```

### 3. Protect LogWriter state with LogWriter mutex

Moving request processing out of the dispatch mutex means LogWriter can no
longer rely on dispatch serialization. The following paths now explicitly use
`LogWriter::mMutex`:

```text
LogWriter::Enqueue()
LogWriter::RequestCommitted()
LogWriter::ScheduleFlush()
```

This protects pending queues, commit state, and flush scheduling when multiple
client threads reach the log writer concurrently.

## What This Does Not Yet Solve

This first step does not make `Tree::create()` itself parallel across parent
directories. It deliberately keeps metadata mutation serialized through the
metadata request mutex.

The goal is to remove one oversized outer lock and introduce clearer lock
ownership:

```text
dispatch state -> dispatch mutex
metadata mutation -> metadata request mutex
log writer state -> log writer mutex
```

This is a safe prerequisite for deeper metatree concurrency work.

## Next Steps Toward True MetaTree Concurrency

### Step 1: Add profiling around the new lock boundaries

Measure:

```text
dispatch mutex wait / hold time
metadata request mutex wait / hold time
LogWriter mutex wait / hold time
Tree::create() latency
Tree::lookup() latency
Tree::link() latency
log flush batch size and latency
```

This confirms whether contention moved from dispatch mutex to metadata request
mutex or LogWriter.

### Step 2: Split read-only and mutation requests

Introduce a metadata operation classification:

```text
read-only ops
mutation ops
log-dependent mutation ops
```

Read-only requests can eventually run under a shared/read lock, while mutation
requests keep exclusive protection.

### Step 3: Refactor metatree storage for sharding

True per-directory create parallelism needs the data structure to stop using one
global mutable B-tree for all dentries/fattrs.

Candidate direction:

```text
fid/fattr index: separately protected or sharded by fid
dentry index: sharded by parent fid
path cache: separately protected or disabled on mutation-heavy workloads
directory counters: parent-chain locking with stable lock ordering
```

Only after this split is it safe to use parent-directory locks for create.

### Step 4: Add parent-directory locking

Once dentry storage is sharded by parent fid:

```text
lock(parent_dir)
  check permissions
  lookup child name
  allocate fid
  insert dentry in parent shard
  insert fattr in fid shard
  update parent counters
```

Lock ordering must be explicit. Rename is the main hard case because it touches
two parent directories and may update path cache and subtree invariants.

### Step 5: Validate replay and transaction ordering

Create is a logged operation. Any concurrency change must preserve:

```text
log sequence order
fid seed replay correctness
idempotent request behavior
rename/create/remove ordering
checkpoint consistency
```

Log ordering can remain serialized even if independent metatree mutations become
parallel internally.

## Verification

Current first-step build verification:

```bash
cmake --build bld --target metaserver -j4
```

The target builds successfully.

## Risk Notes

The safe first step may improve throughput if the dispatch mutex was the main
contention point. If the bottleneck is now the metadata request mutex or log
writer, throughput may not improve significantly.

Do not replace the metadata request mutex with a parent-directory lock until the
global B-tree has been refactored or otherwise proven safe for concurrent
mutation.
