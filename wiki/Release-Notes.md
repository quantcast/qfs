# QFS release notes

## QFS version 2.2.7

## New features

1. Support for python3. Native / platform independent python code is now
compatible with python2 and python3, including QFS meta server web UI and meta
server prune / backup script. QFS python module / bindings is only compatible
with python3, and python2 is no longer supported.

2. All required QFS shared libraries are now installed along with QFS python
module. On macOS and linux runtime relative linker paths are now used in order
to make installed QFS shared libraries relocatable. With this explicitly
specifying QFS libraries runtime linkers paths with python module is no longer
required.

## Minor improvements

1. Benchmarks mstress build now uses maven instead of ant, and now is included
in QFS build and tarball by defaults.

2. QFS python module is now built, tested, and included into QFS tarball if
python 3.6 or higher is available.

## QFS version 2.2.6

## New features

1. Added go language bindings.

## Bug fixes

1. Increase default watchdog poll interval by 0.15 to 1.15 seconds in order to
avoid spurious timeouts during idle time due to 1 second default net manager
sleep interval. Change poll interval dependent default parameters calculation
accordingly.

## Minor improvements

1. Additional object store chunk server / AP assignment modes. One is choose AP
with matching rack only (i.e. with no fall back to choosing from all available
APs), and the other is to choose AP with with matching rack only if client rack
is set / known, otherwise falling back to choosing any available AP. For details
please consult metaServer.readUseProxyOnDifferentHostMode and
metaServer.writeUseProxyOnDifferentHostMode parameters descriptions in annotated
meta server configuration file.
2. Use TLS 1.2 with openssl versions prior to 1.1.
3. Implement script to calculate object store space utilization and count number
of files as function of file access time by scanning meta server checkpoint.
4. Compile with openssl 3.0
5. Build on Ubuntu 22.04

## QFS version 2.2.5

## Bug fixes

1. Meta server: keep up to 8K of the most recently received from synchronous
replication channel(s) "future" log blocks while inactive node is in the process
of actively fetching / syncing log and retry merging these log blocks if / when
log fetch "catches up" to the received log blocks sequence numbers. This
mechanism is intended to handle the case when log sync finishes / exits prior to
reaching the most recently received log block due to high RPC rate / meta server
load.

2. Meta server: re-schedule log sync on inactive node when log block sequence
exceeds last log sequence in order to handle the case when log sync stops before
the synchronous replication catches up in order to make inactive node state
synchronization more robust under high meta server load.

3. Meta server: fix object store delete queue cleanup with VR enabled on backups
by removing delayed queue processing logic that could prevent queue emptying on
the primary therefore never issuing queue reset RPC, instead use dumpster
cleanup timer to delay blocks removal. Parse object store tiers parameter and
create a bitmap with tiers in use bits set, then use the bitmap to validate file
create RPC, failing RPCs with tiers not no use. Discard object store block
deletes if tier is not in use instead of re-queueing block delete in order to
prevent invalid / stale blocks from staying in the delete queue indefinitely
therefore preventing emptying the delete queue on the backups potentially
resulting in unbounded queue growth.

4. Meta server: fix extremely rare primary and backup state diversion with chunk
log in flight RPCs in the case where such RPCs are created while processing
chunk server "bye" (teardown) RPC for a different chunk server, for example, and
with chunk server bye RPC for such server pending in replay or transaction log
queue.

5. Meta server: do not create / log extraneous chunk op in flight and chunk op
completion with object store blocks allocation.

6. Meta server: fix view stamped replication reconfiguration swap nodes
sub-command. The bug manifests it self as panic (fail stop) on all active nodes
at the time of the corresponding VR reconfiguration RPC commit, effectively
rendering file system non operational.

7. Meta server: fix VR view change failure that results in panic in the case
when the node that started view change and was about to become primary preempted
(due to timing out or connectivity failure) by another node that has already
transitioned into primary state.

8. Meta server: do not attempt to fetch data from other meta server nodes when
VR ID is not configured, and remove fetch state file, if exists, in such a case.
This change is intended to simplify initial VR configuration setup attempts by
handling operator errors more intuitively / gracefully.

9. Meta server: fix log writer instrumentation by not attempting to save the
instrumentation data into a file if file name set to an empty string.

10. Meta server: fix VR status propagation from logger to main thread by
updating it on every change in order to get gid or spurious status ring debug
instrumentation trace messages.

11. Meta server: do not enter replication check if no chunks exist, i.e. if only
object store used.

12. Chunk server: fix rare failure (panic) due to incorrect handling of IO
buffer manager suspend / granted logic in the client connection state machine
run by the client connections servicing thread.

13. Client library: retry replicated file get size if / when chunk server or
replica disappears.

14. IO library: fix SSL / TLS "filter" error handling with end of file / stream
close. The problem manifests itself with authentication enabled by excessive CPU
utilization due to continuous retries and socket / connection "leak" as, in
theory, connection might never get out of this state.

15. Added support for org.apache.hadoop.fs.FileSystem.getScheme(). This method
is used by Spark NLP, and possibly other packages.

## Minor improvements

1. Meta server: simplify log chunk in flight RPC handling given that now the RPC
can no longer be en-queued after the first teardown attempt when meta chunk
server bye RPC for a given chunk server is en-queued. Implement backward
compatibility handling in replay code path only by adding a boolean to
distinguish chunk in flight RPC created with the new logic.

2. Common library: change watchdog default poll interval to 1 second when max.
timeouts set to negative value in order to sample and report watchdog and poll
entries time overruns.

3. Tools: implement stricter `qfsadmin` command line parameters validation.

4. Implement meta server transaction log truncation / roll back script intended
to be used for debugging and recovery.

5. Add recovery and hitless upgrade sections to QFS Administrator's Guide.

6. Build Hadoop shim for all stable Hadoop releases, include 3.x code line.

## QFS version 2.2.4

## Bug fixes

1. Meta server: fix condition reversal in rename RPC WORM mode specific handling
   logic resulting treating files with .tmp suffix as files with no such suffix
   and the other way around.
2. Fix CentOS builds by updating all RPMs prior to dependencies installation.
3. Update to compile with newer boost version and C++ compiler dialect on MacOS.

## QFS version 2.2.3

## New features

1. Chunk server node ID support. Node ID can be configured on [chunk
   server](https://github.com/quantcast/qfs/blob/7644e583e40ae69851067f53637e1f1381892690/conf/ChunkServer.prp#L84)
   and [QFS
   client](https://github.com/quantcast/qfs/blob/7644e583e40ae69851067f53637e1f1381892690/conf/QfsClient.prp#L169).
   Node ID is intended to be used instead of IP/port for determining if chunk
   server QFS are co-located on the same network node and use chunk server to
   serve client requests.

## Bug fixes

1. Meta server: Change view stamped replication state machine to ignore start
   view change if node's current state is primary, but no log replication
   channels are available / up with the node initiating start view change. Doing
   so is intended to prevent view change / log replication timeout failure loop
   due to possible network communication problem and / or incorrect
   configuration where the node initiating start view change actual log
   replication listener parameters do not match current VR configuration.
2. Chunk server: likely fix for rare intermittent deadlock in record appender --
   handle the case of possible recursive locking by detecting such case and not
   dropping the lock and not attempting to incorrectly reacquire locks.
   Implement debug code instrumentation intended to simulate such case with
   reasonably high probability by introducing small delay / sleep in checksum
   computation.

## QFS version 2.2.2

## Bug fixes

1. Meta server: fix rare intermittent incorrect extra replicas removal that
   could be triggered by re-replication and re-balancing.
2. Chunk server: recover 0 sized chunk files that have smaller size than padded
   header on startup. Such files can appear in the case when host file system
   does not support space pre-allocation and the chunk file was created, and
   then transitioned into stable state with no data / payload, and chunk server
   did no gracefully exist (i.e. exited without closing chunk files). Presently
   0 sized chunk files can appear due to write append failure.

## QFS version 2.2.1

## Bug fixes

1. Fix open socket double accounting in TCP socket accept. The bug manifests
   itself as chunk server IO failures because the counter is used to limit
   number of open sockets.

## QFS version 2.2.0

## New features

1. Symbolic links support.
   In this release QFS client library and Hadoop shim do not support cross file
   systems symbolic links.

## Bug fixes

1. QFS-350 bug fix.
   Ensure that Hadoop configuration parameter fs.qfs.createParams value if
   different than default (S) has effect with Hadoop versions higher than 1.
   Add fs.qfs.create.forceType boolean configuration parameter to allow to set
   force file type parameter in Hadoop configuration.

## QFS version 2.1.3

## Bug fixes

1. Fix DNS resolver's number of open socket accounting bug. This bug might
   manifest itself in at least one non obvious way: chunk server might
   continuously fail to create or open chunk files if/when the open sockets
   counter becomes negative.

## QFS version 2.1.2

## New features

Watchdog thread polls meta and / or chunk server threads and aborts the process,
when configured to do so, in the case if one or more threads appear not to be
making progress due to likely server and / or OS malfunction.

## Bug fixes

1. Fix hex integer parser return code in the case when input length is 0.

2. Chunk server: fix theoretically possible null pointer de-reference, and
   access after free in the record appender and meta server state machines error
   handling code paths.

3. Turn off TLS 1.3 with openssl 1.1.1 by default for PSK only SSL contexts as
   PSK does not appear to work with it, even though openssl documentation
   suggests that TLS 1.2 callbacks are intended to work with 1.3.

4. Meta server: validate chunk server hello rack id, emit error message in the
   case if rack id is outside of the supported range, and mark rack id as
   undefined in order treat it as such consistently everywhere including meta
   server web UI. Annotated configuration files: add valid rack ID range
   definition, and describe handling of rack ids outside of valid range.

5. Meta server: implement debug instrumentation that stores pre-configured
   number of committed RPC status codes, and writes this information into trace
   and optionally separate file log when transitioning out primary VR state.

6. Meta server: change user and group DB load / update to allow assigning
   multiple names to same group numeric ID.

7. Meta server: fix chunk server RPC transmit after re-authentication.

8. Java build: automatically determine if the lowest supported release by java
   compiler is higher than 1.6 and use this release to build QFS java shim.

9. Tools: do not follow symbolic links on local file system in the case of
   recursive traversal and / or fetching directory entries attributes in order
   to make qfs tool behavior more similar to the relevant Unix commands.

10. Client library: fix condition reversal in the chunk lease renew RPC.

11. Update build QFS system to work with newer versions of external / system
    libraries and tools.

## QFS version 2.1.1

## Bug fixes

1. Fix backward compatibility with chunk server 1.x releases by correctly
   handling the case where 1.x chunk server is a replication data source for 2.x
   chunk server. The problem can only occur with mix of 2.x and 1.x chunk server
   versions.

2. Omit linux release patch version from the tar file name in order to make
   links to Travis builds consistent.

3. Update wiki in qfs.git from qfs.wiki.git.

## QFS version 2.1.0

## New features

1. Non blocking DNS resolver. Resolver implementation at
   <https://github.com/wahern/dns> is used by default. It is possible to configure
   QFS to use OS DNS resolver. DNS related configuration options are described
   in the annotated [configuration
   files](https://github.com/quantcast/qfs/tree/master/conf)
   Non blocking DNS resolver allows higher IO concurrency with S3 \[compatible\]
   object store.

2. Basic DNS query result cache. The cache is on by default only for S3 object
   store. The default cache timeout is 1 second. The cache is intended to improve
   S3 object store IO performance and reduce DNS servers load.

## Bug fixes

1. Fixed homebrew osxfuse build.

2. Fixed client authentication in the case when meta server configured
   with no "client" threads.

3. Fixed file system URL parsing in QFS tool.

## Upgrade from 2.0. release

The 2.0.1 release is backward and forward compatible with 2.0 release.

## QFS version 2.0

## New features

1. Meta server replication (VR) is the major new feature in this release. Meta
   server replication provides automatic meta server fail over. With meta server
   replication configured QFS does not have single point of failure.

2. Create exclusive, make directory, remove, remove directory, rename operation
   are guaranteed to be applied only once in the presence of communication
   errors. (Idempotent RPCs).

3. Partial chunk server inventory synchronization.
   The goal is to reduce chunk servers re-connect to the meta server time with
   large chunk inventory.
   Chunk servers now transmit only list of non-stable / writable chunk IDs, and
   chunk IDs of the RPCs in flight at the time of last disconnect, instead of
   transmitting all "hosted" chunk ids.
   For example for file system with 2 billion chunks full inventory
   synchronization requires around 30 minutes, while partial inventory
   synchronization typically can complete in a few seconds.

4. "Off line" fsck can optionally emit the same report "on line" fsck, as chunk
   inventory stored in file system meta data (checkpoint, and transaction logs).

## Notable changes, and bug fixes

1. New version of QFS protocol with more compact RPC representation to minimize
   network overheads, and CPU utilization.

2. QFS client read and write support larger than 2GB buffers.

3. QFS client write pipelining fixed. Now write path with adequately large
   write behind is no longer latency bound.

4. Fixed RS (striped) sparse files recovery.

5. Updated GF complete library version, now includes run time CPU vector
   features detection, and ARM NEON vector instructions support.

6. Fixed sporadic file descriptor close in meta server checkpoint write in case
   when lock file was configured / used.

7. Fixed bug in S3 block delete state machine that appear in the case when more
   one upload ID returned by S3.

8. Fixed re-authentication bug in chunk, meta servers, and client library, that
   resulted connection stalls / timeouts.

9. Changed file delete, by always moving files to dumpster first, then deleting
   file after one lease interval after the most recent read of write lease
   relinquish or expiration.

10. File or directory creation in the dumpster directory is not permitted. Only
    permit super user to move file out of dumpster in order to prevent its
    deletion.

11. File delete scheduled at lower / background priority, avoid delete "bursts"
    in order to maintain low RPCs service latency.

12. Chunk delete queue used with file truncate to reduce chunk delete bursts
    with large files, by scheduling chunk deletes at lower than client requests
    priority.

13. More compact checkpoint and transaction log format, in order to reduce
    disk, network bandwidth, and CPU utilization.

14. Added S3 option that allows to turn off upload ID querying prior to
    S3 block delete, in order to allow to use potentially more cost effective
    external to QFS process of finding and removing possible stale multi part
    uploads.

15. Meta server crypto keys are now always stored in checkpoint and transaction
    log. Configuration parameter metaServer.cryptoKeys.keysFileName is
    deprecated.

16. WROM mode configuration parameter `metaServer.wormMode` is deprecated, and
    has no effect. WORM mode is now stored in checkpoint and transaction logs.
    `logcompactor` has an option to set worm mode when converting checkpoint and
    transaction log into new format. `qfsadmin` or `qfstoggleworm` can be used
    to change WORM mode.

17. Retry QFS client directory listing in the case of parse errors in order to
    handle possible network errors.

18. Fix integer overflow in IO buffer pool with pool size equal or greater 4GB.
    The problem affects both chunk and meta servers. However, typically, only
    chunk server, if configured with IO buffer pool larger than 4GB, and S3,
    might use enough buffers for the problem to occur.

19. Implemented files and directories access time update. By default access time
    update turned off. For details please see the following parameters
    description in meta server annotated configuration file:
    metaServer.ATimeUpdateResolution and metaServer.dirATimeUpdateResolution.

## Upgrade from prior releases

Meta server checkpoint and transaction log segments must be converted to new
format. `logcompactor` can be used to convert file system meta data. Please
consult [[Administrator's-Guide]] for details.
