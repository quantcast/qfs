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
   https://github.com/wahern/dns is used by default. It is possible to configure
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

## Upgrade from 2.0. release.

The 2.0.1 release is backward and forward compatible with 2.0 release.

## QFS version 2.0.

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
