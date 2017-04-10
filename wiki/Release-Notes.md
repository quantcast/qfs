QFS version 2.0.

New features
-------------

1. Meta server replication (VR) is the major new feature in this release. Meta
server replication provides automatic meta server fail over. With meta server
replication configured QFS does not have single point of failure.

2. Create exclusive, make directory, remove, remove directory, rename operation
 are guaranteed to be applied only once in the presence of communication
 errors. (Idempotent RPCs).

3, Partial chunk server inventory synchronization.
 The goal is to reduce chunk servers re-connect to the meta server time with
 large chunk inventory.
 Chunk servers  now transmit only list of non-stable / writable chunk IDs, and
 chunk IDs of the RPCs in flight at the time of last disconnect, instead of
 transmitting all "hosted" chunk ids.
 For example for file system with 2 billion chunks full inventory
 synchronization requires around 30 minutes, while partial inventory
 synchronization typically can complete in a few seconds.
 
4. "Off line" fsck can optionally eimit the same report "on line" fsck, as chunk
 inventory stored in file system meta data (checkpoint, and transaction logs).

Notable performance improvements and bug fixes
----------------------------------------------

1. New version of QFS protocol with more compact RPC representation to minimize
 network overheads, and CPU utilization.

2. Implement QFS client read and write support for larger than 2GB IOs / buffers.

3. QFS client write pipelining fixed. Now write path with adequately large
write behind is no longer latency bound.

4. Fix RS sparse files recovery.

5. Updated GF complete library version, now includes run time CPU vector features
 detection, and ARM NEON vector instructions support.

6. Fix sporadic file descriptor close in meta server checkpoint path in case
 when lock file was configured / used.

7. Fix bug in S3 block delete state machine that appear in the case when more
 single upload ID was returned by S3.

8. Fix re-authentication bug in chunk, meta servers, and client library, that
 resulted connection stalls / timeouts.

9. Change file delete, by always moving files to dumpster first, then deleting
 file after one lease interval after the most recent read of write lease
 relinquish or expiration.
 
10. Do not allow file or directory creation in the dumpster directory. Only
permit super user to move file out of dumpster in order to prevent its deletion.

11. Schedule file delete at lower / background priority, avoid delete "bursts"
 in order to maintain low RPCs service latency.

12. Implement chunk delete queue used with file truncate to reduce chunk delete
 bursts with large files, by scheduling chunk deletes at lower priority.
 
13. More compact checkpoint and transaction log format, in order to reduce
 disk, network bandwidth, and CPU utilization.
 
14. Implement S3 option that allows to turn off upload ID querying prior to
 S3 block delete, in order to allow to use possibly more cost effective
 external to QFS process of finding and removing possible stale multi part
 uploads.

Upgrade from prior releases
---------------------------

Meta server checkpoint and transaction log segments must be converted to new
format. `logcompactor` can be used to convert file system meta data. Please
consult [[Administrator's-Guide]] for details.
