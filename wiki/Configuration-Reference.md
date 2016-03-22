Configuration Reference
=======================

Client Tool
-----------
The following parameters may be included in the configuration file passed to the
qfs client tool (e.g. `bin/tools/qfs`).

| parameter                               | default  |  description                                             |
| ---------                               | -------  |  -----------                                             |
| fs.msgLogWriter.logLevel | INFO | trace log level, one of the following: DEBUG, INFO, NOTICE, ERROR, CRIT, ALERT, FATAL
| fs.trash.minPathDepth | 5 | file or directories that path depth less than this value will not be moved into trash unless dfs.force.remove=true
| fs.trash.current | Current | the name of the current trash checkpoint directory
| fs.trash.homesPrefix | /user | home directories prefix
| fs.trash.interval | 60 | interval in seconds between emptier runs
| fs.trash.trash | .Trash | the name of the trash directory
| dfs.force.remove | false | see fs.trash.minPathDepth
| fs.euser | process effective id | Numeric effective user id
| fs.egroup | process effective group id | Numeric effective group id

Metaserver
----------

### Static
The following configuration parameters cannot be modified at run time but
require a metaserver restart.

| parameter                               | default  |  description                                             |
| ---------                               | -------  |  -----------                                             |
| metaServer.bufferPool.partionBuffers    | 262144 bytes on 64bit systems / 32768 bytes on 32bit systems | The size of the netowrk I/O buffer pool.  The buffer size is 4K, therefore the amount of memory is 4K * *metaServer.bufferPool.partionBuffers*.  All I/O buffers are allocated at startup.  If memory locking is enabled io buffers are locked in memory at startup. **Note**: This parameter name is not a typo. |
| metaServer.chunkServerPort              |           | Listener TCP port used by the chunk server.                   |
| metaServer.clientPort                   |           | Listener TCP port used by QFS clients.                       |
| metaServer.clientThreadCount            | 0             | The size of the client thread pool.  By default the metaserver does not allocate dedicated client threads. When set to greater than 0, dedicated threads are created to do client network I/O, request parsing, and response assembly. The thread pool size should usually be (at least one) less than the number of CPUs. Client threads help to process large numbers of ["short"] requests, when more CPU is used for context switching, network I/O, request parsing, and response assembly, than for the request processing itself. For example, i-node attribute lookups or write append chunk allocations, which can be satisfied by the write append allocation cache.
| metaServer.clientThreadStartCpuAffinity | -1            | Client thread CPU affinity.  Currently  supported only on linux.  The main thread will be assigned to the CPU at the specified index, then the next client thread will be assigned to the CPU index plus one and so on.  For example with 2 client threads and a starting index of 0, the threads' affinity would be 0, 1, and 2 respectively.  This is useful on hardware with multiple multi-core processors that have shared caches.  Assigning the threads to the same processor might help minimize cache misses.  The default is off. |
| metaServer.createEmptyFs                | 0             | When set to 1 the metaserver will create an empty file system if there is no checkpoint present in *metaServer.cpDir*. The default value of 0 is a safety mechanism to prevent the accidental re-initialization of a file system due to a misconfiguration or the loss of the file system checkpoint file. In such a situation the metaserver will fail to start.  **After initializing a new file system, this entry should be removed or set to 0.** |
| metaServer.cpDir                        |           | Directory where file system image checkpoints are stored. |
| metaServer.defaultLoadUser              | 0             | Default user to set on files and directories which have no ownership associated with them when loaded from checkpoint. |
| metaServer.defaultLoadGroup             | 0             | Default group to set on files and directories that have no group ownership associated with them when loaded from checkpoint. |
| metaServer.defaultLoadFileMode          | 0644          | Default file mode to set on files that have no access permissions set on them when loaded from checkpoint. |
| metaServer.defaultLoadDirMode           | 0755          | Default directory mode to set on directories that have no access permissions set on them when loaded from checkpoint. |
| metaServer.logDir                       |           | Directory where transaction logs are stored. |
| metaServer.maxLockedMemory              | 0             | Sets the maximum number of bytes the metaserver will allocate and locks the the virtual address space of the metaserver into RAM.  This setting can be used to prevent the OS from paging out the file system image to disk. A value of 0 disables this feature. **When running as a non-privledged user on Linux, the locked memory "hard" ulimit must be greater than or equal to the specified value in order for the metaserver to start.** |
| metaServer.rootDirGroup                 | 0             | Root directory gid.  **Used only when the file system is first initialized.** |
| metaServer.rootDirMode                  | 0755          | Root directory permissions. **Used only when the file system is first initialized.** |
| metaServer.rootDirUser                  | 0             | Root directory uid. **Used only when the file system is first initialized.** |

### Dynamic
These parameters can be changed at run time by editing the configuration file
and sending the metaserver process a SIGHUP.

**NOTE:** In order to restore a parameter to its default at run time, the
default value must be explicitly set in the configuration file.  In other
words, commenting out the parameter will not have any effect until a
restart.

| parameter                               | default  | description                                             |
| ---------                               | -------  |  -----------                                             |
| metaServer.assignMasterByIp             | 0             | For write append use the low order bit of the IP address for master/slave assignment.   This scheme works well if the least significant bit of the IP address uniformly distributes masters and slaves within a rack, especially with *metaServer.inRackPlacementForAppend* enabled. The default disables assignment by IP. The obvious downside of using assignment by IP is that assignment then depends on the chunk server connection order. |
| metaServer.auditLogWriter.logFilePrefixes |             | Colon (:) separated file name prefixes to store log segments. |
|  metaServer.auditLogWriter.maxLogFiles  | -1            | Maximum number of log segments.  Default is unlimited. |
| metaServer.auditLogWriter.maxLogFileSize | -1           | Maximum audit log segment size in bytes.  Default is -1 unlimited. |
| metaServer.auditLogWriter.waitMicroSec   | -1           | Maximum time to wait for the log buffer to become available.  When wait is enabled the request processing thread will wait for the log buffer disk I/O to complete. If the disk subsystem cannot keep up with the logging it will slow down the metaserver request processing. Default is -1, do not wait, drop log record instead. |
| metaServer.checkpoint.interval          | 3600 secs     | The time between writing out file system image checkpoints. |
| metaServer.checkpoint.lockFileName      |               | When set the specified lock file will be created and can be used to syncronize access to the metaserver checkpoint and transaction log files with external programs. |
| metaServer.checkpoint.maxFailedCount    | 2             | Maximum number of checkpoint failures before the metaserver will exit with error. |
| metaServer.chekpoint.writeBufferSize    | 16777216      | Checkpoint write buffer size. The buffer size should be adequate with synchronous write mode enabled, especially if journal and data of the host's file system are on the same spinning media device, in order to minimize the number of seeks. |
| metaServer.checkpoint.writeSync          | 1             | Use synchronous mode to write checkpoint, i.e. tell host os to flush all data to disk prior to write system call return. The main purpose is to reduce the number of "dirty" / unwritten pages in the host os vm subsystem / file system buffer cache, therefore reducing memory contention and lowering the chances of paging out the metaserver and other processes with no memory locking. |
| metaServer.chekpoint.writeTimeoutSec    | 7200 secs     | The maximum time a checkpoint write out can take before the metaserver declares write failure. |
| metaServer.chunkServer.chunkAllocTimeout | 40 secs      | Chunk allocation timeout in seconds.|
| metaServer.chunkServer.chunkReallocTimeout | 75 secs     | Chunk reallocation timeout in seconds. |
| metaServer.chunkServer.heartbeatInterval | 30 secs      | Chunk server heartbeat interval. |
| metaServer.chunkServer.heartbeatTimeout | 60 secs       | Time in seconds before a chunk server is marked as dead. |
| metaServer.chunkServer.makeStableTimeout | 330 secs     | Chunk stability timeout. |
| metaServer.chunkServer.replicationTimeout | 330 secs    | Chunk replication timeout. |
| metaServer.chunkServer.requestTimeout   | 600 secs      | Chunk server request timeout. |
| metaServer.chunkServerMd5sums           |               | A whitelist of space separated chunk server md5sums which are allowed to connect to the metaserver.  When left empty any chunk server can connect to the metaserver. The default is an empty string, which allows all connections. |
| metaServer.clientSM.auditLogging        | 0             | All client request headers and response status are logged. The audit log records are null (' 0') separated. The log can be useful for debugging and audit purposes. The logging requires some CPU, but the main resource consumption is that of disk I/O. |
| metaServer.clientSM.maxPendingOps       | 16            | Maximum number of single client connection requests in flight.  Increasing this value may reduce CPU utilization and alleviate "head of the line blocking" when a single client connection is shared between multiple concurrent file readers and writers.  This potentially would be at the cost of reducing "fairness" between the client connections. Increasing this value may also reduce number of context switches and OS scheduling overhead with the "client" threads enabled. The default is 16 if *metaServer.clientThreadCount* is enabled, and 1 otherwise. |
| metaServer.clusterKey                   |               | A string which uniquely identifies the file system.  The metaserver will not accept connections from chunk servers that have a different cluster key set.  This helps ensure multiple QFS metaservers and chunk servers on the same hardware cannot possibly interact.  The default is an empty string. |
| metaServer.defaultUser                  | 0xFFFFFFFF    | Default user to use for a client that does not support user permissions.  Default is no user. |
| metaServer.defaultGroup                 | 0xFFFFFFFF    | Default group to use for a client that does not support group permissions. Default is no group. |
| metaServer.defaultFileMode              | 0644          | Default file mode to use for a client that does not support file permissions. |
| metaServer.defaultDirMode               | 0755          | Default directory mode to use for a client that does not support directory permissions. |
| metaServer.forceEUserToRoot             | 0             | Force effective user to root always.  This effectively turns off permissions control.  The default is 0 which is off. |
| metaServer.getAllocOrderServersByLoad   | 0             | Order chunk replica locations by the chunk load average metric in get allocation responses. The read client logic attempts to use replicas in this order. The default of 0 has the effect of replica locations being shuffled randomly. |
| metaServer.inRackPlacement              | 0             | Enable placing chunk replicas and RS blocks on the same rack to save cross rack bandwidth at the cost of reduced reliability. Useful for temporary / scratch file systems.
| metaServer.inRackPlacementForAppend     | 0             | Enable placing chunk replicas and RS blocks of files opened for appending on the same rack to save cross rack bandwidth at the cost of reduced reliability. Useful for temporary / scratch file systems. |
| metaServer.leaseOwnerDownExpireDelay    | 30 secs       | In cases where the write master disconnects, extend the write lease by n seconds to allow for a re-connect. |
| metaServer.maxDownServersHistorySize    | 4096          | The maximum length of the chunk server reconnect history. |
| metaServer.maxGoodCandidateLoadRatio    | 4             | When allocating a chunk, do not consider chunk servers with a load exceeding average load multiplied by *metaServer.maxGoodCandidateLoadRatio*.|
| metaServer.maxGoodMasterLoadRatio       | 4             | When allocating a chunk, do not consider chunk servers with a load exceeding average "master" chunk server load multiplied by *metaServer.maxGoodMasterLoadRatio* if the chunk server is used as a master (head or synchronous replication chain). |
| metaServer.maxGoodSlaveLoadRatio        | 4             | When allocating a chunk, do not consider chunk servers with a load exceeding average "slave" load multiplied by *metaServer.maxGoodSlaveLoadRatio* if the chunk server is used as slave. |
| metaServer.maxLocalPlacementWeight      | 1.0           | When allocating a chunk, do not consider chunk servers running on the same host as the writer if the average number of chunks opened for write per drive exceeds the average number of chunks opened for write across all disks. |
| metaServer.maxRebalanceRunTime          | 0.03 secs     | Single re-balance scan time limit in seconds. |
| metaServer.maxRebalanceScan             | 1024          | The maximum number of chunks to scan per re-balance run.  Increasing this value will speed up re-balancing but also increase CPU utilization. |
| metaServer.maxRebalanceSpaceUtilThreshold | 0.82        | Space re-balancing threshold.  Move chunks from servers that have a space utilization greater than this percentage. |
| metaServer.maxSpaceUtilizationThreshold | 0.95          | Chunk servers with space utilization exceeding this percentage are not considered as candidates for chunk placement. The default is 95% space utilized. |
| metaServer.maxWritesPerDriveRatio       | 1.5           | When allocating a chunk, do not consider chunk servers with the average number of chunks opened for write per drive (disk) exceeding average number of chunks opened for write across all disks / chunks servers multiplied by *metaServer.maxWritesPerDriveRatio*. |
| metaServer.minChunkservers              | 1             | The minimum number of connected functional chunk servers required for the file system to be functional. |
| metaServer.minRebalanceSpaceUtilThreshold | 0.72        | Space re-balancing threshold. Move chunks to a server with a space utilization less than this percentage. |
| metaServer.msgLogWriter.logLevel        | INFO          | Message log level FATAL, ALERT, CRIT, ERROR, WARN, NOTICE, INFO, DEBUG. The default is DEBUG; for non debug builds with NDEBUG defined, INFO is the default. |
| metaServer.msgLogWriter.logFilePrefixes |               | Colon (:) separated file name prefixes to store log segments. Default is an empty list. The default is to use file name from the command line or if none specified write into file descriptor 2 -- stderror. |
| metaServer.msgLogWriter.maxLogFileSize | -1            | Maximum log segment size in bytes. The default is unlimited. |
| metaServer.msgLogWriter.maxLogFiles     | -1            | Maximum number of log segments to write.  The default is unlimited. |
| metaServer.msgLogWriter.waitMicroSec    | -1            | Maximum time to wait for the log buffer to become available.  When wait is enabled the request processing thread will wait for the log buffer disk I/O to complete. If the disk subsystem cannot keep up with the logging it will slow down the metaserver request processing. Default is -1, do not wait, drop log record instead. |
| metaServer.MTimeUpdateResolution        | 1 sec         | The file modification time update resolution.  Increasing the value will reduce the number of transaction log writes for larger files. |
| metaServer.pastEofRecoveryDelay         | 21600 secs    | Delay recovery for the chunks that are past the logical end of file in files with Reed-Solomon redundant encoding. The delay is required to avoid starting recovery while the file is being written into, and the chunk sizes are not known / final. The writer can stop writing into a file, and the corresponding chunks' write leases might time out and will be automatically revoked. The existing writer logic sets logical EOF when it closes the file; before that the logical file size remains 0 during write. (Unless it is a re-write, which is currently not really supported with RS files). The timeout below should be set to at least to the max. practical file "write" time. Setting the timeout to a very large value will prevent processing the chunks sitting in the replication delayed queue from "abandoned" files, i.e. files that the writer wrote something and then exited without closing the file. The parameter and the corresponding "delay" logic will likely be removed in the future releases and replaced with the write leases renew logic. |
| metaServer.rackPrefixes                 |               | Assign a rack ID to a given chunk server by its IP address or IP address prefix.  This effectively defines chunk placement groups. The default is empty, i.e. use  the rack id assigned in the chunk server's configuration.  Example: `10.6.1. 1  10.6.4.1? 2` would assign all chunk servers with IP addresses beginning with 10.6.1 to rack/group 1 and all IP addresses starting with 10.6.4.1? to rack/group 2. |
| metaServer.rackWeights                  |               | Defines static placement weights of each rack/group.  Useful when racks contain very different numbers of spindles and nodes.  The greater the weight, the more likely a given rack will be chosen for chunk allocations.  Example: `1 1  2 1  3 0.9  4 1.2` sets rack 1's weight to 1, rack 2's weight to 1, rack 3's weight to .9, and rack 4's weight to 1.2. The default empty string sets all rack weights to 1. |
| metaServer.rebalancingEnabled           | 1             | Used to disable space and placement re-balancing.  When enabled the metaserver constantly scans all chunks in the system and checks chunk placement within the replication or RS groups, and moves chunks from chunk servers that are above *metaServer.maxRebalanceSpaceUtilThreshold* to the chunk servers that are below *metaServer.minRebalanceSpaceUtilThreshold*. |
| metaServer.rebalanceRunInterval         | 0.512 secs    | Minimum time between two consecutive re-balance scans. |
| metaServer.recoveryInterval             | 30 secs       | Time the metaserver should wait in seconds after starting before considering the file system to be functional.  This gives the chunk servers a chance to reconnect. |
| metaServer.replicationCheckInterval     | 5 secs        | Time between replication queue scans.  Decreasing this value will increase metaserver CPU utilization. |
| metaServer.rootHosts                    |               | A list of space separated IP addresses of hosts where the root user is allowed.  An empty list means that that root user can connect from any host. |
| metaServer.serverDownReplicationDelay   | 120 sec       | Delay in seconds before recovering or re-replicating chunks upon a chunk server disconnect. |
| metaServer.sortCandidatesByLoadAvg      | 0             | When allocating a chunk, prefer chunk servers with a lower load averages.  For a write intensive file system turning this mode on is recommended.  This parameter is mutually exclusive with *metaServer.sortCandidatesBySpaceUtilization*.  When both values are set true *metaServer.sortCandidatesBySpaceUtilization* takes precedence. |
| metaServer.sortCandidatesBySpaceUtilization | 0         | When allocating a chunk, prefer chunk servers with lower disk space utilization. This parameter is mutually exclusive with *metaServer.sortCandidatesByLoadAvg*.  When both values are set true, this parameter takes precedence. |

Chunk Server
------------

### Static
The following parameters can only be used in the chunk server configuration.

| parameter                               | default  | description                                             |
| ---------                               | -------  |  -----------                                             |
| chunkServer.bufferManager.maxRatio       | 0.4      | The buffer manager portion of all I/O buffers. This value defines the maximum number of I/O buffers that can be used for servicing client requests, chunk re-replication, and recovery as a percentage. |
| chunkServer.chunkDir                     |          | Space separated list of directories where chunks will be stored.  Each directory should correspond to a separate physical disk. A JBOD configuration is recommended allowing QFS to handle failures and striping. |
| chunkServer.clientPort                   |          | Chunk server client port to use. |
| chunkServer.clusterKey                   |          | A unique string identifying the instance of QFS.  This parameter must match the corresponding metaserver's configuration parameter *metaServer.clusterKey *.|
| chunkServer.diskQueue.threadCount        | 2        | The number of I/O threads per chunk directory.  This in effect is the maximum number of allowed disk I/O requests in flight per chunk directory. |
| chunkServer.ioBufferPool.lockMemory      | 0        | Enable or disable I/O buffer memory locking.  See *chunkServer.maxLockedMemory*, but this only applies to the I/O buffer pool. |
| chunkServer.ioBufferPool.partitionBufferCount | 65536 bytes on 32 bit systems / 196608 bytes on 64 bit systems | Defines the maximum size of I/O buffer memory used by the chunk server. The value set here, 128K means 128K * 4K buffer = 512M of buffers. The default values are 64K (128MB) for 32 bit build, and 192K (768MB) for 64 bit build. The optimal amount of memory depends on the number of disks in the system, and the I/O (read, write) concurrency -- the number of concurrent clients. The memory should be increased if a large number of concurrent write appenders is expected. Ideally the disk I/O requests should be around 1MB, thus for each each chunk opened for append at least 1MB of I/O buffers is recommended. |
| chunkServer.maxLockedMemory              | 0 bytes       | Sets the maximum number of bytes the chunk server will allocate and locks the the virtual address space of the metaserver into RAM.  This setting can be used to prevent the OS from paging out chunk server memory to disk. A value of 0 disables this feature. **When running as a non-privileged user on Linux the locked memory "hard" ulimit must be greater than or equal to the specified value in order for the chunk server to start.** |
| chunkServer.metaServer.hostname          |          | The IP address of the metaserver.  **Note, host names should not be used.** |
| chunkServer.metaServer.port              |          | The metaserver port. |
| chunkServer.requireChunkHeaderChecksum   | 0        | Enable or disable chunk header checksum verification.  Set to 1 if no backward compatibility with previous QFS releases is required. When set to 0, the 0 header checksum (all 8 bytes are 0) is treated as no checksum and therefore no chunk file header checksum verification is performed.  The downside of leaving this disabled is that the chunk server might not detect the cases where the host OS zero fills the data during the host file system recovery / journal / transaction log replay, thus a data loss / corruption problem might not be detected. |
| chunkServer.stderr                       |          | Redirect stderr to a specified file.  Normally all log message output is performed by the message writer thread which handles I/O gracefully by dropping log messages. It is recommened that for extra safety this parameter be set to /dev/null in case some library function attempts to write to stderr. |
| chunkServer.stdout                       |          | Redirect stdout to a specified file.  Normally all log message output is performed by the message writer thread which handles I/O gracefully by dropping log messages. It is recommened that for extra safety this parameter be set to /dev/null in case some library function attempts to write to stdout. |

### Dynamic
The following parameters can be overridden dynamically in the metaserver
configuration or included in as part of the chunk server configuration.

| parameter                               | default  | description                                             |
| ---------                               | -------  |  -----------                                             |
| chunkServer.allowSparseChunks           | 1        | Enable or disable support for sparse data chunks. |
| chunkServer.bufferedIo                  | 0        | Controls buffered I/O.  By default the chunk server will bypass the OS buffer cache and instead use direct I/O when supported. It is conceivable that enabling buffered I/O might help with short reads for "broadcast" / "web server" type loads. For the "typical" large I/O (1MB) request sequential type loads, enabling caching will likely lower cluster performance due to higher system (OS) cpu overhead and memory contention. |
| chunkServer.bufferManager.waitingAvgInterval | 20 secs  | Averaging interval for calculating the average wait time the incoming client's requests spend in the I/O buffer wait queue. The average wait time is the value used by the metaserver for chunk placement. |
| chunkServer.chunkPlacementPendingReadWeight  | 0        |The weight of pending disk I/O on chunk placement. If set to 0 or less, the pending I/O (number of I/O bytes in the disk queue) has no effect on the placement of chunks. If the weight is set to greater than 0, then the average pending I/O per chunk directory (host file system / disk) is calculated, as (total_pending_read_bytes * total_pending_read_weight + total_pending_write_bytes * total_pending_write_weight) / chunk_directory_count.  Chunk directories with pending_read + pending_write that exceed the value above are taken out of the consideration for placement. |
| chunkServer.chunkPlacementPendingWriteWeight | 0        |The weight of pending disk I/O on chunk placement. If set to 0 or less, the pending I/O (number of I/O bytes in the disk queue) has no effect on the placement of chunks. If the weight is set to greater than 0, then the average pending I/O per chunk directory (host file system / disk) is calculated, as (total_pending_read_bytes * total_pending_read_weight + total_pending_write_bytes * total_pending_write_weight) / chunk_directory_count.  Chunk directories with pending_read + pending_write that exceed the value above are taken out of the consideration for placement. |
| chunkServer.dirRecheckInterval           | 180 secs | The frequency at which the metaserver will scan for chunk directories that had been marked unavailable to become available. |
| chunkServer.diskIo.maxIoTimeSec          | 270 secs | Disk I/O request timeout in seconds. |
| chunkServer.maxSpaceUtilizationThreshold  | 0.05     | The minimum amount of space that must be available in order for the chunk directories to be used for chunk placement (considered as "writable"), expressed as a percentage of total host file system space. The default is 5%, or in other words, stop using chunk directories which reach 95% space utilization. |
| chunkServer.minFsAvailableSpace         | 67125248 bytes | The minimal amount of space in bytes that must be available in order for a chunk directory to be used for chunk placement (considered writable).  The default is 64MB with a chunk header size of 16KB. |
| chunkServer.msgLogWriter.logLevel       | NOTICE   | Message log level FATAL, ALERT, CRIT, ERROR, WARN, NOTICE, INFO, DEBUG. |
| chunkServer.msgLogWriter.logFilePrefixes |               | Colon (:) separated file name prefixes to store log segments. Default is empty list. The default is to use file name from the command line or if none specified write into file descriptor 2 -- stderror. |
| chunkServer.msgLogWriter.maxLogFileSize  |  -1            | Maximum log segment size in bytes. The default is unlimited. |
| chunkServer.msgLogWriter.maxLogFiles     | -1            | Maximum number of log segments to write.  The default is unlimited. |
| chunkServer.msgLogWriter.waitMicroSec    |  -1            | Maximum time to wait for the log buffer to become available.  When wait is enabled the request processing thread will wait for the log buffer disk I/O to complete. If the disk subsystem cannot keep up with the logging it will slow down the metaserver request processing. Default is -1, do not wait, drop log record instead. |
| chunkserver.rackId                       | -1             | Chunk server's rack id. It has effect only if meta server rack prefixes (metaServer.rackPrefixes) are not set, or chunk server's ip does not match any of the prefix defined in metaServer.rackPrefixes. Default is no rack assigned. |
| chunkServer.recAppender.replicationTimeoutSec | 180 secs      | Record append synchronous replication timeout in seconds.  |
| chunkServer.remoteSync.responseTimeoutSec | 300 secs      | Write replication timeout in seconds.|

Web Monitor
-----------
The web reporting interface can be configured with the following configuration
parameters:

| [webserver] | Description |
| ----------- | ----------- |
|webServer.metaserverHost|The QFS metaserver host name|
|webServer.metaserverPort|The QFS metaserver port|
|webServer.port|HTTP port to connect to web server|
|webServer.docRoot |directory for web server's style sheets, images, etc.|

One should not have to modify the settings in the `[chunk]` section of the web
server config file. Please refer to the sample config used in the example server
for usage.

![Quantcast](//pixel.quantserve.com/pixel/p-9fYuixa7g_Hm2.gif?labels=opensource.qfs.wiki)
