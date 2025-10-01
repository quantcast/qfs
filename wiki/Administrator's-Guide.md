# QFS Admistrator's Guide

## Metaserver

The metaserver is responsible for storing all global QFS file system
information, tracking chunk locations, and coordinating chunk
replication/recovery. This section will discuss basic metaserver
administration. Configuration

The metaserver configuration is normally stored in a file called
`MetaServer.prp`. The [[Deployment Guide]] includes several minimal sample
configurations. For the complete set of configuration parameters see the
[[Configuration Reference]].

### Running Meta Server

```sh
metaserver /path/to/MetaServer.prp
```

To initialize file system by creating initial empty file system's checkpoint and
log segment -c command line option can be used:

```sh
metaserver -c /path/to/MetaServer.prp
```

-c option should **not** be used when running meta server with existing QFS file
system, in order to prevent data loss in the case when meta server starts
with no "latest" checkpoint file.

### Checkpoint and Transaction Log Pruning

The directories that store metaserver checkpoints (*metaServer.cpDir*) and
transaction logs (*metaServer.logDir*) are pruned periodically by the meta server; otherwise they
will fill up and run out of space. Pruning parameters are described in meta server [annotated configuration file](https://github.com/quantcast/qfs/blob/master/conf/MetaServer.prp) section "Meta data (checkpoint and trasaction log) store."

Checkpoint and log pruning scrips required by the prior versions are now obsolete have been removed.

### Creating Backups

From time to time the *metaServer.cpDir* and *metaServer.logDir* should be
backed up, as they can be used to restore a file system which has had a
catastrophic failure. A backup consists of:

- The **latest** checkpoint file in *metaServer.cpDir*
- **ALL** of the transaction logs in *metaServer.logDir*

The simplest way to back up a file system image is to use `tar` to archive these
files.

Given the following configuration:

```properties
metaServer.cpDir = /home/qfs0/state/checkpoint
metaServer.logDir = /home/qfs0/state/transactions
```

A possible solution would be to periodically do the following:

```sh
tar --exclude '*.tmp.??????' \
    -czf /foo/bar/qfs0-backup-"$(date +%d-%H.tar.gz)" \
    checkpoint transactions -C /home/qfs0/state
```

**Note**: this simple script includes all checkpoint files, which is
inefficient; only the latest checkpoint file is required for the backup.

[QFS meta data backup script is available here](https://github.com/quantcast/qfs/blob/master/scripts/qfs_backup).

### Restoring from Backups

To restore a backup, it need only be extracted to the appropriate
*metaServer.cpDir* and *metaServer.logDir* directories of a fresh metaserver
head node.

Using the configuration from the previous example:

```sh
cd /home/qfs0/state && tar -xzf /foo/bar/qfs0-backup-31-23.tar.gz
```

Once the metaserver is started, it will read the latest checkpoint into memory
and replay any transaction logs. Files that were allocated since the backup
will no longer exist and chunks associated with these files will be deleted.
Files that have been deleted since the backup, however, will show up as lost (as
their chunks will have been deleted) but the restored file system image will
still reference them. After a restore, you should run a file system integrity
check and then delete the lost files it identifies. Lastly, any other file
modifications since the backup will be lost.

**Note:** The location of the *metaServer.cpDir* and *metaServer.logDir* should
not change.

### Meta Server Replication (VR)

Meta server replication provides fault tolerance at the meta server level.
The file system can be configured with multiple meta server nodes, in order to
solve single point of failure problem.
Meta server replication design  based on Veiwstamped replication (VR) [paper](http://pmg.csail.mit.edu/papers/vr-revisited.pdf).
The minimal configuration requires 3 meta server nodes. At least 3 nodes are
needed to solve "network partitioning" problem. This problem occurs when some
subset(s) of meta server nodes cannot communicate with another
subset(s) of meta server nodes. In such case a subset of nodes larger than or
equal to N/2 + 1 (a.k.a. quorum), if exists, automatically selected to service
requests. The problem description can be found in the VR paper.

Configuring replicated meta server group / cluster consists of the following
steps.

1. Decide on the number of meta server nodes N. The minimum N is 3. The
   maximum number of tolerated nodes failures is N - (N/2 + 1). QFS clients and
   chunk servers automatically re-connect to newly elected primary node in the
   case when prior primary node becomes unavailable due to node or network
   connectivity failure(s).
2. Assign node IDs. The node ID must be non negative 63 bit integer. Initial
   set of nodes must have node with ID 0. The node with lowest ID is elected as a
   primary. The remaining active nodes are assigned backup status. Node's
   "primary order" (32 bit signed integer) can be used to change primary
   election. The node with the smallest primary order becomes primary. Node's
   primary order takes precedence over node ID. The node ID breaks tie in case when
   primary orders are equal. In initial configuration all nodes primary order
   must be set to 0. Node primary order is VR configuration parameter, it can be
   changed with qfsadmin vr_reconfiguration command.
   **Note that the node ID must be unique, and should never be re-used. Non unique
   IDs withing the same meta server group / file system can result file system
   loss.**
3. Configure meta data fetch / sync, and log receiver listeners. The relevant
   parameters are described in "Meta meta data initial fetch / synchronization"
   section of the meta server annotated configuration file.
4. Copy or create new file system checkpoint and transaction log segments
   on/to the node with ID 0, and ensure that other nodes have empty checkpoint
   and transaction log directories. Specify file system ID in the meta server
   configuration file with metaServer.metaDataSync.fileSystemId parameter.
   File system ID can be obtained from the beginning of the checkpoint
   file: the first number on the line with the "filesysteminfo/fsid/" prefix.
5. Create DNS record with the list of meta server nodes IP addresses, or add
   meta server nodes IP addresses to the client configuration file by using
   client.metaServerNodes parameter.
   Chunk servers, similarly to QFS client, need to be configured with the meta
   server DNS name, and / or list of meta server nodes network locations.
   Please see chunkServer.meta.nodes parameter description in chunk server
   configuration file.
6. Start meta server on all meta server nodes. The nodes with non 0 IDs should
   fetch checkpoint and transaction log from node with ID 0.
7. Use `qfsadmin vr_reconfiguration` command to configure replication. VR
   configuration stored in the checkpoint and transaction log, and replicated
   onto all meta server nodes.

The first step is to add all nodes with their respective transaction log
listeners network addresses [locations], the second is to activate nodes. Meta
server should respond to vr_reconfiguration without arguments with the
command description.

QFS admin configuration file has the same format and parameters as QFS client
configuration file. Typically configuration file describes authentication
configuration. With no authentication configured the configuration
file is required in the case when parameter client.metaServerNodes is used
instead of DNS meta server host A or AAAA records to associate all meta server
nodes IP addresses with the meta server host.

Admin tool needs to be told of all meta server nodes; it is just a special QFS
client. Admin tool will find the node that is currently primary (for initial
configuration it is always be the node with ID 0), and then send RPCs to the primary
node. The primary node will, in turn, replicate VR reconfiguration RPC to the
secondaries nodes, the same way as it replicates non admin RPCs.

In the case when meta server nodes connected with more than one network links /
IP addresses, multiple redundant network connections can be configured to
increase connectivity reliability by specifying list of log listeners' network
locations (IP address and port).

Please note that DNS name that lists all meta server nodes is to be used with
qfsadmin commands in the example below.

### Configuring VR example

Add node 0 to VR configuration:

```sh
qfsadmin -f qfsadmin.cfg \
    -s meta-server-host \
    -p meta-server-port \
    -F op-type=add-node \
    -F arg-count=1 \
    -F node-id=0 \
    -F args='node0-ip-address node0-log-listener-port-number' \
    vr_reconfiguration
```

Add node 1 to VR configuration:

```sh
qfsadmin -f qfsadmin.cfg \
    -s meta-server-host \
    -p meta-server-port \
    -F op-type=add-node \
    -F arg-count=1 \
    -F node-id=1 \
    -F args='node1-ip-address node1-log-listener-port-number' \
    vr_reconfiguration
```

Add node 2 to VR configuration:

```sh
qfsadmin -f qfsadmin.cfg \
    -s meta-server-host \
    -p meta-server-port \
    -F op-type=add-node \
    -F arg-count=1 \
    -F node-id=2 \
    -F args='node1-ip-address node2-log-listener-port-number' \
    vr_reconfiguration
```

Activate nodes:

```sh
qfsadmin -f qfsadmin.cfg \
    -s meta-server-host \
    -p meta-server-port \
    -F op-type=activate-nodes \
    -F arg-count=3 \
    -F args='0 1 2' \
    vr_reconfiguration
```

Once VR configured, changing VR configuration does not require
file system downtime. New meta server nodes can be added to the configuration,
existing nodes can be removed, or replaced with new nodes.

Adding and removing node consists of two steps. The first step is to
add node, or inactivate node. The second step is to either activate or remove
or replace (swap with currently active) node. The two step process allows for
new node to synchronize its state with active nodes, and verify node
connectivity prior to its activation, in order to minimize re-configuration
time, and downtime due possible operator error.

With default parameters system switch over time (new primary election, and
chunk servers and clients connecting to the new primary) should be around
10 seconds.

### VR Status

`qfsadmin vr_get_status` can be used to query VR status of the file system or
status of specific meta server node.(with -n parameter)
For example:

```sh
qfsadmin -s meta-server-host -p meta-server-port vr_get_status
```

The output will look like the following. The node state will be primary (status
 might be backup when querying specific node), if everything is OK.

```console
vr.nodeId: 0
vr.status: 0
vr.active: 1
vr.state: primary
vr.primaryId: 0
vr.epoch: 5
vr.view: 71
vr.log: 5 71 120444929
vr.commit: 5 71 120444929
vr.lastViewEnd: 5 69 1258
vr.quorum: 2
vr.ignoreInvalidVrState: 0
vr.fileSystemId: 160517748448112759
vr.clusterKey: mrs-kfs-sort-b
vr.metaMd5: b6f2a25365e37c75b7fdf12914d2392d
vr.viewChangeReason: restart, node: 2
vr.viewChangeStartTime: 1491775660
vr.currentTime: 1491877576

logTransmitter.channel.0.location: 10.6.46.38 30200
logTransmitter.channel.0.id: 2
logTransmitter.channel.0.receivedId: 2
logTransmitter.channel.0.primaryId: 0
logTransmitter.channel.0.active: 1
logTransmitter.channel.0.ack: 5 71 120444929
logTransmitter.channel.0.sent: 5 71 120444929
logTransmitter.channel.1.location: 10.6.34.1 30200
logTransmitter.channel.1.id: 1
logTransmitter.channel.1.receivedId: 1
logTransmitter.channel.1.primaryId: 0
logTransmitter.channel.1.active: 1
logTransmitter.channel.1.ack: 5 71 120444929
logTransmitter.channel.1.sent: 5 71 120444929
logTransmitter.channel.2.location: 10.6.47.1 30200
logTransmitter.channel.2.id: 0
logTransmitter.channel.2.receivedId: 0
logTransmitter.channel.2.primaryId: 0
logTransmitter.channel.2.active: 1
logTransmitter.channel.2.ack: 5 71 120444929
logTransmitter.channel.2.sent: 5 71 120444929

logTransmitter.activeUpNodesCount: 3
logTransmitter.activeUpChannelsCount: 3

configuration.primaryTimeout: 4
configuration.backupTimeout: 8
configuration.changeViewMaxLogDistance: 65536
configuration.maxListenersPerNode: 16
configuration.node.0.id: 0
configuration.node.0.flags: 2
configuration.node.0.active: 1
configuration.node.0.primaryOrder: 0
configuration.node.0.listener: 10.6.47.1 30200
configuration.node.1.id: 1
configuration.node.1.flags: 2
configuration.node.1.active: 1
configuration.node.1.primaryOrder: 0
configuration.node.1.listener: 10.6.34.1 30200
configuration.node.2.id: 2
configuration.node.2.flags: 2
configuration.node.2.active: 1
configuration.node.2.primaryOrder: 0
configuration.node.2.listener: 10.6.46.38 30200
```

Meta server web UI can also be used to obtain VR status.

### Removing VR configuration

VR (meta server replication) configuration stored in checkpoint and transaction
logs. It is not possible to remove / reset VR configuration at run time, i.e.
without downtime while servicing requests.
The following two meta server command line options allow to clear VR
configuration or inactivate all meta server nodes:

1. -clear-vr-config         -- append an entry to the end of the transaction log
   to clear VR configuration, and exit
2. -vr-inactivate-all-nodes -- append an entry to the end of the transaction log
   to inactivate all VR nodes, and exit

### Recovering File System by Truncating Transaction Log

The [`qfsmetalogtruncate.sh`](https://github.com/quantcast/qfs/blob/master/scripts/qfsmetalogtruncate.sh)
can be used to recover file system in case if the meta server state diverges
between run time and replay due to a hardware malfunction (for example memory
error undetected by ECC parity check) or due to a software bug.
In VR configuration if such a problem occurs on the primary, it would manifest
itself as fail stop ("panic") on all backup nodes. In such a case the backup
nodes will exit, and the primary might still be up. The backup nodes would have
error message in the trace log similar to the following:

```console
01-01-2022 01:23:45.678 ERROR - (Replay.cc:2298) error block seq: 11381:4203867266:c/3 1 8323a816/a5f956e40/5b15bf44/0/3 1 8323aa10/116/2c75/56b5fa23
```

The characters between `c/` and the next `/` symbol represent log commit
sequence number in hex. In the above example the commit sequence number is
`3 1 8323a816`. To recover from such an error all meta server nodes in VR
configuration must be shutdown and then the transaction log on all those nodes
can be truncated to the log block that immediately precedes the block with the
sequence number shown in the error message.

For example given the sequence number in the error message the above the
following can be executed on each and every relevant meta server node:

```sh
qfsmetalogtruncate.sh
    -l /home/qfs0/state/transactions
    -c /home/qfs0/state/checkpoint
    -s '3 1 8323a816'
```

By default the script creates a backup of both meta server transaction log and
checkpoint directories. The backup is done by adding current unix time to the
respective directories names, creating these directories in the parent directory
of the original directory and then hard linking checkpoints and log segments
into the backup directories. Backup can be turned off by adding `-b` option. If
the scrips runs as root, it will preserve original files and directories
ownership (user and group).

### Hitless Meta Server Version Upgrade

Unless specifically mentioned in release notes, QFS meta server versions are
backward compatible. Backward compatibility allows to perform "hitless" (no
downtime) upgrade of the meta nodes in VR configuration. However hitless
downgrade / roll back is not possible when new features are introduced and / or
RPC log format changes between versions.

To perform hitless upgrade new meta server executable's md5sum need to be added
to the configuration, in case if `metaServer.metaMds` configuration parameter is
used / not empty. The upgrade must be performed in strict order from the
node with has highest primary VR order and ID to the lowest primary order and
ID. VR primary order takes precedence over VR ID, i.e. node IDs are compared
only in case if primary orders are equal. Default primary order for all nodes is
0, therefore unless primary order explicitly configured the upgrade order
matches the assigned ID order -- from the highest ID to the lowest ID.

Prior to upgrade ensure that all meta server nodes in VR configuration are up
and running or at least more than quorum (half plus one) nodes are up and
operational, as otherwise node restart would render the file system non
operational until node restart completes. Meta server nodes must be restarted
one at a time and the next node should not be restarted until the prior node
restart successfully completes until quorum of nodes is running new version.
Then the remaining nodes should be updated and restarted simultaneously in order
to minimize the time window where quorum of nodes is running new version and the
older version is running on the remaining nodes.

Examples:

1. Upgrade meta server in VR configuration of 3 nodes with ids 0, 1, 2, primary
   order is 0 for all nodes. Quorum is 3/2+1 = 2. Restart node with ID 2, wait
   until it is operational and joins VR. Repeat the prior step for node with ID 1.
   Upgrade and restart node 0.
2. Upgrade meta server in VR configuration of 5 nodes with ids 0, 1, 2, 3, 4,
   primary order is 0 for all nodes. Quorum is 5/2+1 = 3. Restart node with ID 4,
   wait until it is operation and joins VR. Repeat the prior step for node with ID
   3, then for node with ID 2. Upgrade and restart nodes 1 and 0 simultaneously.

### File System Integrity (`qfsfsck`)

The `qfsfsck` tool can be employed in three ways:

- Verify the integrity of a running file system by identifying lost files and/or
  files with chunk placement/replication problems.
- Validate the active checkpoint and transaction logs of a running file system.
- Check the integrity of a file system archive/backup (checkpoint plus a set of
  transaction logs).

### Running File System Integrity Verification

In order to verify the integrity of a running file system by identifying lost
files or files with chunk placement problems, run:

```sh
qfsfsck -m metaServer.hostname -p metaServer.port
```

The output will look something like this if everything is okay:

```console
Lost files total: 0
Directories: 280938
Directories reachable: 280938 100%
Directory reachable max depth: 14
Files: 1848149
Files reachable: 1848149 100%
Files reachable with recovery: 1811022 97.9911%
Files reachable striped: 34801 1.88302%
Files reachable sum of logical sizes: 37202811695550
1  Files reachable lost: 0 0%
2  Files reachable lost if server down: 0 0%
3  Files reachable lost if rack down: 0 0%
4  Files reachable abandoned: 0 0%
5  Files reachable ok: 1848149 100%
File reachable max size: 4011606632
File reachable max chunks: 128
File reachable max replication: 3
Chunks: 19497647
Chunks reachable: 19497647 100%
Chunks reachable lost: 0 0%
Chunks reachable no rack assigned: 0 0%
Chunks reachable over replicated: 0 0%
Chunks reachable under replicated: 0 0%
Chunks reachable replicas: 22715209 116.502%
Chunk reachable max replicas: 3
Recovery blocks reachable: 1858706
Recovery blocks reachable partial: 0 0%
Fsck run time: 6.45906 sec.
Files: [fsck_state size replication type stripes recovery_stripes
stripe_size chunk_count mtime path]
Filesystem is HEALTHY
```

When there are lost or abandoned files, and/or files with placement problems,
they will be placed in one of the four categories listed below. The number
before each category indicates the `fsck_state` associated with that category.

`1  Files reachable lost: 0 0%` # files lost, these files cannot be recovered
`2  Files reachable lost if server down: 0 0%` # these files could be lost with one chunk server down
`3  Files reachable lost if rack down: 0 0%` # this files could be lost with a rack down
`4  Files reachable abandoned: 0 0%` # file which failed to be allocated, automatically pruned

Each problem file will also be listed, prefixed by the `fsck_state` and several
other attributes, followed by the header line seen below:

`Files: [fsck_state size replication type stripes recovery_stripes stripe_size chunk_count mtime path]`

The `fsck_state` identifies which problem category a given file is in. For
example:

```console
1 64517075 2 2 128 0 65536 128 2012-09-22T11:36:40.597073Z /qfs/ops/jarcache/paramDB_9f68d84fac11ecfeab876844e1b71e91.sqlite.gz
3 56433403 2 2 128 0 65536 128 2011-10-05T15:02:28.057320Z /qfs/ops/jarcache/paramDB_7912225a0775efa45e02cf0a5bb5a130.sqlite.gz
3 55521703 2 2 128 0 65536 128 2012-08-28T15:02:07.791657Z /qfs/ops/jarcache/paramDB_f0c557f0bb36ac0375c9a8c95c0a51f8.sqlite.gz
```

means there is one completely lost file, and two other files that could be lost after the failure of a single rack.

### Active Checkpoint and Transaction Logs

In order to validate the checkpoint and transaction logs of a running
metaserver, the *metaServer.checkpoint.lockFileName* parameter must be
configured in the metaserver (as it is used to synchronize access to the
checkpoint files and transaction logs). The lock file, if specified in
*metaServer.checkpoint.lockFileName*, will be created when the second checkpoint
is created.

**Note**: `qfsfsck` will attempt to load the file system image into memory, so
make sure there is enough memory available on the head node to do this.

To run this check:

```sh
qfsfsck -L metaServer.checkpoint.lockFileName -l metaServer.logDir -c metaServer.cpDir
```

### File System Check Example

Given the following configuration:

```console
metaServer.checkpoint.lockFileName /home/qfs0/run/ckpt.lock
metaServer.cpDir = /home/qfs0/state/checkpoint
metaServer.logDir = /home/qfs0/state/transactions
```

the check would be executed like so:

```sh
qfsfsck -L /home/qfs0/run/ckpt.lock \
    -l /home/qfs0/state/transactions \
    -c /home/qfs0/state/checkpoint
```

If everything is okay, the output will look something like this:

```console
09-25-2012 20:39:01.894 INFO - (restore.cc:97) restoring from checkpoint of 2012-09-25T20:00:26.971544Z
09-25-2012 20:39:01.894 INFO - (replay.cc:63) open log file: /home/qfs0/state/transactions/log.55710
09-25-2012 20:39:24.010 INFO - (restore.cc:97) restoring from checkpoint of 2012-09-25T20:03:09.383993Z
09-25-2012 20:39:24.010 INFO - (replay.cc:63) open log file: /home/qfs0/state/transactions/log.55710
09-25-2012 20:39:24.010 INFO - (replay.cc:559) log time: 2012-09-25T20:00:24.161876Z
09-25-2012 20:39:24.010 INFO - (replay.cc:559) log time: 2012-09-25T20:09:43.533466Z
09-25-2012 20:39:24.010 INFO - (replay.cc:63) open log file: /home/qfs0/state/transactions/log.55711
09-25-2012 20:39:24.011 INFO - (replay.cc:559) log time: 2012-09-25T20:09:43.533721Z
09-25-2012 20:39:24.011 INFO - (replay.cc:559) log time: 2012-09-25T20:19:43.829361Z
09-25-2012 20:39:24.011 INFO - (replay.cc:63) open log file: /home/qfs0/state/transactions/log.55712
09-25-2012 20:39:24.011 INFO - (replay.cc:559) log time: 2012-09-25T20:19:43.829674Z
09-25-2012 20:39:24.012 INFO - (replay.cc:559) log time: 2012-09-25T20:29:44.712673Z
```

otherwise `qfsfsck` will exit in error.

### Object Store (S3) File System Integrity Verification (`qfsobjstorefsck`)

The `qfsobjstorefsck` tool can be used to verify object store (S3) bloks inventory.

Object store fsck loads checkpoint, replays transaction logs, then reads
object store block keys from standard in, one key per line, and outputs "lost"
file names on standard out (files with keys that were not present in standard
in), if any.

Note that the list of object store block keys must be more recent than
checkpoint, and transaction logs, and valid meta server host and port must be
specified in order for this work correctly (no false positives) if the file
system is "live" / being modified.

In other words, the correct procedure to check "live" file system is to copy /
save checkpoint, and transaction logs, then create list of object store
blocks, then run this tool.

### File System Archive

Checking a file system image backup is very similar to that of checking a
running metaserver's checkpoint and transaction logs, except no lock file
(*metaServer.checkpoint.lockFileName*) is required. The backup must be
extracted to the same set of paths from which it was archived. Therefore it
should be extracted to the location specified by *metaServer.cpDir* and
*metaServer.logDir* of its associated metaserver.

### Verification Example

Given the following configuration:

```console
metaServer.cpDir = /home/qfs0/state/checkpoint
metaServer.logDir = /home/qfs0/state/transactions
```

and an archive located at: `/foo/bar/qfs0-backup-31-23.tar.gz` created from
`/home/qfs0/state`

The following commands can be used to verify the backup:

```sh
mkdir -p /home/qfs0/state &&
cd /home/qfs0/state &&
tar -xzf /foo/bar/qfs0-backup-31-23.tar.gz &&
qfsfsck -l /home/qfs0/state/transactions -c /home/qfs0/state/checkpoint
```

If everything is okay, the output will look something like this:

```console
09-25-2012 20:39:01.894 INFO - (restore.cc:97) restoring from checkpoint of 2012-09-25T20:00:26.971544Z
09-25-2012 20:39:01.894 INFO - (replay.cc:63) open log file: /home/qfs0/state/transactions/log.55710
09-25-2012 20:39:24.010 INFO - (restore.cc:97) restoring from checkpoint of 2012-09-25T20:03:09.383993Z
09-25-2012 20:39:24.010 INFO - (replay.cc:63) open log file: /home/qfs0/state/transactions/log.55710
09-25-2012 20:39:24.010 INFO - (replay.cc:559) log time: 2012-09-25T20:00:24.161876Z
09-25-2012 20:39:24.010 INFO - (replay.cc:559) log time: 2012-09-25T20:09:43.533466Z
09-25-2012 20:39:24.010 INFO - (replay.cc:63) open log file: /home/qfs0/state/transactions/log.55711
09-25-2012 20:39:24.011 INFO - (replay.cc:559) log time: 2012-09-25T20:09:43.533721Z
09-25-2012 20:39:24.011 INFO - (replay.cc:559) log time: 2012-09-25T20:19:43.829361Z
09-25-2012 20:39:24.011 INFO - (replay.cc:63) open log file: /home/qfs0/state/transactions/log.55712
09-25-2012 20:39:24.011 INFO - (replay.cc:559) log time: 2012-09-25T20:19:43.829674Z
09-25-2012 20:39:24.012 INFO - (replay.cc:559) log time: 2012-09-25T20:29:44.712673Z
```

otherwise `qfsfsck` will exit in error.

### WORM Mode

WORM (or Write Once, Read Many) is a special file system mode which makes it
imposible to delete files from the file system. This feature is useful for
protecting critical data from deletion. The `qfstoggleworm` tool is used to
turn WORM mode on and off.

To turn WORM mode on do the following:

```sh
qfstoggleworm -s metaServer.host -p metaServer.port -t 1
```

Likewise, to turn WORM mode off do the following:

```sh
qfstoggleworm -s metaServer.host -p metaServer.port -t 0
```

When a QFS instance is running in WORM mode, a file can only be created if it
ends with a `.tmp` suffix. Once stored in the file system, it can then be
renamed without the `.tmp` suffix. For example, to write `foo.bar` to a QFS
instance running in WORM mode, it would have to be created as `foo.bar.tmp` then
moved into place as `foo.bar`. Once moved, the file cannot be deleted or
modified, unless WORM mode is disabled.

### Web Reporting Interface

The QFS web interface `qfsstatus.py` provides a rich set of real-time
information, which can be used monitor file system instances.

### Configuration

The web server configuration is normally stored in a file called `webUI.cfg`.
See the [[Configuration Reference]] for a complete set of web UI configuration
parameters. Also the sample servers used in the examples include a typical web
UI configuration. Running

```sh
qfsstatus.py /path/to/webUI.cfg
```

### Interface

The following sample image of the web reporting interface is for a QFS instance
with 1 metaserver and 1 chunk server, configured with three chunk directories.
The host file system size is ~~18GB, out of which ~~10GB is used (not by QFS)
and ~~8GB is available.

![QFS WebUI](images/Administrator's Guide/qfs-webui.png)

The following table describes some UI elements:

| vega:20000                 | The metaserver host name and port number.                                                                                                                                                                                                |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Chunk Servers Status       | Opens a page with chunk servers statistics. One could select various chunk server parameters to be displayed, the refresh interval and delta.                                                                                            |
| Metaserver Status          | Opens a page with metaserver statistics. One could select various metaserver parameters to be displayed, the refresh interval and delta.                                                                                                 |
| Total space                | Total space of the host file system(s) where QFS stores chunks.                                                                                                                                                                          |
| Used space                 | Space used by QFS.                                                                                                                                                                                                                       |
| Free space                 | Available space in the host file system(s) where QFS stores chunks. When the free space becomes less than a % threshold (given by a metaserver configuration value) the metaserver stops using this chunk directory for chunk placement. |
| WORM mode                  | Status of the write-once-read-many mode.                                                                                                                                                                                                 |
| Nodes                      | Number of nodes in different states in the file system.                                                                                                                                                                                  |
| Replications               | Number of chunks in various states of replication.                                                                                                                                                                                       |
| Allocations                | File system-wide count of QFS clients, chunk servers, and so on.                                                                                                                                                                         |
| Allocations b+tree         | Internal b+tree counters. In this example, root directory + dumpster directory make up the 2 in fattr.                                                                                                                                   |
| Chunk placement candidates | Out of all chunk servers, how many are used for chunk placement, which are assigned racks.                                                                                                                                               |
| Disks                      | Number of disks in the file system.**Note**: our recommendation is to use one chunk directory per physical disk.                                                                                                                         |
| All Nodes                  | Table of one row per chunk server, describing a summary for each chunk server.                                                                                                                                                           |

A metaserver or chunk server ping can be used to dump the file system status.
All of the information presented by the web interface is available via a ping.
This makes it fairly easy to build automation around a QFS file system.

You can use `qfsping` to ping a metaserver:

```sh
qfsping -m -s metaServer.hostname -p metaServer.portg
```

The command is similar for a chunk server ping:

```sh
qfsping -c -s chunkServer.hostname -p chunkServer.port
```

Parsing the output of a ping is beyond the scope of this document but the Python
web interface `qfsstatus.py` provides an example of this and more.

### Ugrading from previous release (`logcompactor`)

The recommended procedure is to use logcompactor from previous release to create
checkpoint and possibly single log segment, then use logcompactor from the new
release to convert file system meta data into new format.

```sh
logcomactor -l state/transactions -c state/checkpoint \
   -T state/transactions_new -C state/checkpoint_new
```

## Chunk Server

The chunk server is the workhorse of QFS file system and is responsible for
storing and retrieving file chunk data. This section will discuss basic chunk
server administration.

### Chunk Server Configuration

The chunk server configuration is normally stored in a file called
`ChunkServer.prp`. The [[Deployment Guide]] includes several minimal sample
configurations. For the complete set of configuration parameters see the
[[Configuration Reference]].

### Running Chunk Server

```sh
chunkserver /path/to/ChunkServer.prp
```

### Hibernation

Hibernation is used to temporarily take a chunk server offline, such as for
maintenance of the physical server. When a chunk server is hibernated, the
metaserver will not actively attempt to re-replicate or recover chunks hosted by
the hibernated chunk server for the specified hibernation period. However,
chunks will be passively recovered if they're necessary to fulfill a request.

This feature is useful in preventing replication/recovery storms when performing
node or rack level maintenance.

### qfshibernate

The `qfshibernate` tool is used to hibernate a chunk server:

```sh
qfshibernate -m chunkServer.metaServer.hostname -p chunkServer.metaServer.port \
   -c chunkServer.hostname -d chunkServer.clientPort -s delay # (in seconds)
```

### Chunk Server Hibernate Example

Given the following metaserver configuration:

```properties
chunkServer.metaServer.hostname = 192.168.1.1
chunkServer.metaServer.port = 10000
```

To hibernate a chunk server at 192.168.10.20 (*chunkServer.hostname*) running on
a client port of 1635 (*chunkServer.clientPort*) for 30 minutes one would
execute the following command:

```sh
qfshibernate -m 192.168.1.1 -p 10000 -c 192.168.10.20 -d 1635 -s 1800
```

This would instruct the metaserver at 192.168.1.1:10000 to hibernate the chunk
server at 192.168.10.20:1635 for 1800 seconds or 30 minutes. Upon hibernation
the chunk server will exit.

### Notes

- Running qfshibernate again with the same chunk server will update hibernation window.
- The longer the hibernation window, the greater the likelihood of data loss. A
  window of no more than an hour is recommended for this reason.

### Evacuation

Evacuation can be used to permanently or temporarily retire a chunk server
volume. It is recommended that evacuation be used instead of hibernation if the
expected down time exceeds one hour.

To evacuate a chunk server, create a file named *evacuate* in each of its chunk
directories (*chunkServer.chunkDir*). This will cause the chunk server to
safely remove all chunks from each chunk directory where the *evacuate* file is
present. Once a chunk directory is evacuated, the chunk server will rename the
*evacuate* file to *evacuate.done*.

### Example

To evacuate a chunk server with following chunk directories configured:

```sh
chunkServer.chunkDir /mnt/data0/chunks /mnt/data1/chunks /mnt/data2/chunks
```

one could use the following script:

```sh
#!/bin/bash
for data in /mnt/data*; do
    chunkdir=$data/chunks
    if [ -e $chunkdir ]; then
        touch $chunkdir/evacuate
    fi
done
```

This will cause the chunk server to evacuate all chunks from
`/mnt/data0/chunks`, `/mnt/data1/chunks`, and `/mnt/data2/chunks`. As each chunk
directory is evacuated, the chunk server will rename its *evacuate* file to
*evacuate.done*.

To check the status of the evacuation:

```sh
cd /mnt && find -name evacuate.done | wc -l
```

Once the count returned equals 3, all chunk directories have been evacuated and
it's safe to stop the chunk server.

**Note**: the metaserver web UI will also list all chunk server evacuations and
their status.

## Client Tools

QFS includes a set of client tools to make it easy to access the file system.
This section describes those tools.

| Tool            | Purpose                                                         | Notes                                                                                                                                                                                                                                                                                  |
| --------------- | --------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cpfromqfs`     | Copy files from QFS to a local file system or to stdout         | Supported options: skipping holes, setting of write buffer size, start and end offsets of source file, read ahead size, op retry count, retry delay and retry timeouts, partial sparse file support. See `./cpfromqfs -h` for more.                                                    |
| `cptoqfs`       | Copy files from a local file system or stdin to QFS             | Supported options: setting replication factor, data and recovery stripe counts, stripes size, input buffer size, QFS write buffer size, truncate/delete target files, create exclusive mode, append mode, op retry count, retry delay and retry timeouts. See `./cptoqfs -h` for more. |
| `qfscat`        | Output the contents of file(s) to stdout                        | See `./qfscat -h` for more information.                                                                                                                                                                                                                                                |
| `qfsput`        | Reads from stdin and writes to a given QFS file                 | See `./qfsput -h` for more information.                                                                                                                                                                                                                                                |
| `qfsdataverify` | Verify the replication data of a given file in QFS              | The `-c` option compares the checksums of all replicas. The `-d` option verifies that all N copies of each chunk are identical. Note that for files with replication 1, this tool performs **no** verification. See `./qfsdataverify -h` for more.                                     |
| `qfsfileenum`   | Prints the sizes and locations of the chunks for the given file | See `./qfsfileenum -h` for more information.                                                                                                                                                                                                                                           |
| `qfsping`       | Send a ping to metaserver or chunk server                       | Doing a metaserver ping returns list of chunk servers that are up and down. It also returns the usage stats of each up chunk server.\\Doing a chunk server ping returns a the chunk server stats. See `./qfsping -h` for more.                                                         |
| `qfshibernate`  | Hibernates a chunk server for the given number of seconds       | See `./qfshibernate -h` for more information.                                                                                                                                                                                                                                          |
| `qfsshell`      | Opens a simple client shell to execute QFS commands             | By default this opens an interactive shell. One can bypss the interactive shell and execute commands directly by using the `-q` option. See `./qfsshell -h` for more.                                                                                                                  |
| `qfsstats`      | Reports qfs statistics                                          | The `-n` option is used to control the interval between reports. The RPC stats are also reported if the `-t` option is used. See `./qfsstats -h` for more.                                                                                                                             |
| `qfstoggleworm` | Set the WORM (write once read many) mode of the file system     |                                                                                                                                                                                                                                                                                        |

## Related Documents

- [[Deployment Guide]]
- [[Configuration Reference]]

![Quantcast](//pixel.quantserve.com/pixel/p-9fYuixa7g_Hm2.gif?labels=opensource.qfs.wiki)
