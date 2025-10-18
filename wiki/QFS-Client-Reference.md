# QFS Client Reference

This page provides additional information on QFS best practices and how to use
the QFS C++/Java Client API and command line tools.

## FAQ

### What are the pros/cons of packing available nodes into smaller number of failure groups (with large number of nodes in each group) vs larger number of failure groups (with small number of nodes in each group)?

Assuming the chunks are distributed equally among failure groups, a higher
number of failure groups leads to greater number of tolerated group failures.
This comes at the expense of less or no data locality, but the fact that network
is faster than disks these days doesn’t make this an issue.

It is the opposite for a lower number of failure groups. It results in fewer
tolerated group failures, but more locality. Another downside is the fact that
your storage capacity might diminish significantly if a group failure is not
transient. For instance, for a cluster with 30 nodes packed into three failure
groups with ten nodes each, one might lose 1/3th of the storage capacity for
extended failure periods since an entire failure group is down.

Note that, in general, failure group choices must be a function of the file
encoding and reliability requirements. For instance, two failure groups would be
sufficient to tolerate one group failure with replication 2 and no erasure
encoding.

### How does QFS handle chunk placement in general?

When placing a chunk in a file, the metaserver will generally choose the
chunkserver which has more space available on its physical drives. This is the
default configuration. Different chunks of the same file are placed in a way
that fault tolerant placement is taken into account as well. That is; if there
are more racks available, the metaserver will spread out the different chunks of
a file into different racks decreasing the chance of data loss under failures.
Note that metaserver also actively tries to re-balance available space among
storage nodes by moving chunks around from over-utilized chunkservers to
under-utilized ones.

### How can I alter the default chunk placement policy?

One possibility is to change the field values that define the behaviour of
the chunk placement policy and the space re-balancer. Some of these fields are
[metaServer.maxSpaceUtilizationThreshold](https://github.com/quantcast/qfs/blob/master/conf/MetaServer.prp#L255),
[metaServer.rebalancingEnabled](https://github.com/quantcast/qfs/blob/master/conf/MetaServer.prp#L316),
[metaServer.maxRebalanceSpaceUtilThreshold](https://github.com/quantcast/qfs/blob/master/conf/MetaServer.prp#L325) and
[metaServer.minRebalanceSpaceUtilThreshold](https://github.com/quantcast/qfs/blob/master/conf/MetaServer.prp#L331).

Another alternative is to assign static weights to racks in metaserver
configuration file by using
[metaServer.rackWeights](https://github.com/quantcast/qfs/blob/master/conf/MetaServer.prp#L203)
field. When static weights are assigned, the more weight and chunkservers that a
rack has, the more likely it will be chosen for chunk placement.

Additionally, one can effect the chunk placement decision in a finer level by
explicitly specifying the range of storage units that the chunks of a file
should be placed on through the use of tiers. See [the related
FAQ](http://localhost:4567/preview#faq_what-exactly-are-tiers-in-qfs-and-how-can-i-use-them)
for more information on how one can set tiers in QFS.

### What exactly are tiers in QFS and how can I use them?

Tiers are used to refer storage units with similar characteristics. For example,
a filesystem might be backed up by varying storage solutions such as HDDs, SSDs,
and RAM disks offering distinct advantages. In such a case, HDDs, SSDs and RAM
disks can be grouped in different tiers. Then, when creating a file, one can
explicitly tell the metaserver to place a file in a specific tier or in a range
of tiers.

Tier settings can be specified in metaserver configuration file by
`chunkServer.storageTierPrefixes` field. For example,

```properties
chunkServer.storageTierPrefixes = /mnt/ram 10 /mnt/flash 11
```

tells the metaserver that the devices that are mounted to /mnt/ram and
/mnt/flash in chunkservers belong to tiers 10 and 11, respectively. Then, a hot
file for which we tell the metaserver to store in tier 10 will be stored in a
chunkserver that has a storage device mounted under /mnt/ram.

### Can I change the tier range of existing chunks of a file by using the `KfsClient::SetStorageTierRange` function?

The `KfsClient::SetStorageTierRange` function is used to change the min tier and
max tier attributes of a non-object store file. However, it doesn't have an
effect on chunks which have been already written. The new values for these
attributes are effective only for the new data that would be appended to the
file.

To change the tier of existing chunks in a file, one must make a copy of the
file and specify the min and max tier values during creation of the new copy.

### Is there a way to display the data locations for a file?

Invoking the *qfs* command-line tool like below displays which chunkservers
contain the data between \<start-byte\>th and \<end-byte\>th bytes in a file:

```sh
qfs -fs <filesystem> -dloc <start-byte> <end-byte> <filename>
```

### What is the best way to handle non-uniformity in storage nodes caused by differing disk counts and disk capacities?

Non-uniformity in storage nodes can have serious impacts on performance and
reliability, if not dealt with properly. To give an example for what might go
wrong, suppose that there are 5 racks and rack 1 has significantly more space
compared to other racks. Then, a disproportionate amount of files would have
rack 1 involved. This does not only increase the network traffic on rack 1 and
cause greater latencies for file operations, but also a larger number of files
will be impacted, should something happen to rack 1.

The best way to deal with this is to organize the failure groups as
homogeneously as possible. For instance, if storage units have varying sizes,
it is best to split them horizontally across racks (and across nodes in each
rack) so that each rack (and each node in a rack) has roughly the same amount of
available space. Alternatively, assuming each rack itself is homogeneous, one
can also assign static rack weights to balance write load among racks.

### Assuming RS 6+3 with replication 1 is used, are chunks for a given file ever written to more than 9 nodes?

Yes, they are. In this configuration, each chunk can hold data upto 64MB. If one
writes a very large file, the first 364MB (64MB\*6) of original data along with
corresponding 192MB (64MB\*3) parity data will be stored in 9 chunks, ideally in
9 separate chunkservers.  For the next 364MB of original data, the metaserver
will allocate a different set of 9 chunks (again 6 chunks for original data and
3 more for parity) and more likely store them in a different set of 9
chunkservers, assuming the number of available chunkservers is sufficiently
large.

## QFS Client Properties

Throughout the related text, we refer to two values; write-stride and
read-stride. write-stride is defined as *(number of data stripes + number of
recovery stripes) * stripe size*, whereas read-stride is defined as *number of
data stripes * stripe size*.

### File Properties

* *ioBufferSize:* Serves as write-behind threshold and governs when buffered
data gets actually written. During file creation/opening, QFS client sets
*ioBufferSize* to *defaultIOBufferSize* (see *defaultIOBufferSize* below for
details). During file creation/opening, QFS client ensures that *ioBufferSize*
for Reed-Solomon files does not go below (*number of data stripes*+*number of
recovery stripes*)\**targetDiskIoSize* and it is write-stride size aligned.
After file creation/opening, users can overwrite the value set by QFS client by
calling `KfsClient::SetIoBufferSize(int fd, size_t size)`. Note that
`KfsClient::SetIoBufferSize(int fd, size_t size)` does not have an effect on
previously submitted write requests.

* *readAheadBufferSize:* Defines the minimum number of bytes read from a file
regardless of the actual number of bytes that a read call intents to read.
&nbsp;During file creation/opening, QFS client automatically sets
*readAheadBufferSize* to *defaultReadAheadBufferSize* (see
*defaultReadAheadBufferSize* below for details). During file creation/opening,
QFS client ensures that *readAheadBufferSize* for Reed-Solomon files does not go
below *number of data stripes*\**targetDiskIoSize* and it is read-stride size
aligned. After file creation/opening, users can overwrite the value set by QFS
client by calling `KfsClient::SetReadAheadSize(int fd, size_t size)`. Note that
`KfsClient::SetReadAheadSize(int fd, size_t size)` does not have an effect on
previously submitted read requests.

* *diskIOReadSize:* Defines the maximum number of bytes read from a file each
time data is received from a chunk server. Consequently, it also controls the
size of the disk IO read operation at the chunk server -- how many bytes that
the chunk server reads from the underlying storage device at each access.
Currently, it is set to *maxReadSize* (see below *maxReadSize* for details) for
all types of read operations (regular read, read-ahead, prefetch read).

* *diskIOWriteSize:* Defines the maximum number of bytes written to a file each
time data is sent to a chunk server. Consequently, it also controls the size of
the disk IO write operation at the chunk server -- how many bytes that the chunk
server writes to the underlying storage device at each access. For 3x
Replication files, *diskIOWriteSize* is set to *ioBufferSize*, whereas for
Reed-Solomon files it is set to *ioBufferSize* / *(number of data stripes+number
of recovery stripes)* and it is checksum block size aligned. Note that
*diskIOWriteSize* can’t go beyond 4MB or QFS client’s global *maxWriteSize* (see
*maxWriteSize* below for details).

### Global Client Properties

* *targetDiskIoSize*: Ensures a minimum value for *ioBufferSize* and
*readAheadBufferSize* of a Reed-Solomon file during file creation/opening (see
*ioBufferSize* and *readAheadBufferSize* above for details), so that size of
each disk IO for reads/writes in a chunk server satisfies the target value.
Users can set *targetDiskIoSize* during QFS client initialization by setting
QFS_CLIENT_CONFIG environment variable to `client.targetDiskIoSize=<value>`.
Otherwise, *targetDiskIoSize* is set to 1MB.

* *defaultIOBufferSize:* Used to set *ioBufferSize* of a file during file
creation/opening (see *ioBufferSize* above for details). When necessary
conditions are satisfied, it is also used to set *defaultReadAheadBufferSize*
(see *defaultReadAheadBufferSize* below for details). Users can set
*defaultIOBufferSize* during QFS client initialization by setting
QFS_CLIENT_CONFIG environment variable to `client.defaultIoBufferSize=<value>`.
Note that if users don’t provide a value or the provided value is less than
checksum block size (64KB), *defaultIOBufferSize* is set to *max(1MB,
targetDiskIoSize)*. Once QFS client is initialized, users can overwrite the
value of *defaultIOBufferSize* by calling
`KfsClient::SetDefaultIoBufferSize(size_t size)`. Note that
`KfsClient::SetDefaultIoBufferSize(size_t size)` will not have an effect on
already created or opened files.

* *defaultReadAheadBufferSize:* Used to set *readAheadBufferSize* of a file
during file creation/opening (see *readAheadBufferSize* above for details).
Users can set *defaultReadAheadBufferSize* during QFS client initialization by
setting QFS_CLIENT_CONFIG environment variable to
`client.defaultReadAheadBufferSize=<value>`.
Note that *defaultReadAheadBufferSize* is set to *defaultIOBufferSize*, if users
don’t provide a value and *defaultIOBufferSize* is greater than checksum block
size (64KB). Otherwise, it is set to 1MB. Once QFS client is initialized, users
can overwrite the value of *defaultReadAheadBufferSize* by calling
`KfsClient::SetDefaultReadAheadSize(size_t size)`.
Note that `KfsClient::SetDefaultReadAheadSize(size_t size)` will not have an
effect on already created or opened files.

* *maxReadSize:* Provides a maximum value for *diskIOReadSize* of a file. Users
can set *maxReadSize* during QFS client initialization by setting
QFS_CLIENT_CONFIG environment variable to `client.maxReadSize=<value>`. If users
don’t provide a value or the provided value is less than the checksum block size
(64KB), *maxReadSize* is set to *max(4MB, targetDiskIoSize)*.

* *maxWriteSize:* Provides a maximum value for *diskIOWriteSize* of a file.
Users can set *maxWriteSize* during QFS client initialization by setting
QFS_CLIENT_CONFIG environment variable to `client.maxWriteSize=<value>`*.* If
users don’t provide a value, *maxWriteSize* is set to *targetDiskIoSize*.

* *randomWriteThreshold:* Users can set *randomWriteThreshold* during QFS client
initialization by setting QFS_CLIENT_CONFIG environment variable to
`client.randomWriteThreshold=<value>`. If users don’t provide a value,
*randomWriteThreshold* is set to *maxWriteSize* (if provided in the environment
variable).

* *connectionPool*: A flag that tells whether a chunk server connection pool
should be used by QFS client. This is used to reduce the number of chunk server
connections and presently used only with radix sort with write append. Users can
set *connectionPool* during QFS client initialization by setting
QFS_CLIENT_CONFIG environment variable to `client.connectionPool=<value>`.
Default value is false.

* *fullSparseFileSupport*: A flag that tells whether the filesystem might be
hosting sparse files. When it is set, a short read operation does not produce an
error, but instead is accounted as a read on a sparse file. Users can set
*fullSparseFileSupport* during QFS client initialization by setting
QFS_CLIENT_CONFIG environment variable to
`client.fullSparseFileSupport=<value>`. Once QFS client is initialized, users
can change the current value by calling
`KfsClient::SetDefaultFullSparseFileSupport(bool flag)`. Default value is false.

## Read and Write Functions

### `KfsClient::Read(int fd, char* buf, size_t numBytes)`

Used for blocking reads with a read-ahead logic managed by QFS client.
QFS client performs the following steps in order.

* Checks if the current read call could be served from an
ongoing prefetch read. The number of remaining bytes to read
is updated accordingly.

* Checks if the remaining number of bytes could be read from
the existing content in the read-ahead buffer. The number of
remaining bytes to read is updated accordingly.

* Next, if the number of remaining bytes is sufficiently less
than *read-ahead buffer size*, a new read-ahead operation is issued
and the remaining bytes to read for the current read call are served
from the new content of the read-ahead buffer. Note that QFS client
allocates an additional buffer for read-ahead operations on a file.

* If previous step is skipped, QFS client creates a regular blocking
read operation. Note that it does not make a copy of the source buffer.
Once the read is over, it issues a new read-ahead operation for subsequent
read calls on the same file. This read-ahead operation is performed
in a non-blocking fashion.

### `KfsClient::ReadPrefetch(int fd, char* buf, size_t numBytes)`

Used for non-blocking reads in which user provides a prefetch buffer.
QFS client does not make a copy of the prefetch buffer, so users
should ensure that the provided prefetch buffer is not used until the
prefetch operation completes. Completion handling is done when user makes
a blocking read call. If data prefetched is less than what is asked in
blocking read, the blocking read call will read the remaining data.

### `KfsClient::WriteAsync(int fd, const char* buf, size_t numBytes)`

*Note:* This mode of write is yet to be fully supported.

Used for non-blocking writes. QFS client doesn’t make a copy of the
user provided source buffer, so user should not use the source buffer
until the write gets completed. Completion handling is done by invoking
`KfsClient::WriteAsyncCompletionHandler(int fd)`. This function will wait
until all of the non-blocking write requests on that file complete.

### `KfsClient::Write(int fd, const char* buf, size_t numBytes)`

Used for blocking writes. However, how QFS client actually performs the
write depends on 1) the number of bytes that we want to write with the
current call (denoted as&nbsp;*numBytes* below), 2) the number of pending
bytes to be written from previous calls (denoted as *pending* below) and
3) write-behind threshold. Following two cases are possible.

* *numBytes + pending < write-behind threshold*: QFS client makes a
copy of the source buffer. Write operation is delayed until the number
of pending bytes (including the bytes from the current write call) exceeds
*write-behind threshold* by subsequent write calls or until user calls
`KfsClient::Sync(int fd)`.

* *numBytes + pending >= write-behind threshold*: QFS client makes
a copy of the source buffer only if *write-behind threshold* is greater than
zero. Write is performed in a blocking fashion.

## Append Operations

### RecordAppend

RecordAppend can be used by a single writer to append to a replicated file.
There is no support for append to RS files or object store files.

The file should be opened using the mode O_APPEND or O_WRONLY | O_APPEND.
When RecordAppend is used to write a record, the entire record is guaranteed to
be written to the same chunk. If the record will not fit in the current chunk, a
new chunk is started. Note that this will generally create a sparse file since
the new chunk will start at the next chunk boundary location.

For example, using default settings, the sequence

```cpp
char data[] = { '1', '2', '3' };
int fd = client->Open(filename, O_CREAT | O_EXCL);
client->Write(fd, &data[0], 1);
client->Close(fd);

fd = client->Open(filename, O_WRONLY | O_APPEND);
client->RecordAppend(fd, &data[1], 1);
client->RecordAppend(fd, &data[2], 1);
client->Close(fd);
```

will likely result in a file with 2 chunks, each replicated 3 times, and with a
size of 134217728 bytes.

A chunk can be in two states - "unstable" (dirty/nonreadable), and "stable"
(read only). The metaserver handles the transition of a chunk from unstable to
stable and this process may take some time.
For more information, see src/cc/chunk/AtomicRecordAppender.cc

When a file is opened for append, any chunk that is written will be unstable for
a while even if the file is closed. If the file is reopened for append, then new
data may be appended to an existing unstable chunk rather than creating a new
chunk.

An attempt to read a chunk that is not stable will stall the read until the
chunk is stable.

Reading the above file will result in a short read error unless sparse file
support is enabled. This can be accomplished by calling
`KfsClient::SetDefaultFullSparseFileSupport(bool flag)` to enable it for all
files or by calling `KfsClient::SetFullSparseFileSupport(int fd, bool flag)` for
an open file before doing any reads.

Using

```cpp
char buffer[134217728];
int fd = client->Open(filename, O_RDONLY);
client->SetFullSparseFileSupport(fd, true);
int bytes = client->Read(fd, buffer, sizeof(buffer));
for(int i=0; i<bytes; i++) {
  if (buffer[i] != 0) printf("Byte at offset %d is %02X\n", i, buffer[i]);
}
printf("Read %d bytes\n", bytes);
client->Close(fd);
```

would output

```console
Byte at offset 0 is 31
Byte at offset 67108864 is 32
Byte at offset 67108865 is 33
Read 134217728 bytes
```

`KfsClient::SkipHolesInFile(int fd)` can be used to both indicate spare files
support and to request that the holes in the files be skipped. Calling
SkipHolesInFile instead of SetFullParseFileSupport in the above code would
output

```console
Byte at offset 0 is 31
Byte at offset 1 is 32
Byte at offset 2 is 33
Read 3 bytes
```

### Writes with O_APPEND

If a file is opened with O_APPEND, then a Write behaves the same as a
RecordAppend.

### Emulating a traditional append

To do a traditional append to end of file with a single writer, open the file
for write then seek to the end of file before writing.

```cpp
char data[] = { '1', '2', '3' };
int fd = client->Open(filename, O_CREAT | O_EXCL);
client->Write(fd, &data[0], 1);
client->Close(fd);

fd = client->Open(filename, O_WRONLY);
client->Seek(fd, 0, SEEK_END);
client->Write(fd, &data[1], 1);
client->Write(fd, &data[2], 1);
client->Close(fd);
```

will result in a file with a single chunk replicated 3 times and a size of 3
bytes.

### AtomicRecordAppend

AtomicRecordAppend can be used by multiple writers to append to a file.

Please see the comments on src/cc/chunk/AtomicRecordAppender.cc and
src/cc/meta/LayoutManager.cc for more information.
