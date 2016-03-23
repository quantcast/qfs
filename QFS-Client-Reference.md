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

    chunkServer.storageTierPrefixes = /mnt/ram 10 /mnt/flash 11

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

    qfs -fs <filesystem> -dloc <start-byte> <end-byte> <filename>

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
