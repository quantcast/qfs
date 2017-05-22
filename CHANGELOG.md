# Changes

## May 21, 2017

Changes since 1.2.0 release. The following changes were back ported from the master (2.0) branch.

* Fixed sporadic file descriptor close in meta server checkpoint write in case when lock file was configured / used.

* Fixed bug in S3 block delete state machine that appear in the case when more one upload ID returned by S3.

* Fixed QFS client write pipelining. Now write path with adequately large write behind is no longer latency bound.

* Fixed integer overflow in IO buffer pool with pool size equal or greater 4GB. The problem affects both chunk and meta servers. However, typically, only chunk server, if configured with IO buffer pool larger than 4GB, and S3, might use enough buffers for the problem to occur.

* Meta server: fixed memory leak in B+tree height decrease by destroying up prior root node.

* IO library: fixed IO buffer stream buffer reset method by setting get and put sequence pointers to 0. Implemented xsgetn stream buffer method to make input stream read more efficient.

* Chunk server: back ported directory checker thread stack size increase and extraneous type casts remove from master branch.

* S3 IO library: added parameter to control querying S3 for upload IDs prior to deleting object store block.
