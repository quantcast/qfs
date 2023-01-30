# Go QFS Bindings

These are Go bindings to interact with [Quantcast File System](https://github.com/quantcast/qfs).
It includes many of the common functions exported by the QFS API.

## Build and Test

Several incantations of the compiler flags and the linker are required to get
qfs to compile against the c library. We may be to remove this necessity with
static linking but time is short.

Some incantation such as this should work:

Should be the path to your qfs build or production qfs.

Here are the environment variables I've used to compile these (Mac OS X specific):

    export QFS_RELEASE=/Users/sday/c/qfs/build/release
    export DYLD_LIBRARY_PATH=$QFS_RELEASE/lib/
    export CGO_CFLAGS=-I$QFS_RELEASE/include/
    export CGO_LDFLAGS=-L$QFS_RELEASE/lib

Note that these are Mac OSX specific, but are nearly identical for a linux
system. Just changing DYLD_LIBRARY_PATH to LD_LIBRARY_PATH should work.
DYLD_LIBRARY_PATH/LD_LIBRARY_PATH may not be necessary with proper linking.

This let's one run the standard go commands:

    go build
    go install goget.corp.qc/go/qfs

Even testing works, with the sample server in the main project, assuming
QFS_SOURCE environment variable is set to the source checkout of qfs:

    python $QFS_SOURCE/examples/sampleservers/sample_setup.py -a install
    python $QFS_SOURCE/examples/sampleservers/sample_setup.py -a start
    go test -qfs.addr localhost:20000

## Caveats

- The locking may be slightly course-grained.
- There are several easy performance enhancements that can be gained by
  eliminating redundant calls to QFS client library and through aggressive
  caching.
