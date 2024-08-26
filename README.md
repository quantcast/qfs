# QFS version 2

QFS version 2.2.7

Release 2.2.7

- Release 2.2.7 updated python module, python3 support, and minor improvements. [Release notes](https://github.com/quantcast/qfs/wiki/Release-Notes).

QFS version 2.2.6

Release 2.2.6

- Release 2.2.6 contains go language bindings, bug fixes, and minor improvements. [Release notes](https://github.com/quantcast/qfs/wiki/Release-Notes).

QFS version 2.2.5

Release 2.2.5

- Release 2.2.5 contains bug fixes and minor improvements. [Release notes](https://github.com/quantcast/qfs/wiki/Release-Notes).

QFS version 2.2.4

Release 2.2.4

- Release 2.2.4 is a bug fix release. [Release notes](https://github.com/quantcast/qfs/wiki/Release-Notes).

Release 2.2.3

- Support for chunk server's node ID that can be used instead of IP/port for
  determining if chunk server and QFS client are co-located on the same network
  node and use chunk server to service client requests.
- Bug fixes.

[Release notes](https://github.com/quantcast/qfs/wiki/Release-Notes).

QFS version 2.2.2

Release 2.2.2 is a bug fix release. [Release notes](https://github.com/quantcast/qfs/wiki/Release-Notes).

QFS version 2.2.1

Release 2.2.1 is a bug fix release. [Release notes](https://github.com/quantcast/qfs/wiki/Release-Notes).

QFS version 2.2.0

Release 2.2.0 contains symbolic links support, and minor bug fixes.

QFS version 2.1.3

Release 2.1.3 contains DNS resolver's number of open socket accounting bug fix.
This bug might manifest itself in at least one non obvious way: chunk server
might continuously fail to create or open chunk files if/when the open sockets
counter becomes negative.

Release 2.1.2 contains minor bug fixes, meta and chunk servers watchdog.

Release 2.1.1 contains minor bug fixes.

Non blocking DNS resolver is the new feature in release 2.1.0. The new
DNS resolver is intended to improve S3 [compatible] object store IO
concurrency and reduce latency.

Meta server replication is the major new feature in release 2.0.0. Meta server
replication provides automatic meta server fail over. With meta server replication
configured QFS does not have single point of failure.

Release notes are available [here](https://github.com/quantcast/qfs/wiki/Release-Notes).

# Quantcast File System

Quantcast File System (QFS) is a high-performance, fault-tolerant, distributed
file system developed to support MapReduce processing, or other applications
reading and writing large files sequentially.

QFS used in Quantcast production cluster.

QFS servers have been tested on 64-bit CentOS 6 extensively and run on Linux
variants. The QFS client tools work on OS X and Cygwin as well.

| Platform     | Build Status                                                                                                                                                   |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Mac OS X     | [![Build Status](https://github.com/quantcast/qfs/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/quantcast/qfs/actions/workflows/build.yml) |
| Ubuntu 14.04 | [![Build Status](https://github.com/quantcast/qfs/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/quantcast/qfs/actions/workflows/build.yml) |
| Ubuntu 18.04 | [![Build Status](https://github.com/quantcast/qfs/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/quantcast/qfs/actions/workflows/build.yml) |
| Ubuntu 20.04 | [![Build Status](https://github.com/quantcast/qfs/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/quantcast/qfs/actions/workflows/build.yml) |
| Ubuntu 22.04 | [![Build Status](https://github.com/quantcast/qfs/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/quantcast/qfs/actions/workflows/build.yml) |
| CentOS 6     | [![Build Status](https://github.com/quantcast/qfs/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/quantcast/qfs/actions/workflows/build.yml) |
| CentOS 7     | [![Build Status](https://github.com/quantcast/qfs/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/quantcast/qfs/actions/workflows/build.yml) |
| CentOS 8     | [![Build Status](https://github.com/quantcast/qfs/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/quantcast/qfs/actions/workflows/build.yml) |
| Debian 10    | [![Build Status](https://github.com/quantcast/qfs/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/quantcast/qfs/actions/workflows/build.yml) |

The implementation details and features of QFS are discussed in detail in the
project [wiki](https://github.com/quantcast/qfs/wiki/Introduction-To-QFS).

## Getting QFS

QFS is available through various channels.

- **[BigTop][bigtop]**: QFS packages for rpm and debian
  based systems are available through the [BigTop project][packages].
- **[Binary Distributions][bd]**: There are various binary distributions
  packaged as tarballs available on the [Binary Distributions Page][bd].
- **Compile from source**: QFS can be compiled on Linux variants, Mac OS X, and
  Cygwin. See the [wiki][develop] for more information on how to build QFS
  yourself.

## Trying QFS

Once you have aquired QFS through one of the methods above, you can take QFS for
a quick test drive. Setting up a single node configuration to familiarize
yourself with QFS is very easy.

1.  Extract the distribution tarball.

        $ tar -xzf qfs.tgz && cd qfs

1.  Set up a single node QFS instance. This will create a workspace in
    `~/qfsbase`, start two chunk servers and one metaserver.

            $ ./examples/sampleservers/sample_setup.py -a install
            Binaries presence checking - OK.
            Setup directories - OK.
            Setup config files - OK.
            Started servers - OK.

1.  Add tools binary path to`PATH`

        $ PATH=${PWD}/bin/tools:${PATH}

1.  Make a temporary directory on the file system

        $ qfsshell -s localhost -p 20000 -q -- mkdir /qfs/tmp

1.  Create a file containing "Hello World", Reed-Solomon encoded, with
    replication 1.

            $ echo 'Hello World' | cptoqfs -s localhost -p 20000 -S -k /qfs/tmp/helloworld -d -

1.  Cat the file content.

        $ qfscat -s localhost -p 20000 /qfs/tmp/helloworld

1.  Stat the file to see encoding (RS or not), replication level, and mtime.

        $ qfsshell -s localhost -p 20000 -q -- stat /qfs/tmp/helloworld

1.  Copy the file locally to the current directory.

        $ cpfromqfs -s localhost -p 20000 -k /qfs/tmp/helloworld -d ./helloworld

1.  Remove the file from QFS.

        $ qfsshell -s localhost -p 20000 -q -- rm /qfs/tmp/helloworld

1.  Stop the servers.

        $ ./examples/sampleservers/sample_setup.py -a stop

1.  Uninstall the single node instance.

        $ ./examples/sampleservers/sample_setup.py -a uninstall

## Benchmarking QFS

A performance comparison between QFS and HDFS 1.0.2 shows QFS is faster both at
reading and writing 20 TB of uncompressed data on our test system, a
heterogeneous cluster with 6,500 disk drives. See more information regarding
this in our [wiki][perf].

## Contributing to QFS

We welcome contributions to QFS in the form of enhancement requests, patches,
additional tests, bug reports, new ideas, and so on. Please submit issues using
our [JIRA instance][issues] and refer to the [QFS code contribution policy][ccp]
when contributing code.

## Have Questions?

Join the QFS Developer mailing list or search the archives at the
[Google Group](http://groups.google.com/group/qfs-devel).

Post comments or questions to qfs-devel@googlegroups.com.

## License

QFS is released under the Apache 2.0 license.

[bd]: https://github.com/quantcast/qfs/wiki/Binary-Distributions
[bigtop]: https://bigtop.apache.org/
[ccp]: https://github.com/quantcast/qfs/wiki/Code-Contribution-Policy
[issues]: https://quantcast.atlassian.net
[packages]: https://ci.bigtop.apache.org/view/Packages/job/Bigtop-trunk-packages
[perf]: https://github.com/quantcast/qfs/wiki/Performance-Comparison-to-HDFS
[develop]: https://github.com/quantcast/qfs/wiki/Developer-Documentation
