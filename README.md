Quantcast File System
=====================

Quantcast File System (QFS) is a high-performance, fault-tolerant, distributed
file system developed to support MapReduce processing, or other applications
reading and writing large files sequentially.

We have released QFS to open source, at http://quantcast.github.com/qfs.

The implementation details and features of QFS are discussed in the project
Wiki at https://github.com/quantcast/qfs/wiki/Introduction-To-QFS

This document assumes that you have obtained the QFS source code (via git or
tarball) and it is at `~/code/qfs`

Compiling QFS
=============

* QFS can be compiled on Linux variants, Mac OS X, and Cygwin. The QFS servers
  have been tested on 64-bit CentOS 6 extensively and run on Linux variants.
  The QFS client tools work on OS X and Cygwin as well.

* Pre-requisites:
```
g++, make, jdk, java headers for JNI, Apache Maven,
libraries and headers for xfsprogs, libuuid, and openssl,
cmake (preferably, version 2.4.7 or higher),
boost-devel (preferably, version 1.3.4 or higher),
git (preferably, version 1.7.10 or higher)
```

* Once you have the pre-requisite packages installed,

        $ cd ~/code/qfs
        $ make

  This will build the QFS servers executables, libraries and client
  tools executables, and install them under build/release, ready for use.

* QFS compiles and run on various Linux distributions, OS X, and Cygwin. If
  you run into any compile issues or if you prefer to have greater control
  over the build process, please refer to the Wiki pages at
  https://github.com/quantcast/qfs/wiki/Developer-Documentation

* To test the QFS binaries,

        $ cd ~/code/qfs
        $ make test-release

  Note that this test takes a few minutes to complete.


Setting Up QFS
==============

Main components of the QFS server are the 'metaserver' and the 'chunkserver'.
Metaserver provides the namespace for the filesystem while the chunkservers do
the storage/retrieval of file blocks in the form of 'chunks'.

Each server uses a configuration file that sets the run time parameters of the
server. The metaserver is configured with the filesystem port, chunkserver
port, chunk placement groups for replication, the location of transaction
logs and checkpoints and so on. The chunk server is configured with the port
of the metaserver, path to copy the chunks and so on.

An easy set up of QFS has been provided in the examples/ directory, where a
metaserver and two chunk servers are launched, all on the same node. To do this
setup,

        $ cd ~/code/qfs
        $ make
        $ examples/sampleservers/sample_setup.py -a install

The python script creates config files for the QFS servers that can be found
under `~/qfsbase/` directory.

In this example setup, the metaserver listens on the filesystem port 20000 and
the webserver for monitoring filesystem listens on port 22000. Note that in
practice one chunkserver is deployed per QFS instance per host, but for the
purpose of illustration, this example setup uses two chunkservers per host.
Hence the capacity statistics are counted twice and reported as such via the
web UI at `http://localhost:22000`

For a more practical setup of QFS, please refer to the QFS Wiki documents at
https://github.com/quantcast/qfs/wiki/Deployment-Guide

Using QFS
=========

Once the QFS servers are up and running, one can use the QFS by different
means.

* Use client tools that are built during the compile process. For instance,

        $ cd ~/code/qfs
        $ PATH=${PWD}/build/release/bin/tools:${PATH}
        $ qfsshell -s localhost -p 20000 -q -- mkdir /tmp
        $ echo 'Hello World' | cptoqfs -s localhost -p 20000 -k /tmp/HW.dat -d -
        $ qfscat -s localhost -p 20000 /tmp/HW.dat
        $ qfsshell -s localhost -p 20000 -q -- rm /tmp/HW.dat

* If you built the QFS FUSE client, then you can mount the QFS at a local mount
  point by,

        $ mkdir /mnt/qfs
        $ cd ~/code/qfs/build/release/bin/
        $ ./qfs_fuse localhost:20000 /mnt/qfs -o allow_other,ro

  Further information about compiling and using QFS FUSE is at
  https://github.com/quantcast/qfs/wiki/Developer-Documentation

* Build your own QFS client in C++, Java, or Python (experimental) using the
  QFS client libraries that are generated during the compile process. See
  examples in directory `~/code/qfs/examples/` for reference.


Benchmarking QFS
================

A performance comparison between QFS and HDFS 1.0.2 shows QFS is faster both at
reading and writing 20 TB of uncompressed data on our test system,
a heterogeneous cluster with 6,500 disk drives.
See more at https://github.com/quantcast/qfs/wiki/Performance-Comparison-to-HDFS


Contributing to QFS
===================

We welcome contributions to QFS in the form of enhancement requests and patches, additional tests, bug-reports, new ideas and so on. Please submit issues at https://github.com/quantcast/qfs/issues and refer to QFS code contribution policy at https://github.com/quantcast/qfs/wiki/Code-Contribution-Policy.


Have Questions?
===============

Join the QFS Developer mailing list or search the archives at
http://groups.google.com/group/qfs-devel

Post comments or questions to qfs-devel@googlegroups.com


License
=======

QFS is released under the Apache 2.0 license.
