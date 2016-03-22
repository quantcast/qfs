Welcome to the QFS Wiki
=======================

Quantcast File System (QFS) is a high-performance, fault-tolerant, distributed
file system developed to support MapReduce processing, or other applications
reading and writing large files sequentially. This wiki provides various pieces
of information regarding QFS, its setup, best practices, etc. If there are any
specific questions or comments you have that aren't answered in this wiki,
please feel free to reach out to us through our mailing list
[**qfs-devel@googlegroups.com**](mailto:qfs-devel@googlegroups.com) or search
the archives at the
[QFS Developer Mailing List](http://groups.google.com/group/qfs-devel).

For an indepth view into QFS and how it works, please read the
[QFS paper presented at VLDB](http://db.disi.unitn.eu/pages/VLDBProgram/pdf/industry/p808-ovsiannikov.pdf).

Some selected pages for you to read are available below. A full list of all wiki
pages is available on the right sidebar.

- [[Introduction To QFS]]
- [[Repository Organization]]
- [[Administrator's Guide]]
- [[Deployment Guide]]
- [[Configuration Reference]]
- [[Developer Documentation]]
- [[Migration Guide]]
- [[Performance Comparison to HDFS]]
- [[QFS Client Reference]]

This page describes the steps necessary to test-drive QFS on your computer. For
multi-node setups, please refer to the [[Deployment Guide]].

Binary Distributions
--------------------
Several binary distributions have been provided to make it easy to try out QFS
on a single node. See the [Download
Page](https://github.com/quantcast/qfs/wiki/Binary-Distributions) for a list of
tarballs organized by operating system.

Single Node Test Drive
----------------------
The first step of the single-node setup is to download the tarball that matches
the Linux/OS X distribution you are running. Setting up a single node
configuration to familiarize yourself with QFS is very easy.

1. Extract the distribution tarball.

        $ tar -xzf $QFSTAR.tgz && cd $QFSTAR

1. Set up a single node QFS instance. This will create a workspace in
`~/qfsbase`, start two chunk servers and one metaserver.

        $ ./examples/sampleservers/sample_setup.py -a install
        Binaries presence checking - OK.
        Setup directories - OK.
        Setup config files - OK.
        Started servers - OK.

1. Confirm everything is running.

        $ ps x | grep [q]fsbase
        14424 pts/6    Sl     0:00 ./examples/sampleservers/../../bin/metaserver -c /home/zim/qfsbase/meta/conf/MetaServer.prp /home/zim/qfsbase/meta/MetaServer.log
        14429 pts/6    Sl     0:00 ./examples/sampleservers/../../bin/chunkserver /home/zim/qfsbase/chunk2/conf/ChunkServer.prp /home/zim/qfsbase/chunk2/ChunkServer.log
        14431 pts/6    Sl     0:00 ./examples/sampleservers/../../bin/chunkserver /home/zim/qfsbase/chunk1/conf/ChunkServer.prp /home/zim/qfsbase/chunk1/ChunkServer.log
        14442 pts/6    S      0:00 python ./examples/sampleservers/../../webui/qfsstatus.py /home/zim/qfsbase/web/conf/WebUI.cfg

The file system web UI will be running on port 22000: <http://localhost:22000>.
**Note:** In a production environment only one chunk server is deployed per QFS
instance per host. For simplicity this example deploys two chunk servers that
belong to the same QFS instance to a single host.  Hence the capacity statistics
are counted twice and reported as such via the web UI.

1. Add tools binary path to`PATH`

        $ PATH=${PWD}/bin/tools:${PATH}

1. Make a temporary directory on the file system

        $ qfsshell -s localhost -p 20000 -q -- mkdir /qfs/tmp

1. Create a file containing "Hello World", Reed-Solomon encoded, with
replication 1.

        $ echo 'Hello World' | cptoqfs -s localhost -p 20000 -S -r 1 -k /qfs/tmp/helloworld -d -

1. Cat the file content.

        $ qfscat -s localhost -p 20000 /qfs/tmp/helloworld

1. Stat the file to see encoding (RS or not), replication level, and mtime.

        $ qfsshell -s localhost -p 20000 -q -- stat /qfs/tmp/helloworld

1. Copy the file locally to the current directory.

        $ cpfromqfs -s localhost -p 20000 -k /qfs/tmp/helloworld -d ./helloworld

1. Remove the file from QFS.

        $ qfsshell -s localhost -p 20000 -q -- rm /qfs/tmp/helloworld

1. Stop the servers.

        $ ./examples/sampleservers/sample_setup.py -a stop

1. Uninstall the single node instance.

        $ ./examples/sampleservers/sample_setup.py -a uninstall


![Quantcast](//pixel.quantserve.com/pixel/p-9fYuixa7g_Hm2.gif?labels=opensource.qfs.wiki)
