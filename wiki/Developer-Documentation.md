Prerequisites
=============
To compile and run QFS you need to have the following software packages
installed in your development system.

| RHEL/CentOS      | Debian/Ubuntu        | OS X    | Cygwin             | Notes                                                                                                                                         |
|------------------|----------------------|---------|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `gcc-g++`        | `gcc`                |         | `gcc-g++`          |                                                                                                                                               |
| `make`           | `make`               |         |                    |                                                                                                                                               |
| `git`            | `git`                |         |                    | version 1.7.10 or higher                                                                                                                      |
| `cmake`          | `cmake`              | `cmake` |                    | version 2.4.7 or higher                                                                                                                       |
| `maven`          | `maven2`             | `maven` |                    | version 3.0.3 or higher                                                                                                                       |
| `boost-devel`    | `libboost-regex-dev` | `boost` | `libboost-devel`   | version 1.3.4 or higher (for mac, may need to install boost with `'-no_single'` option if only the `/opt/local/lib/*-mt.dylib` are installed) |
| `krb5-devel`     | `libkrb5-dev`        |         |                    |                                                                                                                                               |
| `xfsprogs-devel` | `xfslibs-dev`        |         |                    |                                                                                                                                               |
| `openssl-devel`  | `libssl-dev`         |         | `openssl-devel`    |                                                                                                                                               |
| `python-devel`   | `python-dev`         |         |                    | for python bindings                                                                                                                           |
| `fuse-devel`     | `libfuse-dev`        |         |                    | for FUSE bindings                                                                                                                             |
| `java-openjdk`   | `default-jdk`        |         |                    | for java access                                                                                                                               |
| `java-devel`     |                      |         | `jdk`              | for java access                                                                                                                               |
| `libuuid-devel`  |                      |         |                    |                                                                                                                                               |
|                  |                      | `Xcode` |                    |                                                                                                                                               |
|                  |                      |         | `bzip2`            |                                                                                                                                               |
|                  |                      |         | `autoconf`         |                                                                                                                                               |
|                  |                      |         | `automake`         |                                                                                                                                               |
|                  |                      |         | `gcc-java`         |                                                                                                                                               |
|                  |                      |         | `libstdc++6-devel` |                                                                                                                                               |
|                  |                      |         | `tar`              |                                                                                                                                               |

The description below assumes that you're using a CentOS 64-bit machine.
Please interpret paths accordingly if you use a different architecture.

Code Layout
-----------
This document assumes that you have downloaded the source code to ~/code/qfs
directory. We assume that you want to build the source in
~/code/qfs/build/release. If you want to change the top-level directory from
~/code to something else, please keep the same hierarchy starting from
~code/qfs.

The `~/code/qfs` directory should contain directories as specified in the
[[Repository-Organization]] page.

Compiling The Source
====================
The top-level Makefile automatically compiles QFS and generates the server
binaries, client tools and client libraries. This section has information that
gives you greater control over the compile process.

This section also provides information to a developer who wants to write a QFS
client application to access files in QFS. The mode of execution of a QFS client
application is as follows:

1. The application interacts with the QFS client library. The application can be
   written in C++, Java, or Python (python QFS adapter is in experimental
   stage).
1. The QFS client library, in turn, interfaces with the metaserver and/or chunk
   server(s).
1. If data is replicated, whenever there is a failure in a chunk server, the
   client library will fail-over to another chunk server that has the data; this
   fail-over is transparent to the application.

Compiling C++ Side
------------------
Compiling the C++ side produces metaserver and chunk server binaries, client and
admin tools, and the C++ client library. We use **`cmake`** to build the C++
code. Here are the steps:

1. Change directory to the build root (i.e. `~/code/qfs/build/release`).

        $ cd ~/code/qfs && mkdir -p build/release && cd build/release

1. Generate the makefiles.

        $ cmake ~/code/qfs/

**Note:** If you have non-default locations for your boost or java files, then
set `BOOST_ROOT`, `BOOST_LIBRARYDIR`, and `JAVA_INCLUDE_PATH` accordingly.

        $ cmake -D BOOST_ROOT=/usr/local/include/boost-1_37 \\
                -D BOOST_LIBRARYDIR=/usr/local/lib \\
                -D JAVA_INCLUDE_PATH=/usr/java/default/include \\
                ~/code/qfs

1. Compile the source

        $ make

1. Once compilation is success, install the binaries. **Note:** Although the
   following command suggests system installation what it actually does is to
   just copy the binaries and libraries to separate directories under the build
   directory.

        $ make install

At the end of the last step, you will have the following hierarchy:

| Directory                    | Notes                          |
| ---------                    | -----                          |
|`~/code/qfs/build/release/bin`| This will contain the binaries.|
|`~/code/qfs/build/release/bin/tools/`| This will contain the various QFS utilities that are equivalent to the *nix commands such as, cp, mv, mkdir, rmdir etc.|
|`~/code/qfs/build/release/bin/tests/`|This contains simple unit-test programs to make/remove directories, read/write files, etc.|
|`~/code/qfs/build/release/lib/`| This will contain the shared libraries.|
|`~/code/qfs/build/release/lib/static/`| This will contain the static libraries.|

**Note:** If you run into JNI build error on Cygwin due to gcc not knowing about
the type “_\_int64″, then apply the following patch to the file: `jni_md.h`

     typedef long jint;`
    +#ifdef __GNUC__
    +typedef long long jlong;
    +#else
     typedef __int64 jlong;
    +#endif
     typedef signed char jbyte;


### Debug Vs. Release Binaries
It is advisable to build both debug/release binaries. For the purposes of
deployment, for performance reasons, it is recommended to use release binaries
with debugging info enabled (such as, compile flags of "-O2 -g"). Having
binaries with debugging info simplifies debugging.

To build DEBUG/RELEASE binaries, here is a suggested directory hierarchy:
  * `~/code/qfs/build`         -- build root
  * `~/code/qfs/build/debug`   -- debug build area
  * `~/code/qfs/build/release` -- release build area
  * `~/code/qfs/build/reldbg`  -- build area for release binares with debug info

One can tell cmake to build Release or Debug binaries by using cmake's
`CMAKE_BUILD_TYPE` variable. By default this is "Debug".

To build debug binaries:

    $ mkdir -p ~/code/qfs/build/debug & cd ~/code/qfs/build/debug
    $ cmake ~/code/qfs
    $ make

To build release binaries (This will typically invoke compiler with "-O3"):

    $ mkdir -p ~/code/qfs/build/release & cd ~/code/qfs/build/release
    $ cmake -DCMAKE_BUILD_TYPE:STRING="Release" ~/code/qfs
    $ make

To build release binaries with debug information (This will typically invoke
compiler with "-O2 -g"):

    $ mkdir -p ~/code/qfs/build/reldbg & cd ~/code/qfs/build/reldbg
    $ cmake -D CMAKE_BUILD_TYPE=RelWithDebInfo ~/code/qfs
    $ make

Note: `-DCMAKE_BUILD_TYPE=x` and `-DCMAKE_BUILD_TYPE:STRING="x"` both work

### Verbose Build Output
Once you run cmake to generate the makefiles, run make as follows to see verbose
output:

    $ mkdir -p ~/code/qfs/build/release & cd ~/code/qfs/build/release
    $ cmake -DCMAKE_BUILD_TYPE:STRING="RelWithDebInfo" ~/code/qfs
    $ make VERBOSE=1

### `make test` Targets
The top level `Makefile` has information on how to run an end-to-end test as
part of `make`. This is useful when you do changes to QFS source and want to
make sure the core functionality is intact.

    $ cd ~/code/qfs
    $ make test-release

**Notes**
1. Since this test invokes the metaserver and chunk servers locally and performs
   various checks, it may take a couple of minutes to complete.
1. Also note that if you are running this from a partition that is nearly full,
   the test may fail. Please refer to `maxSpaceUtilizationThreshold` in
   [[Configuration Reference]].

### Compiling QFS FUSE
For building FUSE support for QFS, you will need install the FUSE package on the
target machines and then compile the source as described above.

1. Install FUSE package

        $ yum install fuse-devel fuse-libs fuse (CentOS)
        $ apt-get install libfuse-dev           (Debian/Ubuntu)
        Install OSXFUSE                         (OSX)

1. In `~/code/qfs/CMakeLists.txt`, provide the path to libfuse.so. This is given
   by the following line in CMakeLists.txt: `SET (Fuse_LIBRARY_DIR <libfuse
   path>)`
1. Build the fuse binary

        cd ~/code/qfs/build/release
        cmake ~/code/qfs
        make && make install

After the last step qfs_fuse binary will be installed in
`~/code/qfs/build/release/bin/`

You can use the qfs_fuse binary directly or via /etc/fstab.

1. Direct usage:
    - Mount using `$ sudo ./qfs_fuse <metaserver>:20000 /mnt/qfs -o allow_other,ro`
    - Unmount using `$ sudo umount /mnt/qfs`
1. Editing /etc/fstab to mount automatically at startup:
    - Create a symlink to qfs\_fuse `$ ln -s <path-to-qfs_fuse> /sbin/mount.qfs`
    - Add the following line to /etc/fstab:`<metaserver>:20000 /mnt/qfs qfs ro,allow_other 0 0`

Due to licensing issues, you can include FUSE only if it is licensed under LGPL
or any other license that is compatible with Apache 2.0 license.

### Developing a C++ client
One can follow the sample C++ client at
`~/code/qfs/examples/cc/qfssample_main.cc`. The QFS client library API is
defined in `~/code/qfs/src/cc/libclient/KfsClient.h`.

The C++ client should link with libqfs\_client library and other libqfs\_
dependencies. The shared libraries are at `~/code/qfs/build/release/lib/` and
static libraries are at `~/code/qfs/build/release/lib/static/`.

**Note:** the default build will contain libraries built with the "Debug"
option. This library code in this mode is "very chatty". Hence, it is
recommended that you use the libraries built with "Release" option with your
applications (such as, `~/code/qfs/build/release/...)

Compiling Java Side
-------------------
Compile the Java side to get the QFS access jar (which contains the wrapper
calls to native C++ via JNI; this allows Java apps to access files stored in
QFS) and the Apache Hadoop QFS plugin jar.

The Ahache Hadoop QFS plugin includes the QFS access classes within, therefore,
someone who wants to use QFS with Hadoop will only need to use one extra jar
file.

Apache Maven is used to build Java jars. In order to support different versions
of Apache Hadoop, we have wrapped the `mvn` calls in a script in
`src/java/javabuild.sh`.

    $ cd ~/code/qfs
    $ src/java/javabuild.sh -h      // Prints out usage
    $ src/java/javabuild.sh         // Builds QFS Access jar
    $ src/java/javabuild.sh 1.0.4   // Builds QFS Access jar and Apache Hadoop (version 1.0.4) QFS plugin jar

For instance, running `src/java/javabuild.sh 1.0.4` with QFS version 1.0.1 would
produce:

    ~/code/qfs/build/java/qfs-access/classes
    ~/code/qfs/build/java/qfs-access/qfs-access-1.0.1.jar
    ~/code/qfs/build/java/hadoop-qfs/classes
    ~/code/qfs/build/java/hadoop-qfs/hadoop-1.0.4-qfs-1.0.1.jar

### Developing a Java Client
For Java applications, we use the JNI interface to get at the C++ QFS client
library code from Java applications. One could refer to the Java client example
at ~/code/qfs/examples/java/QfsSample.java. The QFS Java client library API is
defined in
`~/code/qfs/src/java/qfs-access/src/main/java/com/quantcast/qfs/access/KfsAccess.java`.

The Java compiler should have
`~/code/qfs/build/java/qfs-access/qfs-access-<version>.jar` in the `CLASSPATH`;
in addition, to execute the client, `~/code/qfs/build/release/lib/` should be in
the `LD_LIBRARY_PATH` (or `DYLD_LIBRARY_PATH`, if it is Mac OS X).
To build,

    $ cd ~/code/qfs/examples/java
    $ qfsjar=`echo ../../build/qfs-access/qfs-access*.jar`
    $ javac -classpath "$qfsjar" QfsSample.java

To execute,

    $ libdir="`cd ../../build/release/lib && pwd`"
    $ export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${libdir}"
    $ qfsjar=`echo ../../build/qfs-access/qfs-access*.jar`
    $ java -Djava.library.path="$libdir" -classpath ".:$qfsjar" QfsSample 127.0.0.1 20000

Compiling Python Side (Experimental)
------------------------------------
Python applications can access QFS by using the python extension module. This
section describes how to build and install the python extension module.

### Building Python Extension Module

One can build the python module after building QFS client library.

1. With build directory as ~/code/qfs/build, build and install the QFS client
library as described above.

        $ cd ~/code/qfs/build/release
        $ cmake ~/code/qfs/ && make && make install


1. From `~/code/qfs/build/release`, run the python setup script. This will
create the python extension module under
`~/code/qfs/build/release/build/<distdir>/`.

        $ cd ~/code/qfs/build/release
        $ python ~/code/qfs/src/cc/access/kfs_setup.py build

The file `~/code/qfs/build/build/<distdir>/qfs.so` will be created at the end of
last step.

**Note:** If you want to generate the modules in a path different from 'build',
issue the following command:

    $ python ~/code/qfs/src/cc/access/kfs_setup.py build --build-base ./pybuild

### Installing The Python Module
The `qfs.so` library needs to be installed in the site-packages for python
applications to use it. Default installation may require write access to
privileged directories. Custom installation allows installing at an alternate
location.

- Follow these steps for default installation.

        $ cd ~/code/qfs/build/release
        $ python ~/code/qfs/src/cc/access/kfs_setup.py install

If `/usr/lib64/python2.6/site-packages/` is where the site packages go then
`qfs.so` will go here.

- Follow these steps for custom installation.

        $ cd ~/code/qfs/build/release
        $ python ~/code/qfs/src/cc/access/kfs_setup.py install --home ./qfs_python

This command will install the python module at
`~/code/qfs/build/release/qfs_python/lib[64]/python/qfs.so`

**Note:** To allow python run-time environment to find this package, one will
need to add the package path to `PYTHONPATH`. The package path is the path
leading to `qfs.so`. In the above example, one would do:

    $ export PYTHONPATH=${PYTHONPATH}:~/code/qfs/build/release/qfs_python/lib[64]/python


### Developing a Python Client
Python applications use the python QFS extension module, `qfs.so`, to get at the
C++ QFS client library. The example program
`~/code/qfs/examples/python/qfssample.py` illustrates how to write a Python
client for QFS.

Set `$PYTHONPATH` accordingly if you are using a `qfs.so` install path different
from the default path, so that the python run-time can detect the `qfs.so`.
Also, ensure that LD_LIBRARY_PATH has the path to the QFS libraries.

    $ cd ~/code/qfs/examples/python
    $ PYTHONPATH=~/code/qfs/build/release/qfs_python/lib64/python LD_LIBRARY_PATH=~/code/qfs/build/release/lib python qfssample.py qfssample.cfg

![Quantcast](//pixel.quantserve.com/pixel/p-9fYuixa7g_Hm2.gif?labels=opensource.qfs.wiki)
