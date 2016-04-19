Prerequisites
=============
To compile and run QFS you need to have the following software packages
installed in your development system.

| RHEL/CentOS      | Debian/Ubuntu        | OS X    | Cygwin             | Notes                                                                                                                                         |
|------------------|----------------------|---------|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `gcc-g++`        | `gcc`                |         | `gcc-g++`          |                                                                                                                                               |
| `make`           | `make`               |         |                    |                                                                                                                                               |
| `git`            | `git`                |         |                    | version 1.7.10 or higher                                                                                                                      |
| `cmake`          | `cmake`              | `cmake` |                    | version 2.8.4 or higher                                                                                                                       |
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

Repository Organization
=======================
```
    QFS top-level directory
    │
    ├──── benchmarks
    │     └──── mstress                (Benchmark comparing meta operations)
    │
    ├──── build                        (Build output directory)
    │
    ├──── examples
    │     ├──── cc                     (C++ client example)
    │     ├──── java                   (Java client example)
    │     ├──── python                 (Experimental python client example)
    │     └──── sampleservers          (Scripts to bring up local QFS for testing)
    │
    ├──── contrib                      (Contributions including sample deployment packages)
    │
    ├──── conf                         (Sample configs for release packaging)
    │
    ├──── scripts                      (Admin and init scripts)
    │
    ├──── webui                        (metaserver web UI monitor)
    │
    └──── src
          │
          ├──── cc
          │     ├──── access           (Java/Python glue code)
          │     ├──── chunk            (chunk server code)
          │     ├──── common           (common declarations)
          │     ├──── devtools         (miscellaneous developer utilities & tools)
          │     ├──── emulator         (QFS offline layout emulator)
          │     ├──── fuse             (FUSE module for Linux and MacOS)
          │     ├──── kfsio            (I/O library used by QFS)
          │     ├──── libclient        (client library code)
          │     ├──── meta             (metaserver code)
          │     ├──── qcdio            (low level io and threading library)
          │     ├──── qcrs             (Reed-Solomon encoder and decoder)
          │     ├──── tests            (QFS tests)
          │     └──── tools            (QFS user/admin tools)
          │
          ├──── java
          │     ├──── qfs-access       (Java wrappers to call QFS-JNI code)
          │     └──── hadoop-qfs       (Apache Hadoop QFS plugin)
          │
          ├──── python                 (Experimental python client code)
          └──── test-scripts           (Scripts to test QFS servers and components)
```

Compiling The Source
====================
The top-level Makefile automatically compiles QFS and generates the server
binaries, client tools and client libraries. This section has information that
gives you greater control over the compile process. This section also provides
information to a developer who wants to write a QFS client application to access
files in QFS. The mode of execution of a QFS client application is as follows:

1. The application interacts with the QFS client library. The application can be
   written in C++, Java, or Python (python QFS adapter is in experimental
   stage).
1. The QFS client library, in turn, interfaces with the metaserver and/or chunk
   server(s).
1. If data is replicated, whenever there is a failure in a chunk server, the
   client library will fail-over to another chunk server that has the data; this
   fail-over is transparent to the application.

Compiling the C++ Code
------------------
Compiling the C++ code produces the metaserver, chunkserver, client, and admin
tool binaries. It also produces the C++ client library. We use **`cmake`** to
build the C++ code. You can use the top-level Makefile as a wrapper around cmake
to build the project by simply executing `make`.

Once the build is complete, you will find the build artifacts in the
`build/debug` directory.

### Types of Builds
The default build type is a debug build. You can execute a release build by
running `BUILD_TYPE=release make`. The build artifacts for this build will be
available in the `build/release` directory.

Debug binaries are useful when developing QFS since they include debugging
symbols, greatly simplifying debugging. However, for a production deployment,
it is recommended to use release binaries with debugging info enabled (such as,
compile flags of "-O2 -g"). Having binaries with debugging info in production
simplifies debugging should a problem arise in a production environment.

### Verbose Build Output
To build with verbose output, use the environment variable `VERBOSE=true` to
build. For example, use `VERBOSE=true make`.

### `make test` Targets
To run qfs tests, use `make test`. This will ensure that all core functionality
is intact. Note that this test invokes the metaserver and chunk servers locally
and performs various checks, it may take a couple of minutes to complete. If you
are running this from a partition that is nearly full, the test may fail. Please
refer to `maxSpaceUtilizationThreshold` in [[Configuration Reference]].

#### Writing Tests
C++ tests are stored in `src/cc/tests`. QFS uses [Google Test][gt] as its
testing framework. Unit tests which test specific functionality in a function or
library can simply use a bare test, e.g. `TEST(Foo, Bar)`. However, all
integration tests (tests which interact with the metaserver or chunkserver)
should use test fixtures, subclassing the `QFSTest` class. For example, you
could add a new test class like so:

    #include <gtest/gtest.h>
    #include "tests/integtest.h"

    class FooTest : public QFSTest {
    public:
        virtual void SetUp() { ... }
        virtual void TearDown() { ... }
    };

    TEST_F(FooTest, TestBar) {
        ...
    }

Furthermore, you should place tests in the corresponding directory for which
functionality you are testing. For example, if you are testing functionality for
code stored in the `libclient` directory, place the tests in
`src/cc/tests/libclient`. Either a unit or integration test should be added for
each functionality change to QFS. Please see the tests in `src/cc/tests` for
some examples on how QFS has already implemented various tests.

### Developing a C++ client
To develop a c++ client, see the sample code in the
`examples/cc/qfssample_main.cc` file. The QFS client library API is
defined in `src/cc/libclient/KfsClient.h`.

The C++ client should link with libqfs\_client library and other libqfs\_
dependencies. Note that default build will contain libraries built with the
"Debug" option. This library code in this mode is "very chatty". Hence, it is
recommended that you use the libraries built with "Release" option with your
applications.

Compiling Java Side
-------------------
Compile the Java code to get the QFS access jar (which contains the wrapper
calls to native C++ via JNI; this allows Java apps to access files stored in
QFS) and the Apache Hadoop QFS plugin jar. The Apache Hadoop QFS plugin includes
the QFS access classes within, therefore, someone who wants to use QFS with
Hadoop will only need to use one extra jar file.

Apache Maven is used to build Java jars. Use the top-level makefile (`make
java`) to build the java jars.

### Developing a Java Client
For Java applications, we use the JNI interface to get at the C++ QFS client
library code from Java applications. One should refer to the Java client example
at `examples/java/QfsSample.java`. The QFS Java client library API is
defined in
`src/java/qfs-access/src/main/java/com/quantcast/qfs/access/KfsAccess.java`.

The Java compiler should have
`build/java/qfs-access/qfs-access-<version>.jar` in the `CLASSPATH`. In
addition, to execute the client, `build/release/lib` should be in
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
section describes how to build and install the python extension module. To build
the python module, use the command `make python`. The file
`build/build/<distdir>/qfs.so` will be created at the end.

### Developing a Python Client
Python applications use the python QFS extension module, `qfs.so`, to get at the
C++ QFS client library. The example program
`examples/python/qfssample.py` illustrates how to write a Python client for QFS.

Set `PYTHONPATH` accordingly if you are using a `qfs.so` install path different
from the default path, so that the python run-time can detect the `qfs.so`.
Also, ensure that `LD_LIBRARY_PATH` has the path to the QFS libraries. For
example,

    $ export PYTHONPATH=/usr/lib/python2.7/dist-packages:$PYTHONPATH
    $ export LD_LIBRARY_PATH=/usr/lib/python2.7/dist-packages/qfs:$LD_LIBRARY_PATH
    $ python examples/python/qfssample.py

![Quantcast](//pixel.quantserve.com/pixel/p-9fYuixa7g_Hm2.gif?labels=opensource.qfs.wiki)

[gt]: https://github.com/google/googletest
