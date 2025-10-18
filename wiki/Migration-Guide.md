# QFS Migration Guide

This page provides information on how to use QFS with Apache Hadoop.

## Obtaining the Hadoop QFS plugin

With QFS 1.0.1, we have simplified the integration of QFS with Apache Hadoop
deployment. One only needs to copy the Hadoop QFS plugin jar and set the java
library path in order to use QFS as the backing store for Hadoop.

You can obtain the Hadoop QFS plugin jar in one of two ways:

- If there is a [[QFS binary tarball|Binary-Distributions]] for your platform,
  then the tarball already has the jars and native libraries.

    Obtain the QFS tarball, say, `$QFSTAR.tgz`

    ```sh
    tar -xvzf "$QFSTAR".tgz && cd "$QFSTAR" &&
    ls -1 lib/hadoop*.jar
    ```

    ```console
    lib/hadoop-0.23.4-qfs-1.0.1.jar
    lib/hadoop-1.0.2-qfs-1.0.1.jar
    lib/hadoop-1.0.4-qfs-1.0.1.jar
    lib/hadoop-1.1.0-qfs-1.0.1.jar
    lib/hadoop-2.0.2-alpha-qfs-1.0.1.jar
    ```

- If there is no pre-built tarball for your platform, you could obtain the QFS
  source and build the tarball yourself. As long as you have the pre-requisite
  packages in your system (see [[Developer Documentation]] all you need to do is
  to run `make tarball` from the QFS source directory, and this will produce
  the $QFSTAR.tgz in the build directory.

## Using the Hadoop QFS plugin with your Hadoop deployment

When the Hadoop QFS jar is in your class path and the QFS native libraries are
loadable, accessing QFS from Hadoop is as simple as,

```sh
cd "${HADOOP_HOME}" &&
bin/hadoop fs                                                  \
    -Dfs.qfs.impl=com.quantcast.qfs.hadoop.QuantcastFileSystem \
    -Dfs.default.name=qfs://localhost:20000                    \
    -Dfs.qfs.metaServerHost=localhost                          \
    -Dfs.qfs.metaServerPort=20000                              \
    -ls /
```

In the example above, the sample QFS metaserver (see [[Getting Started|Home]])
listens on port 20000 on `localhost`.

1. **To add Hadoop QFS jar to your classpath**, either copy the
   `**hadoop-<xxx>-qfs-<yyy>.jar**` to `$HADOOP_HOME/lib/` or add the absolute
   path of the jar file to `$HADOOP_HOME/conf/hadoop-env.sh` as `export
   HADOOP_CLASSPATH=</absolute/path/of/the.jar>`.

1. **To make the QFS native libraries loadable**, you could set
   `JAVA_LIBRARY_PATH` or `java.library.path` to `${QFSTAR}/lib/` (for Hadoop
   1.x.x versions you may need to copy `${QFSTAR}/lib/libqfs_*` to
   `${HADOOP_HOME}/lib/native/<your platform>/`, and also, edit
   `${HADOOP_HOME}/conf/hadoop-env.sh` file to add the line `export
   LD_LIBRARY_PATH=${HADOOP_HOME}/lib/native/<your-platform>`).

Once these are set, you could access QFS from Hadoop using `qfs://` URIs by
setting `fs.qfs.impl` to `com.quantcast.qfs.hadoop.QuantcastFileSystem` and
`fs.default.name` (or `fs.defaultFS`, in newer Hadoop versions) to
`qfs://<qfs-meta-host>:<qfs-meta-port>`, as shown in the example above.

## Migrating HDFS Data to QFS

If you have existing data in HDFS that you want to copy to QFS in order to use
QFS as your backing store, you could run a distributed copy provided by Apache
Hadoop. In the following example, `namehost:8020` is the host name and port
number of the namenode of an HDFS instance and `metahost:20000` is the
corresponding location of a QFS metaserver.

```sh
cd "${HADOOP_HOME}" &&
bin/hadoop distcp                                              \
    -Dfs.qfs.impl=com.quantcast.qfs.hadoop.QuantcastFileSystem \
    -Dfs.default.name=qfs://localhost:20000                    \
    -Dfs.qfs.metaServerHost=localhost                          \
    -Dfs.qfs.metaServerPort=20000                              \
    hdfs://localhost:8020/hdfs_dir/70MFile qfs://localhost:20000/qfs_dir/70Mcopy
```

Note that this is a map-reduce job, and therefore there should be job trackers
and task trackers available to do the distibuted copy.

## Submitting Jobs that use QFS

If you want to submit a job to Apache Hadoop that would use QFS, you could
follow this example:

```sh
cd "${HADOOP_HOME}" &&
bin/hadoop jar hadoop-examples-1.0.3.jar randomwriter          \
    -Dfs.qfs.impl=com.quantcast.qfs.hadoop.QuantcastFileSystem \
    -Dfs.default.name=qfs://metahost:20000                     \
    -Dfs.qfs.metaServerHost=metahost                           \
    -Dfs.qfs.metaServerPort=20000                              \
    /tmp/randomOut
```

![Quantcast](//pixel.quantserve.com/pixel/p-9fYuixa7g_Hm2.gif?labels=opensource.qfs.wiki)
