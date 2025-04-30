/**
 * $Id$
 *
 * Author: Thilee Subramaniam
 *
 * Copyright 2012,2016 Quantcast Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * This Java client performs filesystem meta operations on the Hadoop namenode
 * using HDFS DFSClient.
 */
package com.quantcast.qfs.mstress;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MStress_Client {

    static final Path TEST_BASE_DIR = new Path(Path.SEPARATOR, "mstress");

    static FileSystem dfs_ = null;
    static int totalCreateCount = 0;
    static final int COUNT_INCR = 500;

    //From commandline
    static String dfsServer_ = "";
    static int dfsPort_ = 0;
    static String testName_ = "";
    static String prefix_ = "";
    static String planfilePath_ = "";
    static String hostName_ = "";
    static String processName_ = "";

    //From plan file
    static String type_ = "";
    static int levels_ = 0;
    static int inodesPerLevel_ = 0;
    static int pathsToStat_ = 0;
    static boolean _debugTrace = false;

    public static void main(String args[]) {
        parseOptions(args);

        try {
            Configuration conf = new Configuration(true);
            String confSet = "hdfs://" + dfsServer_ + ":" + dfsPort_;
            conf.set("fs.default.name", confSet);
            conf.set("fs.trash.interval", "0");
            dfs_ = FileSystem.get(conf);

            if (parsePlanFile() < 0) {
                System.exit(-1);
            }

            int result;
            if (testName_.equals("create")) {
                result = createDFSPaths();
            } else if (testName_.equals("stat")) {
                result = statDFSPaths();
            } else if (testName_.equals("readdir")) {
                result = listDFSPaths();
            } else if (testName_.equals("delete")) {
                result = removeDFSPaths();
            } else {
                System.out.printf("Error: unrecognized test \'%s\'\n", testName_);
                result = -1;
            }
            if (result != 0) {
                System.exit(-1);
            }
        } catch (IOException e) {
            printStackTrace(e);
            System.exit(-1);
        }
        System.exit(0);
    }

    private static void printStackTrace(Throwable e) {
        e.printStackTrace(System.err);
    }

    private static void parseOptions(String args[]) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-s") && i + 1 < args.length) {
                dfsServer_ = args[i + 1];
                System.out.println(args[i + 1]);
                i++;
            } else if (args[i].equals("-p") && i + 1 < args.length) {
                dfsPort_ = Integer.parseInt(args[i + 1]);
                System.out.println(args[i + 1]);
                i++;
            } else if (args[i].equals("-t") && i + 1 < args.length) {
                testName_ = args[i + 1];
                System.out.println(args[i + 1]);
                i++;
            } else if (args[i].equals("-a") && i + 1 < args.length) {
                planfilePath_ = args[i + 1];
                System.out.println(args[i + 1]);
                i++;
            } else if (args[i].equals("-c") && i + 1 < args.length) {
                hostName_ = args[i + 1];
                System.out.println(args[i + 1]);
                i++;
            } else if (args[i].equals("-n") && i + 1 < args.length) {
                processName_ = args[i + 1];
                System.out.println(args[i + 1]);
                i++;
            } else if (args[i].equals("-P") && i + 1 < args.length) {
                prefix_ = args[i + 1];
                System.out.println(args[i + 1]);
                i++;
            } else if (args[i].equals("-h") || args[i].equals("--help")) {
                usage(0);
            } else {
                System.err.println("invalid argument: " + args[i]);
                usage(1);
            }
        }

        if (dfsServer_.isEmpty()
                || testName_.isEmpty()
                || planfilePath_.isEmpty()
                || hostName_.isEmpty()
                || processName_.isEmpty()
                || dfsPort_ <= 0) {
            usage(1);
        }
        if (prefix_ == null) {
            prefix_ = "PATH_";
        }
    }

    private static void usage(int status) {
        final String className = MStress_Client.class.getName();
        (0 == status ? System.out : System.err).printf(
                "Usage: java %s -s dfs-server -p dfs-port"
                + " [-t [create|stat|readdir|rmdir] -a planfile-path -c host"
                + " -n process-name -P prefix]\n"
                + "   -t: this option requires -a, -c, and -n options.\n"
                + "   -P: default prefix is PATH_.\n"
                + "eg:\n"
                + "    java %s -s <metaserver-host> -p <metaserver-port> -t"
                + " create -a <planfile> -c localhost -n Proc_00\n",
                className, className
        );
        System.exit(status);
    }

    @SuppressWarnings("UseSpecificCatch")
    private static int parsePlanFile() {
        int ret = -1;
        try {
            FileInputStream fis = new FileInputStream(planfilePath_);
            DataInputStream dis = new DataInputStream(fis);
            BufferedReader br = new BufferedReader(new InputStreamReader(dis));

            if (prefix_.isEmpty()) {
                prefix_ = "PATH_";
            }

            String line;
            while ((line = br.readLine()) != null) {
                if (line.length() == 0 || line.startsWith("#")) {
                    continue;
                }
                if (line.startsWith("type=")) {
                    type_ = line.substring(5);
                    continue;
                }
                if (line.startsWith("levels=")) {
                    levels_ = Integer.parseInt(line.substring(7));
                    continue;
                }
                if (line.startsWith("inodes=")) {
                    inodesPerLevel_ = Integer.parseInt(line.substring(7));
                    continue;
                }
                if (line.startsWith("nstat=")) {
                    pathsToStat_ = Integer.parseInt(line.substring(6));
                }
            }
            dis.close();
            if (levels_ > 0 && !type_.isEmpty() && inodesPerLevel_ > 0
                    && pathsToStat_ > 0) {
                ret = 0;
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
        return ret;
    }

    private static long timeDiffMilliSec(Date alpha, Date zigma) {
        return zigma.getTime() - alpha.getTime();
    }

    private static int CreateDFSPaths(int level, Path parentPath) {
        final Boolean isLeaf = level + 1 >= levels_;
        final Boolean isDir = !isLeaf || type_.equals("dir");

        for (int i = 0; i < inodesPerLevel_; i++) {
            Path path = new Path(parentPath, prefix_ + Integer.toString(i));
            if (_debugTrace) {
                System.out.printf(
                        "Creating (isdir=%b) [%s]\n",
                        isDir, path.toString()
                );
            }

            if (isDir) {
                try {
                    if (!dfs_.mkdirs(path)) {
                        System.out.printf("Error in mkdirs(%s)\n", path);
                        return -1;
                    }
                    totalCreateCount++;
                    if (totalCreateCount % COUNT_INCR == 0) {
                        System.out.printf(
                                "Created paths so far: %d\n", totalCreateCount);
                    }
                    if (!isLeaf) {
                        if (CreateDFSPaths(level + 1, path) < 0) {
                            System.out.printf(
                                    "Error in CreateDFSPaths(%s)\n", path);
                            return -1;
                        }
                    }
                } catch (IOException e) {
                    printStackTrace(e);
                    return -1;
                }
            } else {
                try {
                    dfs_.create(path, true);
                    totalCreateCount++;
                    if (totalCreateCount % COUNT_INCR == 0) {
                        System.out.printf(
                                "Created paths so far: %d\n", totalCreateCount);
                    }
                } catch (IOException e) {
                    printStackTrace(e);
                    return -1;
                }
            }
        }
        return 0;
    }

    private static int createDFSPaths() {
        Path basePath = new Path(TEST_BASE_DIR, hostName_ + "_" + processName_);
        try {
            if (!dfs_.mkdirs(basePath)) {
                System.out.printf(
                        "Error: failed to create test base dir [%s]\n",
                        basePath.toString()
                );
                return -1;
            }
        } catch (IOException e) {
            printStackTrace(e);
            throw new RuntimeException();
        }

        Date alpha = new Date();

        if (CreateDFSPaths(0, basePath) < 0) {
            return -1;
        }

        final Date zigma = new Date();
        System.out.printf(
                "Client: %d paths created in %d msec\n",
                totalCreateCount, timeDiffMilliSec(alpha, zigma)
        );
        return 0;
    }

    private static int statDFSPaths() {
        final Path basePath
                = new Path(TEST_BASE_DIR, hostName_ + "_" + processName_);
        final Date alpha = new Date();
        final Random random = new Random(alpha.getTime());

        for (int count = 0; count < pathsToStat_; count++) {
            Path path = basePath;
            for (int d = 0; d < levels_; d++) {
                int randIdx = random.nextInt(inodesPerLevel_);
                String name = prefix_ + Integer.toString(randIdx);
                path = new Path(path, name);
            }

            if (_debugTrace) {
                System.out.printf("Doing stat on [%s]\n", path);
            }
            try {
                dfs_.getFileStatus(path);
            } catch (IOException e) {
                printStackTrace(e);
                return -1;
            }
            if (count % COUNT_INCR == 0) {
                System.out.printf("Stat paths so far: %d\n", count);
            }
        }
        final Date zigma = new Date();
        System.out.printf(
                "Client: Stat done on %d paths in %d msec\n",
                pathsToStat_, timeDiffMilliSec(alpha, zigma)
        );
        return 0;
    }

    private static int listDFSPaths() {
        final Date alpha = new Date();
        final Queue<Path> pending = new LinkedList<Path>();
        pending.add(new Path(TEST_BASE_DIR, hostName_ + "_" + processName_));

        int inodeCount = 0;
        while (!pending.isEmpty()) {
            Path parent = pending.remove();
            try {
                FileStatus[] children = dfs_.listStatus(parent);
                for (FileStatus child : children) {
                    String localName = child.getPath().getName();
                    if (_debugTrace) {
                        System.out.printf(
                                "Readdir going through [%s/%s]\n",
                                parent.toString(), localName
                        );
                    }
                    if (localName.equals(".") || localName.equals("..")) {
                        continue;
                    }
                    inodeCount++;
                    if (inodeCount % COUNT_INCR == 0) {
                        System.out.printf(
                                "Readdir paths so far: %d\n", inodeCount);
                    }
                    if (child.isDir()) {
                        pending.add(new Path(parent, localName));
                    }
                }
            } catch (IOException e) {
                printStackTrace(e);
                return -1;
            }
        }

        final Date zigma = new Date();
        System.out.printf(
                "Client: Directory walk done over %d inodes in %d msec\n",
                inodeCount, timeDiffMilliSec(alpha, zigma)
        );
        return 0;
    }

    private static int removeDFSPaths() {
        final Path rmPath
                = new Path(TEST_BASE_DIR, hostName_ + "_" + processName_);

        System.out.printf("Deleting %s ...\n", rmPath.toString());

        final int countLeaf
                = (int) Math.round(Math.pow(inodesPerLevel_, levels_));
        final List<Integer> leafIdxRangeForDel = new ArrayList(countLeaf);
        for (int i = 0; i < countLeaf; i++) {
            leafIdxRangeForDel.add(i);
        }
        Collections.shuffle(leafIdxRangeForDel);

        final Date alpha = new Date();
        try {
            for (int idx : leafIdxRangeForDel) {
                Path path = null;
                for (int lev = 0; lev < levels_; lev++) {
                    int delta = idx % inodesPerLevel_;
                    idx /= inodesPerLevel_;
                    if (path != null) {
                        path = new Path(prefix_ + delta, path);
                    } else {
                        path = new Path(prefix_ + delta);
                    }
                }
                dfs_.delete(new Path(rmPath, path), true);
            }
            dfs_.delete(rmPath, true);
        } catch (IOException e) {
            printStackTrace(e);
            return -1;
        }
        final Date zigma = new Date();
        System.out.printf(
                "Client: Deleted %s. Delete took %d msec\n",
                rmPath, timeDiffMilliSec(alpha, zigma)
        );
        return 0;
    }
}
