/**
 * $Id$
 *
 * Author: Thilee Subramaniam
 *
 * Copyright 2012 Quantcast Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * This Java client performs filesystem meta operations on the Hadoop namenode
 * using HDFS DFSClient.
 */

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

public class MStress_Client
{
  static final String TEST_BASE_DIR = new String("/mstress");

  static DFSClient dfsClient_ = null;
  static StringBuilder path_  = new StringBuilder(4096);
  static int pathLen_         = 0;
  static int totalCreateCount = 0;
  static final int COUNT_INCR = 500;

  //From commandline
  static String dfsServer_    = "";
  static int dfsPort_         = 0;
  static String testName_     = "";
  static String prefix_       = "";
  static int prefixLen_       = 0;
  static String planfilePath_ = "";
  static String hostName_     = "";
  static String processName_  = "";

  //From plan file
  static String type_         = "";
  static int levels_          = 0;
  static int inodesPerLevel_  = 0;
  static int pathsToStat_     = 0;

  private static void pathPush(String leafStr) {
    int leafLen = leafStr.length();
    if (leafLen == 0) {
      return;
    }
    if (leafStr.charAt(0) != '/') {
      path_.insert(pathLen_, "/");
      System.out.printf("Leaf = %s, path_ = [%s]\n", leafStr, path_.toString());
      pathLen_ ++;
    }
    path_.insert(pathLen_, leafStr);
    System.out.printf("After push Leaf = %s, path_ = [%s]\n", leafStr, path_.toString());
    pathLen_ += leafLen;
  }

  private static void pathPop(String leafStr) {
    int leafLen = leafStr.length();
    if (leafLen > pathLen_ - 1) {
      System.out.printf("Error in pop: %s from %s, leafLen = %d, pathLen_ = %d\n", leafStr, path_.toString(), leafLen, pathLen_);
      return;
    }
    String lastPart = path_.substring(pathLen_ - leafLen, pathLen_);
    System.out.printf("lastPart = [%s - %s] leafStr = [%s - %s]\n", lastPart, lastPart.getClass().getName(), leafStr, leafStr.getClass().getName());

    if (!leafStr.equals(lastPart)) {
      System.out.printf("Error in pop: %s from %s\n", leafStr, path_.toString());
      System.exit(1);
      return;
    }
    pathLen_ -= leafLen + 1;
    path_.insert(pathLen_, '\0');
    System.out.printf("After pop, path_ = [%s]\n", path_.toString());
  }

  private static void pathReset() {
    path_.insert(0, '\0');
    pathLen_ = 0;
  }


  public static void main(String args[]) {
    parseOptions(args);
    int result = 0;

    try {
      Configuration conf = new Configuration(true);
      String confSet = "hdfs://" + dfsServer_ + ":" + dfsPort_;
      conf.set("fs.default.name", confSet);
      conf.set("fs.trash.interval", "0");
      InetSocketAddress inet = new InetSocketAddress(dfsServer_, dfsPort_);
      dfsClient_ = new DFSClient(inet, conf);

      if (parsePlanFile() < 0) {
        System.exit(-1);
      }

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
        System.exit(-1);
      }
    } catch( IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }

    if (result != 0) {
      System.exit(-1);
    }

    return;
  }

  private static void parseOptions(String args[])
  {
    if (!(args.length == 14 || args.length == 12 || args.length == 5)) {
      usage();
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-s") && i+1 < args.length) {
        dfsServer_ = args[i+1];
        System.out.println(args[i+1]);
        i++;
      } else if (args[i].equals("-p") && i+1 < args.length) {
        dfsPort_ = Integer.parseInt(args[i+1]);
        System.out.println(args[i+1]);
        i++;
      } else if (args[i].equals("-t") && i+1 < args.length) {
        testName_ = args[i+1];
        System.out.println(args[i+1]);
        i++;
      } else if (args[i].equals("-a") && i+1 < args.length) {
        planfilePath_ = args[i+1];
        System.out.println(args[i+1]);
        i++;
      } else if (args[i].equals("-c") && i+1 < args.length) {
        hostName_ = args[i+1];
        System.out.println(args[i+1]);
        i++;
      } else if (args[i].equals("-n") && i+1 < args.length) {
        processName_ = args[i+1];
        System.out.println(args[i+1]);
        i++;
      } else if (args[i].equals("-P") && i+1 < args.length) {
        prefix_ = args[i+1];
        System.out.println(args[i+1]);
        i++;
      }
    }

    if (dfsServer_.length() == 0    ||
        testName_.length() == 0     ||
        planfilePath_.length() == 0 ||
        hostName_.length() == 0     ||
        processName_.length() == 0  ||
        dfsPort_ == 0) {
      usage();
    }
    if (prefix_ == null) {
      prefix_ = new String("PATH_");
    }
    prefixLen_ = prefix_.length();
  }

  private static void usage()
  {
    String className = MStress_Client.class.getName();
    System.out.printf("Usage: java %s -s dfs-server -p dfs-port [-t [create|stat|readdir|rmdir] -a planfile-path -c host -n process-name -P prefix]\n",
                      className);
    System.out.printf("   -t: this option requires -a, -c, and -n options.\n");
    System.out.printf("   -P: default prefix is PATH_.\n");
    System.out.printf("eg:\n");
    System.out.printf("    java %s -s <metaserver-host> -p <metaserver-port> -t create -a <planfile> -c localhost -n Proc_00\n", className);
    System.exit(1);
  }

  private static int parsePlanFile()
  {
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
          continue;
        }
      }
      dis.close();
      if (levels_ > 0 && !type_.isEmpty() && inodesPerLevel_ > 0 && pathsToStat_ > 0) {
        ret = 0;
      }
    } catch (Exception e) {
      System.out.println("Error: " + e.getMessage());
    }
    return ret;
  }

  private static long timeDiffMilliSec(Date alpha, Date zigma)
  {
    return zigma.getTime() - alpha.getTime();
  }

  private static int CreateDFSPaths(int level, String parentPath) {
    Boolean isLeaf = false;
    Boolean isDir = false;
    if (level + 1 >= levels_) {
      isLeaf = true;
    }
    if (isLeaf) {
      if (type_.equals("dir")) {
        isDir = true;
      } else {
        isDir = false;
      }
    } else {
      isDir = true;
    }

    int err = 0;
    for (int i = 0; i < inodesPerLevel_; i++) {
      String path = parentPath + "/" + prefix_ + Integer.toString(i);
      //System.out.printf("Creating (isdir=%b) [%s]\n", isDir, path.toString());

      if (isDir) {
        try {
          if (dfsClient_.mkdirs(path) == false) {
            System.out.printf("Error in mkdirs(%s)\n", path);
            return -1;
          }
          totalCreateCount ++;
          if (totalCreateCount % COUNT_INCR == 0) {
            System.out.printf("Created paths so far: %d\n", totalCreateCount);
          }
          if (!isLeaf) {
            if (CreateDFSPaths(level+1, path) < 0) {
              System.out.printf("Error in CreateDFSPaths(%s)\n", path);
              return -1;
            }
          }
        } catch(IOException e) {
          e.printStackTrace();
          return -1;
        }
      } else {
        try {
          dfsClient_.create(path, true);
          totalCreateCount ++;
          if (totalCreateCount % COUNT_INCR == 0) {
            System.out.printf("Created paths so far: %d\n", totalCreateCount);
          }
        } catch( IOException e) {
          e.printStackTrace();
          return -1;
        }
      }
    }
    return 0;
  }

  private static int createDFSPaths()
  {
    String basePath = new String(TEST_BASE_DIR) + "/" + hostName_ + "_" + processName_;
    try {
      Boolean ret = dfsClient_.mkdirs(basePath);
      if (!ret) {
        System.out.printf("Error: failed to create test base dir [%s]\n", basePath);
        return -1;
      }
    } catch( IOException e) {
      e.printStackTrace();
      throw new RuntimeException();
    }

    Date alpha = new Date();

    if (CreateDFSPaths(0, basePath) < 0) {
      return -1;
    }

    Date zigma = new Date();
    System.out.printf("Client: %d paths created in %d msec\n", totalCreateCount, timeDiffMilliSec(alpha, zigma));
    return 0;
  }

  private static int statDFSPaths()
  {
    String basePath = new String(TEST_BASE_DIR) + "/" + hostName_ + "_" + processName_;

    Date alpha = new Date();
    Random random = new Random(alpha.getTime());

    for (int count = 0; count < pathsToStat_; count++) {
      String path = basePath;
      for (int d = 0; d < levels_; d++) {
        int randIdx = random.nextInt(inodesPerLevel_);
        String name = new String(prefix_) + Integer.toString(randIdx);
        path = path + "/" + name;
      }

      //System.out.printf("Doing stat on [%s]\n", path);
      HdfsFileStatus stat = null;
      try {
        stat = dfsClient_.getFileInfo(path);
      } catch(IOException e) {
        e.printStackTrace();
        return -1;
      }
      if (count % COUNT_INCR == 0) {
        System.out.printf("Stat paths so far: %d\n", count);
      }
    }
    Date zigma = new Date();
    System.out.printf("Client: Stat done on %d paths in %d msec\n", pathsToStat_, timeDiffMilliSec(alpha, zigma));
    return 0;
  }

  private static int listDFSPaths()
  {
    Date alpha = new Date();
    int inodeCount = 0;

    String basePath = new String(TEST_BASE_DIR) + "/" + hostName_ + "_" + processName_;
    Queue<String> pending = new LinkedList<String>();
    pending.add(basePath);

    while (!pending.isEmpty()) {
      String parent = pending.remove();
      DirectoryListing thisListing;
      try {
        thisListing = dfsClient_.listPaths(parent, HdfsFileStatus.EMPTY_NAME);
        if (thisListing == null || thisListing.getPartialListing().length == 0) {
          //System.out.println("Empty directory");
          continue;
        }
        do {
          HdfsFileStatus[] children = thisListing.getPartialListing();
          for (int i = 0; i < children.length; i++) {
            String localName = children[i].getLocalName();
            //System.out.printf("Readdir going through [%s/%s]\n", parent, localName);
            if (localName.equals(".") || localName.equals("..")) {
              continue;
            }
            inodeCount ++;
            if (inodeCount % COUNT_INCR == 0) {
              System.out.printf("Readdir paths so far: %d\n", inodeCount);
            }
            if (children[i].isDir()) {
              pending.add(parent + "/" + localName);
            }
          }
          if (!thisListing.hasMore()) {
            break;
          } else {
            //System.out.println("Remaining entries " + Integer.toString(thisListing.getRemainingEntries()));
          }
          thisListing = dfsClient_.listPaths(parent, thisListing.getLastName());
        } while (thisListing != null);
      } catch (IOException e) {
        e.printStackTrace();
        return -1;
      }
    }

    Date zigma = new Date();
    System.out.printf("Client: Directory walk done over %d inodes in %d msec\n", inodeCount, timeDiffMilliSec(alpha, zigma));
    return 0;
  }

  private static int removeDFSPaths()
  {
    String rmPath = new String(TEST_BASE_DIR) + "/" + hostName_ + "_" + processName_;

    System.out.printf("Deleting %s ...\n", rmPath);

    int countLeaf = (int) Math.round(Math.pow(inodesPerLevel_, levels_));
    int[] leafIdxRangeForDel = new int[countLeaf];
    for(int i=0;i<countLeaf;i++)
        leafIdxRangeForDel[i] = i;
    Collections.shuffle(Arrays.asList(leafIdxRangeForDel));

    Date alpha = new Date();
    try {
      for(int idx : leafIdxRangeForDel) {
          String path = "";
          for(int lev=0; lev < levels_; lev++) {
             int delta = idx % inodesPerLevel_;
             idx /= inodesPerLevel_;
             if(path.length() > 0) {
                 path = prefix_ + delta + "/" + path;
             } else {
                 path = prefix_ + delta;
             }
          }
          dfsClient_.delete(rmPath + "/" + path,true);
      }
      dfsClient_.delete(rmPath, true);
    } catch(IOException e) {
      e.printStackTrace();
      return -1;
    }
    Date zigma = new Date();
    System.out.printf("Client: Deleted %s. Delete took %d msec\n", rmPath, timeDiffMilliSec(alpha, zigma));
    return 0;
  }
}
