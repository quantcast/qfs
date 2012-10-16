/**
 * $Id$
 *
 * Created 2007/08/25
 * Author: Sriram Rao
 *
 * Copyright 2012 Quantcast Corp.
 * Copyright 2007 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * \brief A Sample Java program to access QFSAccess APIs. To run this program,
 *        you need:
 *        - qfs-access jar in your CLASSPATH
 *        - libqfs_access.so in your LD_LIBRARY_PATH
 *        - a QFS deployment
 * Eg:
 *    javac -cp ~/code/qfs/build/java/qfs-access-<ver>.jar QfsSample.java
 *    LD_LIBRARY_PATH=~/code/qfs/build/lib \
 *        java -cp .:~/code/qfs/build/java/qfs-access-<ver>.jar QfsSample \
 *        localhost 20000
 */


import java.io.*;
import java.net.*;
import java.util.Random;
import java.nio.ByteBuffer;
import com.quantcast.qfs.access.*;

public class QfsSample
{
    public static void main(String args[]) {
        if (args.length < 1) {
            System.out.println("Usage: QfsSample <meta server> <port>");
            System.exit(1);
        }
        try {
            // From the command line, get the location of the meta-server
            int port = Integer.parseInt(args[1].trim());

            // Initialize a KfsAccess object.  The KfsAccess object is
            // the glue code that gets us into KFS world.
            //
            KfsAccess kfsAccess = new KfsAccess(args[0], port);

            String basedir = new String("jtest");

            // Make a test directory where we can do something
            if (!kfsAccess.kfs_exists(basedir)) {
                if (kfsAccess.kfs_mkdirs(basedir) != 0) {
                    System.out.println("Unable to mkdir");
                    System.exit(1);
                }
            }

            // What we just created better be a directory
            if (!kfsAccess.kfs_isDirectory(basedir)) {
                System.out.println("KFS doesn't think " + basedir + " is a dir!");
                System.exit(1);

            }

            // Create a simple file with default replication (at most 3)
            String path = new String(basedir + "/foo.1");
            KfsOutputChannel outputChannel;

            // outputChannel implements a WriteableChannel interface;
            // it is our handle to subsequent I/O on the file.
            if ((outputChannel = kfsAccess.kfs_create(path)) == null) {
                System.out.println("Unable to call create");
                System.exit(1);
            }

            // Get the directory listings
            String [] entries;
            if ((entries = kfsAccess.kfs_readdir(basedir)) == null) {
                System.out.println("Readdir failed");
                System.exit(1);
            }

            System.out.println("Readdir returned: ");
            for (int i = 0; i < entries.length; i++) {
                System.out.println(entries[i]);
            }

            // write something to the file
            int numBytes = 2048;
            char [] dataBuf = new char[numBytes];

            generateData(dataBuf, numBytes);

            String s = new String(dataBuf);
            byte[] buf = s.getBytes();

            int res = outputChannel.write(ByteBuffer.wrap(buf, 0, buf.length));
            if (res != buf.length) {
                System.out.println("Was able to write only: " + res);
            }

            // flush out the changes
            outputChannel.sync();

            // Close the file-handle
            outputChannel.close();

            // Determine the file-size
            long sz = kfsAccess.kfs_filesize(path);

            if (sz != buf.length) {
                System.out.println("System thinks the file's size is: " + sz);
            }

            // rename the file
            String npath = new String(basedir + "/foo.2");
            kfsAccess.kfs_rename(path, npath);

            if (kfsAccess.kfs_exists(path)) {
                System.out.println(path + " still exists after rename!");
                System.exit(1);
            }

            KfsOutputChannel outputChannel1 = kfsAccess.kfs_create(path);
            if (outputChannel1 != null) {
                outputChannel1.close();
            }

            if (!kfsAccess.kfs_exists(path)) {
                System.out.println(path + " doesn't exist");
                System.exit(1);
            }

            // try to rename and don't allow overwrite
            if (kfsAccess.kfs_rename(npath, path, false) == 0) {
                System.out.println("Rename with overwrite disabled succeeded!");
                System.exit(1);
            }

            // Remove the file
            kfsAccess.kfs_remove(path);

            // Verify that it is gone
            if (!kfsAccess.kfs_isFile(npath)) {
                System.out.println(npath + " is not a normal file!");
                System.exit(1);
            }

            // Re-open the file to read something.  For reads/writes,
            // Kfs provides a readable/writeable byte channel interface.
            KfsInputChannel inputChannel = kfsAccess.kfs_open(npath);
            if (inputChannel == null) {
                System.out.println("open on " + npath + "failed!");
                System.exit(1);
            }

            // read some bytes
            buf = new byte[128];
            res = inputChannel.read(ByteBuffer.wrap(buf, 0, 128));

            // Verify what we read matches what we wrote
            s = new String(buf);
            for (int i = 0; i < 128; i++) {
                if (dataBuf[i] != s.charAt(i)) {
                    System.out.println("Data mismatch at char: " + i);
                }
            }

            // seek to offset 40.  The KfsInputChannel allows seeking;
            // this is an extension to the basic readablebytechannel api.
            inputChannel.seek(40);

            // Seek and verify that we are we think we are
            sz = inputChannel.tell();
            if (sz != 40) {
                System.out.println("After seek, we are at: " + sz);
            }

            inputChannel.close();

            // remove the file
            kfsAccess.kfs_remove(npath);

            // remove the dir
            if (kfsAccess.kfs_rmdir(basedir) < 0) {
                System.out.println("unable to remove: " + basedir);
                System.exit(1);
            }
            System.out.println("All done...Test passed!");

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Unable to setup KfsAccess");
            System.exit(1);
        }
    }

    private static Random randGen = new Random(100);

    private static void generateData(char buf[], int numBytes)
    {
        int i;

        for (i = 0; i < numBytes; i++) {
            buf[i] = (char) ('a' + (randGen.nextInt(26)));
        }
    }

}
