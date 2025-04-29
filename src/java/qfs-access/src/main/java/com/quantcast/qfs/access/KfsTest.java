/**
 * $Id$
 *
 * Created 2007/08/25
 *
 * Copyright 2008-2017 Quantcast Corporation. All rights reserved.
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
 * \brief A Java unit test program to access KFSAccess APIs.
 */


package com.quantcast.qfs.access;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class KfsTest
{
    @SuppressWarnings("UseSpecificCatch")
    public static void main(String args[]) {
        if (args.length < 1) {
            System.out.println("Usage: KfsTest <meta server> <port>");
            System.exit(1);
        }
        try {
            int port = Integer.parseInt(args[1].trim());
            KfsAccess kfsAccess = new KfsAccess(args[0], port);

            String basedir = "jtest";
            final String euidp = System.getProperty("kfs.euid");
            final String egidp = System.getProperty("kfs.egid");
            final long   euid  = (euidp != null && euidp.length() > 0) ?
                Long.decode(euidp) : KfsFileAttr.KFS_USER_NONE;
            final long   egid  = (egidp != null && egidp.length() > 0) ?
                Long.decode(egidp) : KfsFileAttr.KFS_GROUP_NONE;
            if (euid != KfsFileAttr.KFS_USER_NONE ||
                    egid != KfsFileAttr.KFS_GROUP_NONE) {
                kfsAccess.kfs_retToIOException(
                    kfsAccess.kfs_setEUserAndEGroup(euid, egid, null));
            }

            KfsDelegation dtoken = null;
            try {
                final boolean       allowDelegationFlag   = true;
                final long          delegationValidForSec = 24 * 60 * 60;
                final KfsDelegation result                =
                    kfsAccess.kfs_createDelegationToken(
                        allowDelegationFlag, delegationValidForSec);
                System.out.println("create delegation token: ");
                System.out.println("delegation:  " + result.delegationAllowedFlag);
                System.out.println("issued:      " + result.issuedTime);
                System.out.println("token valid: " + result.tokenValidForSec);
                System.out.println("valid:       " + result.delegationValidForSec);
                System.out.println("token:       " + result.token);
                System.out.println("key:         " + result.key);
                dtoken = result;
            } catch (IOException ex) {
                String msg = ex.getMessage();
                if (msg == null) {
                    msg = "null";
                }
                System.out.println("create delegation token error: " + msg);
            }

            if (dtoken != null) {
                try {
                    kfsAccess.kfs_renewDelegationToken(dtoken);
                    System.out.println("renew delegation token: ");
                    System.out.println("delegation:  " + dtoken.delegationAllowedFlag);
                    System.out.println("issued:      " + dtoken.issuedTime);
                    System.out.println("token valid: " + dtoken.tokenValidForSec);
                    System.out.println("valid:       " + dtoken.delegationValidForSec);
                    System.out.println("token:       " + dtoken.token);
                    System.out.println("key:         " + dtoken.key);
                } catch (IOException ex) {
                    String msg = ex.getMessage();
                    if (msg == null) {
                        msg = "null";
                    }
                    System.out.println("renew delegation token error: " + msg);
                    throw ex;
                }
                try {
                    KfsDelegation ctoken = new KfsDelegation();
                    ctoken.token = dtoken.token;
                    ctoken.key   = dtoken.key;
                    kfsAccess.kfs_cancelDelegationToken(ctoken);
                } catch (IOException ex) {
                    String msg = ex.getMessage();
                    if (msg == null) {
                        msg = "null";
                    }
                    System.out.println("cancel delegation token error: " + msg);
                    throw ex;
                }
                try {
                    kfsAccess.kfs_renewDelegationToken(dtoken);
                } catch (IOException ex) {
                    String msg = ex.getMessage();
                    if (msg == null) {
                        msg = "null";
                    }
                    System.out.println("renew canceled delegation token error: " + msg);
                    dtoken = null;
                }
                if (dtoken != null) {
                    throw new IOException("Token renew after cancellation succeeded");
                }
            }

            if (! kfsAccess.kfs_exists(basedir)) {
                kfsAccess.kfs_retToIOException(kfsAccess.kfs_mkdirs(basedir));
            }

            if (! kfsAccess.kfs_isDirectory(basedir)) {
                throw new IOException("QFS doesn't think " + basedir + " is a dir!");

            }
            final String fname = "foo.1";
            final String path = basedir + "/" + fname;
            final KfsOutputChannel outputChannel = kfsAccess.kfs_create(path);

            long mTime = kfsAccess.kfs_getModificationTime(path);
            Date d = new Date(mTime);
            System.out.println("Modification time for: " + path + " is: " + d.toString());

            // test readdir and readdirplus
            String [] entries;
            if ((entries = kfsAccess.kfs_readdir(basedir)) == null) {
                throw new IOException(basedir + ": readdir failed");
            }

            System.out.println("Readdir returned: ");
            for (String entrie : entries) {
                System.out.println(entrie);
            }

            final String absent = basedir + "/must not exist";
            if ((entries = kfsAccess.kfs_readdir(absent)) != null) {
                throw new IOException(absent + ": kfs_readdir: " + absent +
                    " non null, size: " + entries.length);
            }

            // write something
            int numBytes = 2048;
            char [] dataBuf = new char[numBytes];

            generateData(dataBuf, numBytes);

            String s = new String(dataBuf);
            byte[] buf = s.getBytes();

            ByteBuffer b = ByteBuffer.wrap(buf, 0, buf.length);
            int res = outputChannel.write(b);
            if (res != buf.length) {
                throw new IOException(
                    path + ": was able to write only: " + res);
            }
            // flush out the changes
            outputChannel.sync();
            outputChannel.close();

            final String symName = "foo.1.sym";
            final String slPath = basedir + "/" + symName;
            boolean overwrite = false;
            kfsAccess.kfs_retToIOException(
                kfsAccess.kfs_symlink(path + ".1", slPath, 0777, overwrite),
                "kfs_symlink " + slPath
            );
            res = kfsAccess.kfs_symlink(path + "2", slPath, 0777, overwrite);
            if (0 == res) {
                throw new IOException(
                    slPath + ": symlink succeeded: " + res);
            }
            System.out.println(slPath + ": " + res);
            overwrite = true;
            kfsAccess.kfs_retToIOException(
                kfsAccess.kfs_symlink(fname, slPath, 0777, overwrite),
                "kfs_symlink overwrite " + slPath
            );
            KfsFileAttr slattr = new KfsFileAttr();
            kfsAccess.kfs_retToIOException(
                kfsAccess.kfs_lstat(slPath, slattr),
                "kfs_lstat overwrite " + slPath
            );
            System.out.println(attrToString(slattr, "\n"));
            if (KfsFileAttr.KFS_FILE_ATTR_EXT_TYPE_SYM_LINK !=
                    slattr.extAttrTypes) {
                throw new IOException(
                    slPath + ": lstat extAttrTypes: " + slattr.extAttrTypes +
                    " expected: " + KfsFileAttr.KFS_FILE_ATTR_EXT_TYPE_SYM_LINK);
            }
            if (null == slattr.extAttrs || ! slattr.extAttrs.equals(fname)) {
                throw new IOException(
                    slPath + ": lstat: " + slattr.extAttrs +
                    " expected: " + fname);
            }
            kfsAccess.kfs_retToIOException(
                kfsAccess.kfs_stat(slPath, slattr),
                "kfs_stat " + slPath
            );
            System.out.println(attrToString(slattr, "\n"));
            if (null != slattr.extAttrs) {
                throw new IOException(
                    slPath + ": stat: " + slattr.extAttrs);
            }

            KfsFileAttr[] fattr;
            if ((fattr = kfsAccess.kfs_readdirplus(basedir)) == null) {
                throw new IOException(basedir + ": kfs_readdirplus failed");
            }
            System.out.println("kfs_readdirplus returned: ");
            for (KfsFileAttr fattr1 : fattr) {
                System.out.println(attrToString(fattr1, "\n"));
            }

            if ((fattr = kfsAccess.kfs_readdirplus(absent)) != null) {
                throw new IOException("kfs_readdirplus: " + (Object)fattr +
                    ": non null, size: " + fattr.length);
            }

            System.out.println("Trying to lookup blocks for file: " + path);

            String [][] locs;
            if ((locs = kfsAccess.kfs_getDataLocation(path, 10, 512)) == null) {
                throw new IOException(path + ": kfs_getDataLocation failed");
            }

            System.out.println("Block Locations:");
            for (int i = 0; i < locs.length; i++) {
                System.out.print("chunk " + i + " : ");
                for (String loc : locs[i]) {
                    System.out.print(loc + " ");
                }
                System.out.println();
            }

            if ((locs = kfsAccess.kfs_getBlocksLocation(path, 10, 512)) == null) {
                throw new IOException(path + ": kfs_getBlocksLocation failed");
            }
            if (locs.length < 1 || locs[0].length != 1) {
                throw new IOException(
                    path + ": kfs_getBlocksLocation invalid first slot length");
            }
            final long blockSize = Long.parseLong(locs[0][0], 16);
            if (blockSize < 0) {
                kfsAccess.kfs_retToIOException((int)blockSize, path);
            }
            System.out.println("block size: " + blockSize);
            for (int i = 1; i < locs.length; i++) {
                System.out.print("chunk " + (i-1) + " : ");
                for (String loc : locs[i]) {
                    System.out.print(loc + " ");
                }
                System.out.println();
            }

            long sz = kfsAccess.kfs_filesize(path);

            if (sz != buf.length) {
                System.out.println("System thinks the file's size is: " + sz);
            }

            KfsFileAttr attr = new KfsFileAttr();
            final int ret = kfsAccess.kfs_stat(path, attr);
            if (ret != 0) {
                throw new IOException(path + ": stat failed: " + ret);
            }
            System.out.println("stat: \n" + attrToString(attr, "\n"));

            // rename the file
            String npath = basedir + "/foo.2";
            kfsAccess.kfs_rename(path, npath);

            if (kfsAccess.kfs_exists(path)) {
                throw new IOException(path + " still exists after rename!");
            }

            KfsOutputChannel outputChannel1 = kfsAccess.kfs_create(path);

            if (outputChannel1 != null) {
                outputChannel1.close();
            }

            if (!kfsAccess.kfs_exists(path)) {
                throw new IOException(path + " doesn't exist");
            }

            // try to rename and don't allow overwrite
            if (kfsAccess.kfs_rename(npath, path, false) == 0) {
                throw new IOException(
                    "rename with overwrite disabled succeeded!");
            }

            kfsAccess.kfs_retToIOException(
                kfsAccess.kfs_remove(path),
                "kfs_remove: " + path
            );
            kfsAccess.kfs_retToIOException(
                kfsAccess.kfs_remove(slPath),
                "kfs_remove: " + slPath
            );

            if (!kfsAccess.kfs_isFile(npath)) {
                throw new IOException(npath + " is not a normal file!");
            }

            KfsInputChannel inputChannel = kfsAccess.kfs_open(npath);
            if (inputChannel == null) {
                throw new IOException("open on " + npath + "failed!");
            }

            // read some bytes
            buf = new byte[128];
            res = inputChannel.read(ByteBuffer.wrap(buf, 0, 128));
            if (res != 128) {
                throw new IOException(
                    npath + ": was able to read only: " + res);
            }

            s = new String(buf);
            for (int i = 0; i < 128; i++) {
                if (dataBuf[i] != s.charAt(i)) {
                    System.out.println("Data mismatch at char: " + i);
                }
            }

            // seek to offset 40
            inputChannel.seek(40);

            sz = inputChannel.tell();
            if (sz != 40) {
                System.out.println("After seek, we are at: " + sz);
            }

            inputChannel.close();

            testUtimes(kfsAccess, npath);

            // remove the file
            kfsAccess.kfs_retToIOException(
                kfsAccess.kfs_remove(npath),
                "kfs_remove: " + npath
            );

            testDirs(kfsAccess, basedir + "/");

            // Test recursive remove.
            final String rtest    = basedir + "/rtest";
            final String testPath = rtest + "/a/b/../../c/../d";
            kfsAccess.kfs_retToIOException(kfsAccess.kfs_mkdirs(testPath));
            if (! kfsAccess.kfs_exists(testPath)) {
                throw new IOException(testPath + " doesn't exist");
            }
            kfsAccess.kfs_create(testPath + "/test_file").close();
            kfsAccess.kfs_retToIOException(kfsAccess.kfs_rmdirs(rtest));
            if (kfsAccess.kfs_exists(rtest)) {
                throw new IOException(rtest + " exist");
            }

            // test new create methods
            testCreateAPI(kfsAccess, basedir);

            // test read when read-ahead is disabled
            testDisableReadAhead(kfsAccess, basedir);

            final Iterator<Map.Entry<String, String> > it =
                kfsAccess.kfs_getStats().entrySet().iterator();
            System.out.println("Clients stats:");
            while (it.hasNext()) {
                final Map.Entry<String, String> entry = it.next();
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

            testUtimes(kfsAccess, basedir);
            // remove the dir
            kfsAccess.kfs_retToIOException(kfsAccess.kfs_rmdir(basedir));
            System.out.println("All done...Test passed!");
        } catch (Exception e) {
            e.printStackTrace(System.out);
            System.out.println(e.getMessage());
            System.out.println("Test failed");
            System.exit(1);
        }
    }

    private static final Random randGen = new Random(100);

    private static void generateData(char buf[], int numBytes)
    {
        int i;
        // String nameBuf = new String("sriram");

        for (i = 0; i < numBytes; i++) {
            buf[i] = (char) ('a' + (randGen.nextInt(26)));
            // buf[i] = nameBuf.charAt((i + 6) % 6);
        }
    }

    private static String attrToString(KfsFileAttr attr, String delim)
    {
        return
        "filename: "           + attr.filename + delim +
        "isDirectory: "        + attr.isDirectory + delim +
        "filesize: "           + attr.filesize + delim +
        "modificationTime: "   + attr.modificationTime + delim +
        "attrChangeTime: "     + attr.attrChangeTime + delim +
        "creationTime: "       + attr.creationTime + delim +
        "replication: "        + attr.replication + delim +
        "striperType: "        + attr.striperType + delim +
        "numStripes: "         + attr.numStripes + delim +
        "numRecoveryStripes: " + attr.numRecoveryStripes + delim +
        "stripeSize: "         + attr.stripeSize + delim +
        "minSTier: "           + attr.minSTier + delim +
        "maxSTier: "           + attr.maxSTier + delim +
        "owner: "              + attr.owner + delim +
        "group: "              + attr.group + delim +
        "mode: "               + attr.mode + delim +
        "fileId: "             + attr.fileId + delim +
        "dirCount: "           + attr.dirCount + delim +
        "fileCount: "          + attr.fileCount + delim +
        "chunkCount: "         + attr.chunkCount + delim +
        "ownerName: "          + attr.ownerName + delim +
        "groupName: "          + attr.groupName + delim +
        "extAttrTypes: "       + attr.extAttrTypes + delim +
        "extAttrs: "           + attr.extAttrs
        ;
    }

    private static void testDirs(KfsAccess fs, String root) throws IOException
    {
        final String oldDir = root + "old";
        final String newDir = root + "new";
        for(int i = 0; i < 4; i++) {
            fs.kfs_retToIOException(fs.kfs_mkdirs(oldDir));
            System.out.println("Pass " + i + ": " +
                oldDir + " exists " + fs.kfs_exists(oldDir));
            if (fs.kfs_exists(newDir)) {
                delete(fs, newDir);
            }
            rename(fs, oldDir, newDir);
        }
        delete(fs, newDir);
    }

    private static void rename(KfsAccess kfsAccess, String source, String dest)
            throws IOException
    {
        // KFS rename does not have mv semantics.
        // To move /a/b under /c/, you must ask for "rename /a/b /c/b"
    String renameTarget;
    if (kfsAccess.kfs_isDirectory(dest)) {
        String sourceBasename = (new File(source)).getName();
        if (dest.endsWith("/")) {
        renameTarget = dest + sourceBasename;
        } else {
        renameTarget = dest + "/" + sourceBasename;
        }
    } else {
        renameTarget = dest;
    }
    kfsAccess.kfs_retToIOException(
            kfsAccess.kfs_rename(source, renameTarget));
    }

    // recursively delete the directory and its contents
    private static void delete(KfsAccess kfsAccess, String path)
            throws IOException {
        kfsAccess.kfs_retToIOException(kfsAccess.kfs_isFile(path) ?
            kfsAccess.kfs_remove(path) : kfsAccess.kfs_rmdirs(path)
        );
    }

    private static void testCreateAPI(KfsAccess kfsAccess, String baseDir)
            throws IOException {
        kfsAccess.kfs_retToIOException(kfsAccess.kfs_setUMask(0));
        final String filePath1 = baseDir + "/sample_file.1";
        final boolean exclusiveFlag = true;
        final String  createParams = "1,6,3,1048576,2,15,15";
        final boolean forceTypeFlag = true;
        KfsOutputChannel outputChannel = kfsAccess.kfs_create_ex(filePath1,
                exclusiveFlag, createParams, 0600, forceTypeFlag);
        verifyFileAttr(kfsAccess, outputChannel, filePath1, 0600);

        String filePath2 = baseDir + "/sample_file.2";
        outputChannel = kfsAccess.kfs_create_ex(filePath2, 1, exclusiveFlag,
                -1, -1, 6, 3, 1048576, 2, false, 0666, 15, 15);
        verifyFileAttr(kfsAccess, outputChannel, filePath2, 0666);
        delete(kfsAccess, filePath1);
        delete(kfsAccess, filePath2);
    }

    private static void verifyFileAttr(KfsAccess kfsAccess,
            KfsOutputChannel outputChannel, String filePath, int mode)
            throws IOException {
        final int numBytes = 1048576;
        final char[] dataBuf = new char[numBytes];
        generateData(dataBuf, numBytes);
        final String s = new String(dataBuf);
        final byte[] buf = s.getBytes();
        final ByteBuffer b = ByteBuffer.wrap(buf, 0, buf.length);
        final int res = outputChannel.write(b);
        if (res != buf.length) {
            throw new IOException(
                filePath + ": was able to write only: " + res);
        }
        outputChannel.sync();
        outputChannel.close();
        KfsFileAttr attr = new KfsFileAttr();
        kfsAccess.kfs_retToIOException(kfsAccess.kfs_stat(filePath, attr));
        if (numBytes != attr.filesize || attr.replication != 1 ||
                attr.striperType != 2 || attr.numStripes != 6 ||
                attr.numRecoveryStripes != 3 || attr.stripeSize != 1048576 ||
                mode != attr.mode) {
            throw new IOException(filePath + ": file attributes mismatch");
        }
    }

    private static void testDisableReadAhead(KfsAccess kfsAccess, String baseDir)
            throws IOException {
        final String filePath = baseDir + "/sample_file.1";
        final String createParams = "S";
        final KfsOutputChannel outputChannel = kfsAccess.kfs_create_ex(filePath,
                true, createParams);
        final int numBytes = 1048576;
        final char[] dataBuf = new char[numBytes];
        generateData(dataBuf, numBytes);
        String s = new String(dataBuf);
        final byte[] buf = s.getBytes();
        final ByteBuffer b = ByteBuffer.wrap(buf, 0, buf.length);
        int res = outputChannel.write(b);
        if (res != buf.length) {
            throw new IOException(filePath + ": was able to write only: " + res);
        }
        outputChannel.sync();
        outputChannel.close();

        final KfsInputChannel inputChannel = kfsAccess.kfs_open(filePath);
        inputChannel.setReadAheadSize(0);
        final byte[] dstBuf = new byte[128];
        res = inputChannel.read(ByteBuffer.wrap(dstBuf, 0, 128));
        if (res != 128) {
            throw new IOException(
                filePath + ": was able to read only: " + res);
        }
        s = new String(dstBuf);
        for (int i = 0; i < 128; i++) {
            if (dataBuf[i] != s.charAt(i)) {
                throw new IOException(
                    filePath + ": data mismatch at char: " + i);
            }
        }
        inputChannel.seek(512);
        long pos = inputChannel.tell();
        if (pos != 512) {
            throw new IOException(
                filePath + "failed to seek to byte 512. Pos: " + pos);
        }
        res = inputChannel.read(ByteBuffer.wrap(dstBuf, 0, 128));
        if (res != 128) {
            throw new IOException(
                filePath + ": was able to read only: " + res);
        }
        s = new String(dstBuf);
        for (int i = 0; i < 128; i++) {
            if (dataBuf[512+i] != s.charAt(i)) {
                throw new IOException(filePath + ": data mismatch at char " + i +
                                   " after seeking to byte 512");
            }
        }
        // seek to the beginning, enable read-ahead and make a small read
        inputChannel.seek(0);
        pos = inputChannel.tell();
        if (pos != 0) {
            throw new IOException(
                filePath + ": failed to seek to the beginning. pos: " + pos);
        }
        inputChannel.setReadAheadSize(1048576);
        res = inputChannel.read(ByteBuffer.wrap(dstBuf, 0, 128));
        if (res != 128) {
            throw new IOException(
                filePath + ": was able to read only: " + res);
        }
        s = new String(dstBuf);
        for (int i = 0; i < 128; i++) {
            if (dataBuf[i] != s.charAt(i)) {
                throw new IOException(filePath + ": data mismatch at char " +
                                   i + " after seeking to the beginning");
            }
        }
        inputChannel.close();
        delete(kfsAccess, filePath);
    }

    private static void testUtimes(KfsAccess kfsAccess, String path)
            throws IOException {
        KfsFileAttr attr = new KfsFileAttr();
        kfsAccess.kfs_retToIOException(kfsAccess.kfs_stat(path, attr), path);
        final long mtimeMs = attr.modificationTime - 5001;
        final long atimeMs = attr.creationTime     - 10001;
        final long ctimeMs = attr.attrChangeTime   - 8001;
        kfsAccess.kfs_retToIOException(kfsAccess.kfs_setUTimes(path,
            mtimeMs * 1000, atimeMs * 1000, ctimeMs * 1000), path);
        for (int pass = 0; pass < 2; pass++) {
            kfsAccess.kfs_retToIOException(kfsAccess.kfs_stat(path, attr), path);
            if (mtimeMs != attr.modificationTime ||
                    atimeMs != attr.creationTime ||
                    ctimeMs != attr.attrChangeTime) {
                throw new IOException(
                    path + ": kfs_setUTimes result mismatch: " +
                    " mtime: " + mtimeMs + " -> " + attr.modificationTime +
                    " atime: " + atimeMs + " -> " + attr.creationTime +
                    " ctime: " + ctimeMs + " -> " + attr.attrChangeTime +
                    " pass: " + pass +
                    " SET_TIME_TIME_NOT_VALID: " +
                        String.format("0x%x", kfsAccess.SET_TIME_TIME_NOT_VALID)
                );
            }
            kfsAccess.kfs_retToIOException(kfsAccess.kfs_setUTimes(path,
                mtimeMs * 1000,
                kfsAccess.SET_TIME_TIME_NOT_VALID,
                kfsAccess.SET_TIME_TIME_NOT_VALID), path);
        }
    }
}
