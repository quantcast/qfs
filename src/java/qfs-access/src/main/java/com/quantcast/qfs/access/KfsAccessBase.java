/**
 * $Id$
 *
 * Created 2007/08/24
 * @author: Sriram Rao (Kosmix Corp.)
 *
 * Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
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
 * \brief Java wrappers to get to the KFS client.
 */

package com.quantcast.qfs.access;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;

class KfsAccessBase
{
    public final int DEFAULT_APPEND_REPLICATION   = 2;
    public final int DEFAULT_REPLICATION          = 1;
    public final int DEFAULT_NUM_STRIPES          = 6;
    public final int DEFAULT_NUM_RECOVERY_STRIPES = 3;
    public final int DEFAULT_STRIPE_SIZE          = 65536;
    public final int DEFAULT_STRIPER_TYPE         =
        KfsFileAttr.STRIPED_FILE_TYPE_RS;
    public final long SET_TIME_TIME_NOT_VALID     = 1L << 63;

    // the pointer in C++
    private long cPtr;

    private static native
    long initF(String configFn);

    private static native
    long initS(String metaServerHost, int metaServerPort);

    final protected static native
    void destroy(long ptr);

    private static native
    int cd(long ptr, String  path);

    private static native
    int mkdir(long ptr, String  path, int mode);

    private static native
    int mkdirs(long ptr, String  path, int mode);

    private static native
    int rmdir(long ptr, String  path);

    private static native
    int rmdirs(long ptr, String  path);

    private static native
    String[] readdir(long ptr, String path, boolean prefetchAttr);

    private static native
    String[][] getDataLocation(long ptr, String path, long start, long len);

    private static native
    String[][] getBlocksLocation(long ptr, String path, long start, long len);

    private static native
    short getReplication(long ptr, String path);

    private static native
    short setReplication(long ptr, String path, int numReplicas);

    private static native
    long getModificationTime(long ptr, String path);

    private static native
    int create(long ptr, String path, int numReplicas, boolean exclusive,
        int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
        boolean forceType, int mode, int minSTier, int maxSTier);

    private static native
    int create2(long ptr, String path, boolean exclusive, String createParams);

    private static native
    int create2ex(long ptr, String path, boolean exclusive, String createParams,
        int mode, boolean forceTypeFlag);

    private static native
    int remove(long ptr, String path);

    private static native
    int rename(long ptr, String oldpath, String newpath, boolean overwrite);

    private static native
    int symlink(long ptr, String target, String linkpath, int mode, boolean overwrite);

    private static native
    int open(long ptr, String path, String mode, int numReplicas,
        int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
        int createMode);

    private static native
    int exists(long ptr, String path);

    private static native
    int isFile(long ptr, String path);

    private static native
    int isDirectory(long ptr, String path);

    private static native
    long filesize(long ptr, String path);

    private static native
    long setDefaultIoBufferSize(long ptr, long size);

    private static native
    long getDefaultIoBufferSize(long ptr);

    private static native
    long setDefaultReadAheadSize(long ptr, long size);

    private static native
    long getDefaultReadAheadSize(long ptr);

    private static native
    long setIoBufferSize(long ptr, int fd, long size);

    private static native
    long getIoBufferSize(long ptr, int fd);

    private static native
    long setReadAheadSize(long ptr, int fd, long size);

    private static native
    long getReadAheadSize(long ptr, int fd);

    private static native
    int setUTimes(long ptr, String path, long mtime_usec, long atime_usec, long ctime_usec);

    private static native
    int compareChunkReplicas(long ptr, String path, StringBuffer md5sum);

    // private static native
    // int getStripedType(long ptr, String path);

    private static native
    void setFileAttributeRevalidateTime(long ptr, int secs);

    private static native
    int chmod(long ptr, String path, int mode);

    private static native
    int chmodr(long ptr, String path, int mode);

    private static native
    int fchmod(long ptr, int fd, int mode);

    private static native
    int chowns(long ptr, String path, String user, String group);

    private static native
    int chownsr(long ptr, String path, String user, String group);

    private static native
    int chown(long ptr, String path, long user, long group);

    private static native
    int chownr(long ptr, String path, long user, long group);

    private static native
    int fchowns(long ptr, int fd, String user, String group);

    private static native
    int fchown(long ptr, int fd, long user, long group);

    private static native
    int setEUserAndEGroup(long ptr, long user, long group, long[] groups);

    private static native
    int stat(long ptr, String path, KfsFileAttr attr);

    private static native
    int lstat(long ptr, String path, KfsFileAttr attr);

    private static native
    String strerror(long ptr, int err);

    private static native
    boolean isnotfound(long ptr, int err);

    private static native
    int close(long ptr, int fd);

    private static native
    long seek(long ptr, int fd, long offset);

    private static native
    long tell(long ptr, int fd);

    private static native
    int setUMask(long ptr, int umask);

    private static native
    int getUMask(long ptr);

    private static native
    String createDelegationToken(long ptr, boolean allowDelegationFlag,
        long validTime, KfsDelegation result);

    private static native
    String renewDelegationToken(long ptr, KfsDelegation token);

    private static native
    String cancelDelegationToken(long ptr, KfsDelegation token);

    private static native
    String[] getStats(long ptr);

    static {
        try {
            System.loadLibrary("qfs_access");
        } catch (UnsatisfiedLinkError e) {
            throw new RuntimeException("Unable to load qfs_access native library", e);
        }
    }

    protected KfsAccessBase(String configFn) throws IOException
    {
        cPtr = initF(configFn);
        if (cPtr == 0) {
            throw new IOException("Unable to initialize KFS Client");
        }
    }

    protected KfsAccessBase(String metaServerHost,
            int metaServerPort) throws IOException
    {
        cPtr = initS(metaServerHost, metaServerPort);
        if (cPtr == 0) {
            throw new IOException("Unable to initialize KFS Client");
        }
    }

    // most calls wrap to a call on the KfsClient.  For return values,
    // see the comments in libkfsClient/KfsClient.h
    //
    final public int kfs_cd(String path)
    {
        return cd(cPtr, path);
    }

    // make the directory hierarchy for path
    final public int kfs_mkdirs(String path)
    {
        return mkdirs(cPtr, path, 0777);
    }

    // make the directory hierarchy for path
    final public int kfs_mkdirs(String path, int mode)
    {
        return mkdirs(cPtr, path, mode);
    }

    // make the directory hierarchy for path
    final public int kfs_mkdir(String path, int mode)
    {
        return mkdir(cPtr, path, mode);
    }

    // remove the directory specified by path; remove will succeed only if path is empty.
    final public int kfs_rmdir(String path)
    {
        return rmdir(cPtr, path);
    }

    // remove the directory tree specified by path; remove will succeed only if path is empty.
    final public int kfs_rmdirs(String path)
    {
        return rmdirs(cPtr, path);
    }

    final public String[] kfs_readdir(String path)
    {
        return kfs_readdir(path, false);
    }

    final public String[] kfs_readdir(String path, boolean prefetchAttr)
    {
        return readdir(cPtr, path, prefetchAttr);
    }

    final public class DirectoryIterator
    {
        public long    modificationTime;
        public long    attrChangeTime;
        public long    creationTime;
        public long    filesize;
        public int     replication;
        public boolean isDirectory;
        public int     numStripes;
        public int     numRecoveryStripes;
        public int     striperType;
        public int     stripeSize;
        public byte    minSTier;
        public byte    maxSTier;
        public String  filename;
        public long    owner;
        public long    group;
        public int     mode;
        public String  ownerName;
        public String  groupName;
        public long    dirCount;
        public long    fileCount;
        public long    chunkCount;
        public long    fileId;
        public int     extAttrTypes;
        public String  extAttrs;

        private KfsInputChannel      input;
        private ByteBuffer           buf;
        private int                  limit;
        private final CharsetDecoder decoder = Charset.forName("UTF-8")
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPLACE)
            .onUnmappableCharacter(CodingErrorAction.REPLACE);
        private long prevOwner;
        private long prevGroup;

        public DirectoryIterator(String path) throws IOException
        {
            final int fd = open(cPtr, path, "opendir", 0, 0, 0, 0, 0, 0);
            kfs_retToIOException(fd, path);
            input = null;
            try {
                input = new KfsInputChannel(KfsAccessBase.this, fd);
            } finally {
                if (input == null) {
                    KfsAccessBase.close(cPtr, fd);
                }
            }
        }

        private String readString(int len) throws IOException
        {
            if (len <= 0) {
                return "";
            }
            final int end = buf.position() + len;
            buf.limit(end);
            final String str = decoder.reset().decode(buf).toString();
            buf.position(end).limit(limit);
            return str;
        }

        private void skip(int len) throws IOException
        {
            if (len > 0) {
                buf.position(buf.position() + len);
            }
        }

        public boolean next() throws IOException
        {
            for (; ;) {
                if (buf == null || ! buf.hasRemaining()) {
                    if (input == null) {
                        return false;
                    }
                    buf = input.readNext();
                    limit = buf == null ? 0 : buf.limit();
                    if (limit <= 0) {
                        close();
                        return false;
                    }
                }
                modificationTime   = buf.getLong();
                attrChangeTime     = buf.getLong();
                creationTime       = buf.getLong();
                filesize           = buf.getLong();
                replication        = buf.getInt();
                final int nameLen  = buf.getInt();
                isDirectory        = buf.get() != 0;
                numStripes         = buf.getInt();
                numRecoveryStripes = buf.getInt();
                striperType        = buf.getInt();
                stripeSize         = buf.getInt();
                owner              = buf.getInt();
                group              = buf.getInt();
                mode               = buf.getShort();
                fileId             = buf.getLong();
                fileCount          = isDirectory ? buf.getLong() : 0;
                dirCount           = isDirectory ? buf.getLong() : 0;
                chunkCount         = isDirectory ? 0 : buf.getLong();
                minSTier           = buf.get();
                maxSTier           = buf.get();
                final int onameLen = buf.getInt();
                final int gnameLen = buf.getInt();
                extAttrTypes       = buf.getInt();
                final int exAtLen  =
                    KfsFileAttr.KFS_FILE_ATTR_EXT_TYPE_NONE == extAttrTypes ?
                    0 : buf.getInt();
                owner &= 0xFFFFFFFFL;
                group &= 0xFFFFFFFFL;
                mode  &= 0xFFFF;
                filename  = readString(nameLen);
                if (owner == prevOwner && ownerName != null) {
                    skip(onameLen);
                } else {
                    prevOwner = owner;
                    ownerName = readString(onameLen);
                }
                if (group == prevGroup && groupName != null) {
                    skip(gnameLen);
                } else {
                    prevGroup = group;
                    groupName = readString(gnameLen);
                }
                extAttrs = 0 < exAtLen ? readString(exAtLen) : null;
                if (nameLen > 0) {
                    break;
                }
            }
            return true;
        }

        public void close()
        {
            if (input == null) {
                return;
            }
            buf = null;
            try {
                input.close();
            } catch (IOException ignored) {
            }
            input = null;
        }
    }

    final public KfsFileAttr[] kfs_readdirplus(String path)
    {
        DirectoryIterator itr = null;
        try {
            itr = new DirectoryIterator(path);
            final ArrayList<KfsFileAttr> ret = new ArrayList<KfsFileAttr>();
            while (itr.next()) {
                KfsFileAttr entry = new KfsFileAttr();
                entry.modificationTime   = itr.modificationTime;
                entry.attrChangeTime     = itr.attrChangeTime;
                entry.creationTime       = itr.creationTime;
                entry.filesize           = itr.filesize;
                entry.replication        = itr.replication;
                entry.isDirectory        = itr.isDirectory;
                entry.filename           = itr.filename;
                entry.numStripes         = itr.numStripes;
                entry.numRecoveryStripes = itr.numRecoveryStripes;
                entry.striperType        = itr.striperType;
                entry.stripeSize         = itr.stripeSize;
                entry.owner              = itr.owner;
                entry.group              = itr.group;
                entry.mode               = itr.mode;
                entry.ownerName          = itr.ownerName;
                entry.groupName          = itr.groupName;
                entry.fileId             = itr.fileId;
                entry.dirCount           = itr.dirCount;
                entry.fileCount          = itr.fileCount;
                entry.chunkCount         = itr.chunkCount;
                entry.minSTier           = itr.minSTier;
                entry.maxSTier           = itr.maxSTier;
                entry.extAttrTypes       = itr.extAttrTypes;
                entry.extAttrs           = itr.extAttrs;
                ret.add(entry);
            }
            return ret.toArray(new KfsFileAttr[0]);
        } catch (IOException ex) {
            // System.err.println("kfs_readdirplus " + ex);
        } finally {
            if (itr != null) {
                itr.close();
            }
        }
        return null;
    }

    final public KfsOutputChannel kfs_append(String path)
    {
        return kfs_append(path, DEFAULT_APPEND_REPLICATION);
    }

    final public KfsOutputChannel kfs_append(String path, int numReplicas)
    {
        try {
            return kfs_append_ex(path, numReplicas, 0666);
        } catch (IOException ex) {
            return null;
        }
    }

    final public KfsOutputChannel kfs_append_ex(String path, int numReplicas,
            int mode) throws IOException
    {
        final int fd = open(cPtr, path, "a",
            numReplicas > 0 ?
                numReplicas : DEFAULT_APPEND_REPLICATION,
            0, 0, 0, KfsFileAttr.STRIPED_FILE_TYPE_NONE, mode);
        kfs_retToIOException(fd, path);
        KfsOutputChannel chan = null;
        try {
            final boolean append = true;
            chan = new KfsOutputChannel(this, fd, append);
        } finally {
            if (chan == null) {
                close(cPtr, fd);
            }
        }
        return chan;
    }

    final public KfsOutputChannel kfs_create(String path)
    {
        return kfs_create(path, DEFAULT_REPLICATION);
    }

    final public KfsOutputChannel kfs_create(String path, int numReplicas)
    {
        return kfs_create(path, numReplicas, false);
    }

    // if exclusive is specified, then create will succeed only if the
    // doesn't already exist
    final public KfsOutputChannel kfs_create(String path, int numReplicas,
            boolean exclusive)
    {
        return kfs_create(path, numReplicas, exclusive, -1, -1);
    }

    final  public KfsOutputChannel kfs_create(String path, int numReplicas,
            boolean exclusive, long bufferSize, long readAheadSize)
    {
        try {
            return kfs_create_ex(path, numReplicas, exclusive,
                bufferSize, readAheadSize, 0666);
        } catch (IOException ex) {
            return null;
        }
    }

    final public KfsOutputChannel kfs_create_ex(String path, int numReplicas,
            boolean exclusive, long bufferSize, long readAheadSize,
            int mode) throws IOException
    {
        final boolean forceStriperType = false;
        return kfs_create_ex(
            path,
            DEFAULT_REPLICATION, // numReplicas,
            exclusive,
            bufferSize,
            readAheadSize,
            DEFAULT_NUM_STRIPES,
            DEFAULT_NUM_RECOVERY_STRIPES,
            DEFAULT_STRIPE_SIZE,
            DEFAULT_STRIPER_TYPE,
            forceStriperType,
            mode
        );
    }

    final public void kfs_close(int fd) throws IOException
    {
        kfs_retToIOException(close(cPtr, fd));
    }

    final public KfsOutputChannel kfs_create_ex(String path, int numReplicas,
            boolean exclusive, long bufferSize, long readAheadSize,
            int numStripes, int numRecoveryStripes, int stripeSize,
            int stripedType, boolean forceType, int mode) throws IOException
    {
        int minSTier = 15;
        int maxSTier = 15;
        return kfs_create_ex(path, numReplicas, exclusive, bufferSize,
                readAheadSize, numStripes, numRecoveryStripes, stripeSize,
                stripedType, forceType, mode, minSTier, maxSTier);
    }

    final public KfsOutputChannel kfs_create_ex(String path, int numReplicas,
            boolean exclusive, long bufferSize, long readAheadSize,
            int numStripes, int numRecoveryStripes, int stripeSize,
            int stripedType, boolean forceType, int mode, int minSTier,
            int maxSTier) throws IOException
    {
        final int fd = create(cPtr, path, numReplicas, exclusive,
                numStripes, numRecoveryStripes, stripeSize, stripedType, forceType,
                mode, minSTier, maxSTier);
        kfs_retToIOException(fd, path);
        if (bufferSize >= 0) {
            setIoBufferSize(cPtr, fd, bufferSize);
        }
        if (readAheadSize >= 0) {
            setReadAheadSize(cPtr, fd, readAheadSize);
        }
        KfsOutputChannel chan = null;
        try {
            final boolean append = false;
            chan = new KfsOutputChannel(this, fd, append);
        } finally {
            if (chan == null) {
                close(cPtr, fd);
            }
        }
        return chan;
    }

    final public KfsOutputChannel kfs_create_ex(String path, boolean exclusive,
            String createParams, int mode,
            boolean forceTypeFlag) throws IOException
    {
        return kfs_create_ex_fd(create2ex(cPtr, path, exclusive, createParams,
            mode, forceTypeFlag), path);
    }

    final public KfsOutputChannel kfs_create_ex(String path, boolean exclusive,
            String createParams) throws IOException
    {
        return kfs_create_ex_fd(create2(cPtr, path, exclusive, createParams),
            path);
    }

    private KfsOutputChannel kfs_create_ex_fd(int fd,
            String path) throws IOException
    {
        kfs_retToIOException(fd, path);
        KfsOutputChannel chan = null;
        try {
            final boolean append = false;
            chan = new KfsOutputChannel(this, fd, append);
        } finally {
            if (chan == null) {
                close(cPtr, fd);
            }
        }
        return chan;
    }

    private int kfs_open_ro(String path)
    {
        return open(cPtr, path, "r",
            DEFAULT_REPLICATION,
            DEFAULT_NUM_STRIPES,
            DEFAULT_NUM_RECOVERY_STRIPES,
            DEFAULT_STRIPE_SIZE,
            DEFAULT_STRIPER_TYPE,
            0
        );
    }

    final public KfsInputChannel kfs_open(String path)
    {
        return kfs_open(path, -1, -1);
    }

    final  public KfsInputChannel kfs_open(String path, long bufferSize,
            long readAheadSize)
    {
        try {
            return kfs_open_ex(path, bufferSize, readAheadSize);
        } catch (IOException ex) {
            return null;
        }
    }

    final  public KfsInputChannel kfs_open_ex(String path, long bufferSize,
            long readAheadSize) throws IOException
    {
        final int fd = kfs_open_ro(path);
        kfs_retToIOException(fd, path);
        if (bufferSize >= 0) {
            setIoBufferSize(cPtr, fd, bufferSize);
        }
        if (readAheadSize >= 0) {
            setReadAheadSize(cPtr, fd, readAheadSize);
        }
        KfsInputChannel chan = null;
        try {
            chan = new KfsInputChannel(this, fd);
        } finally {
            if (chan == null) {
                close(cPtr, fd);
            }
        }
        return chan;
    }

    final public int kfs_remove(String path)
    {
        return remove(cPtr, path);
    }

    final public int kfs_rename(String oldpath, String newpath)
    {
        return rename(cPtr, oldpath, newpath, true);
    }

    // if overwrite is turned off, rename will succeed only if newpath
    // doesn't already exist
    final public int kfs_rename(String oldpath, String newpath,
            boolean overwrite)
    {
        return rename(cPtr, oldpath, newpath, overwrite);
    }

    final public int kfs_symlink(String target, String linkpath, int mode,
            boolean overwrite)
    {
        return symlink(cPtr, target, linkpath, mode, overwrite);
    }

    final public boolean kfs_exists(String path)
    {
        return exists(cPtr, path) == 1;
    }

    final public boolean kfs_isFile(String path)
    {
        return isFile(cPtr, path) == 1;
    }

    final public boolean kfs_isDirectory(String path)
    {
        return isDirectory(cPtr, path) == 1;
    }

    final public long kfs_filesize(String path)
    {
        return filesize(cPtr, path);
    }

    // Given a starting byte offset and a length, return the location(s)
    // of all the chunks that cover the region.
    final public String[][] kfs_getDataLocation(String path, long start,
            long len)
    {
        return getDataLocation(cPtr, path, start, len);
    }

    // Given a starting byte offset and a length, return the location(s)
    // of all "chunk blocks" that cover the region.
    // The first entry always contains "chunk block" size in hex notation with
    // leading 0 omitted, or if negative as status code, which can be converted
    // into exceptions with kfs_retToIOException()
    final public String[][] kfs_getBlocksLocation(String path, long start,
            long len)
    {
        final String[][] ret = getBlocksLocation(cPtr, path, start, len);
        if (ret == null) {
            throw new OutOfMemoryError();
        }
        if (ret.length < 1 || ret[0].length != 1) {
            throw new Error("getBlocksLocation internal error");
        }
        return ret;
    }

    // Return the degree of replication for this file
    final public short kfs_getReplication(String path)
    {
        return getReplication(cPtr, path);
    }

    // Request a change in the degree of replication for this file
    // Returns the value that was set by the server for this file
    final public short kfs_setReplication(String path, int numReplicas)
    {
        return setReplication(cPtr, path, numReplicas);
    }

    final public long kfs_getModificationTime(String path)
    {
        return getModificationTime(cPtr, path);
    }

    final public int kfs_setModificationTime(String path, long time)
    {
        return kfs_setUTimes(path, time * 1000,
            SET_TIME_TIME_NOT_VALID, SET_TIME_TIME_NOT_VALID);
    }

    final public int kfs_setUTimes(String path, long mtimeUsec,
        long atimeUsec, long ctimeUsec)
    {
        return setUTimes(cPtr, path, mtimeUsec, atimeUsec, ctimeUsec);
    }

    final public boolean kfs_compareChunkReplicas(
        String path, StringBuffer md5sum) throws IOException
    {
        final int ret = compareChunkReplicas(cPtr, path, md5sum);
        kfs_retToIOException(ret);
        return ret == 0;
    }

    final public long kfs_setDefaultIoBufferSize(long size)
    {
        return setDefaultIoBufferSize(cPtr, size);
    }

    final public long kfs_getDefaultIoBufferSize(long ptr)
    {
        return getDefaultIoBufferSize(cPtr);
    }

    final public long kfs_setDefaultReadAheadSize(long size)
    {
        return setDefaultReadAheadSize(cPtr, size);
    }

    final public long kfs_getDefaultReadAheadSize(long ptr)
    {
        return getDefaultReadAheadSize(cPtr);
    }

    final public long kfs_setIoBufferSize(int fd, long size)
    {
        return setIoBufferSize(cPtr, fd, size);
    }

    final public long kfs_getIoBufferSize(int fd)
    {
        return getIoBufferSize(cPtr, fd);
    }

    final public long kfs_setReadAheadSize(int fd, long size)
    {
        return setReadAheadSize(cPtr, fd, size);
    }

    final public long kfs_getReadAheadSize(int fd)
    {
        return getReadAheadSize(cPtr, fd);
    }

    final public void kfs_setFileAttributeRevalidateTime(int secs)
    {
        setFileAttributeRevalidateTime(cPtr, secs);
    }

    final public int kfs_chmod(String path, int mode)
    {
        return chmod(cPtr, path, mode);
    }

    final public int kfs_chmodr(String path, int mode)
    {
        return chmodr(cPtr, path, mode);
    }

    final public int kfs_chmod(int fd, int mode)
    {
        return fchmod(cPtr, fd, mode);
    }

    final public int kfs_chown(String path, String user, String group)
    {
        return chowns(cPtr, path, user, group);
    }

    final public int kfs_chownr(String path, String user, String group)
    {
        return chownsr(cPtr, path, user, group);
    }

    final public int kfs_chown(String path, long user, long group)
    {
        return chown(cPtr, path, user, group);
    }

    final public int kfs_chownr(String path, long user, long group)
    {
        return chownr(cPtr, path, user, group);
    }

    final public int kfs_chown(int fd, String user, String group)
    {
        return fchowns(cPtr, fd, user, group);
    }

    final public int kfs_chown(int fd, long user, long group)
    {
        return fchown(cPtr, fd, user, group);
    }

    final public int kfs_setEUserAndEGroup(long user, long group, long[] groups)
    {
        return setEUserAndEGroup(cPtr, user, group, groups);
    }

    final public int kfs_stat(String path, KfsFileAttr attr)
    {
        return stat(cPtr, path, attr);
    }

    final public int kfs_lstat(String path, KfsFileAttr attr)
    {
        return lstat(cPtr, path, attr);
    }

    final public void kfs_retToIOException(int ret) throws IOException
    {
        kfs_retToIOException(ret, null);
    }

    final public void kfs_retToIOException(int ret,
            String path) throws IOException
    {
        if (ret >= 0) {
            return;
        }
        final String es = strerror(cPtr, ret);
        if (es == null) {
            throw new OutOfMemoryError();
        }
        final String msg = path == null ? es : path + ": " + es;
        if (isnotfound(cPtr, ret)) {
            throw new FileNotFoundException(msg);
        }
        throw new IOException(msg);
    }

    final public long kfs_seek(int fd, long offset) throws IOException
    {
        final long ret = seek(cPtr, fd, offset);
        if (ret < 0) {
            kfs_retToIOException((int)ret);
        }
        return ret;
    }

    final public long kfs_tell(int fd) throws IOException
    {
        final long ret = tell(cPtr, fd);
        if (ret < 0) {
            kfs_retToIOException((int)ret);
        }
        return ret;
    }

    final public int kfs_setUMask(int umask) throws IOException
    {
        final int ret = setUMask(cPtr, umask);
        if (ret < 0) {
            kfs_retToIOException((int)ret);
        }
        return ret;
    }

    final public int kfs_getUMask() throws IOException
    {
        final int ret = getUMask(cPtr);
        if (ret < 0) {
            kfs_retToIOException((int)ret);
        }
        return ret;
    }

    final public KfsDelegation kfs_createDelegationToken(
        boolean allowDelegationFlag, long validTime) throws IOException
    {
        final KfsDelegation result = new KfsDelegation();
        final String error =
            createDelegationToken(cPtr, allowDelegationFlag, validTime, result);
        if (error != null) {
            throw new IOException(error);
        }
        if (result.key == null || result.token == null) {
            throw new OutOfMemoryError();
        }
        return result;
    }

    final public void kfs_renewDelegationToken(
        KfsDelegation token) throws IOException
    {
        final String error = renewDelegationToken(cPtr, token);
        if (error != null) {
            throw new IOException(error);
        }
        if (token.key == null || token.token == null) {
            throw new OutOfMemoryError();
        }
    }

    final public void kfs_cancelDelegationToken(
        KfsDelegation token) throws IOException
    {
        final String error = cancelDelegationToken(cPtr, token);
        if (error != null) {
            throw new IOException(error);
        }
    }

    final public Map<String, String> kfs_getStats() throws IOException
    {
        final String[] stats = getStats(cPtr);
        if (stats == null) {
            throw new IOException("internal error: null stats array");
        }
        if (stats.length % 2 != 0) {
            throw new IOException(
                "internal error: invalid stats array size: " + stats.length);
        }
        final Map<String, String> ret = new TreeMap<String, String>();
        for (int i = 0; i < stats.length; i += 2) {
            ret.put(stats[i], stats[i+1]);
        }
        return ret;
    }

    final protected void kfs_destroy() {
        if (cPtr != 0) {
            final long ptr = cPtr;
            cPtr = 0;
            destroy(ptr);
        }
    }

    final long getCPtr()
    {
        return cPtr;
    }
}
