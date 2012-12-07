/**
 * $Id$
 *
 * Created 2007/08/24
 * @author: Sriram Rao (Kosmix Corp.)
 *
 * Copyright 2008-2012 Quantcast Corp.
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
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.CoderResult;
import java.util.Properties;
import java.util.ArrayList;

final public class KfsAccess
{
    public final int DEFAULT_APPEND_REPLICATION   = 2;
    public final int DEFAULT_REPLICATION          = 1;
    public final int DEFAULT_NUM_STRIPES          = 6;
    public final int DEFAULT_NUM_RECOVERY_STRIPES = 3;
    public final int DEFAULT_STRIPE_SIZE          = 65536;
    public final int DEFAULT_STRIPER_TYPE         =
        KfsFileAttr.STRIPED_FILE_TYPE_RS;

    // the pointer in C++
    private long cPtr;

    private final static native
    long initF(String configFn);

    private final static native
    long initS(String metaServerHost, int metaServerPort);

    private final static native
    void destroy(long ptr);

    private final static native
    int cd(long ptr, String  path);

    private final static native
    int mkdirs(long ptr, String  path, int mode);

    private final static native
    int rmdir(long ptr, String  path);

    private final static native
    int rmdirs(long ptr, String  path);

    private final static native
    String[] readdir(long ptr, String path, boolean prefetchAttr);

    private final static native
    String[][] getDataLocation(long ptr, String path, long start, long len);

    private final static native
    short getReplication(long ptr, String path);

    private final static native
    short setReplication(long ptr, String path, int numReplicas);

    private final static native
    long getModificationTime(long ptr, String path);

    private final static native
    int create(long ptr, String path, int numReplicas, boolean exclusive,
        int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
        boolean forceType, int mode);

    private final static native
    int remove(long ptr, String path);

    private final static native
    int rename(long ptr, String oldpath, String newpath, boolean overwrite);

    private final static native
    int open(long ptr, String path, String mode, int numReplicas,
        int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
        int createMode);

    private final static native
    int exists(long ptr, String path);

    private final static native
    int isFile(long ptr, String path);

    private final static native
    int isDirectory(long ptr, String path);

    private final static native
    long filesize(long ptr, String path);

    private final static native
    long setDefaultIoBufferSize(long ptr, long size);

    private final static native
    long getDefaultIoBufferSize(long ptr);

    private final static native
    long setDefaultReadAheadSize(long ptr, long size);

    private final static native
    long getDefaultReadAheadSize(long ptr);

    private final static native
    long setIoBufferSize(long ptr, int fd, long size);

    private final static native
    long getIoBufferSize(long ptr, int fd);

    private final static native
    long setReadAheadSize(long ptr, int fd, long size);

    private final static native
    long getReadAheadSize(long ptr, int fd);

    private final static native
    int setModificationTime(long ptr, String path, long time);

    private final static native
    int compareChunkReplicas(long ptr, String path, StringBuffer md5sum);

    private final static native
    int getStripedType(long ptr, String path);

    private final static native
    void setFileAttributeRevalidateTime(long ptr, int secs);

    private final static native
    int chmod(long ptr, String path, int mode);

    private final static native
    int chmodr(long ptr, String path, int mode);

    private final static native
    int fchmod(long ptr, int fd, int mode);

    private final static native
    int chowns(long ptr, String path, String user, String group);

    private final static native
    int chownsr(long ptr, String path, String user, String group);

    private final static native
    int chown(long ptr, String path, long user, long group);

    private final static native
    int chownr(long ptr, String path, long user, long group);

    private final static native
    int fchowns(long ptr, int fd, String user, String group);

    private final static native
    int fchown(long ptr, int fd, long user, long group);

    private final static native
    int setEUserAndEGroup(long ptr, long user, long group, long[] groups);

    private final static native
    int stat(long ptr, String path, KfsFileAttr attr);

    private final static native
    String strerror(long ptr, int err);

    private final static native
    boolean isnotfound(long ptr, int err);

    private final static native
    int close(long ptr, int fd);

    private final static native
    long seek(long ptr, int fd, long offset);

    private final static native
    long tell(long ptr, int fd);

    private final static native
    int setUMask(long ptr, int umask);

    private final static native
    int getUMask(long ptr);

    static {
        try {
            System.loadLibrary("qfs_access");
        } catch (UnsatisfiedLinkError e) {
            e.printStackTrace();
            System.err.println("Unable to load qfs_access native library");
            System.exit(1);
        }
    }

    public KfsAccess(String configFn) throws IOException
    {
        cPtr = initF(configFn);
        if (cPtr == 0) {
            throw new IOException("Unable to initialize KFS Client");
        }
    }

    public KfsAccess(String metaServerHost, int metaServerPort) throws IOException
    {
        cPtr = initS(metaServerHost, metaServerPort);
        if (cPtr == 0) {
            throw new IOException("Unable to initialize KFS Client");
        }
    }

    // most calls wrap to a call on the KfsClient.  For return values,
    // see the comments in libkfsClient/KfsClient.h
    //
    public int kfs_cd(String path)
    {
        return cd(cPtr, path);
    }

    // make the directory hierarchy for path
    public int kfs_mkdirs(String path)
    {
        return mkdirs(cPtr, path, 0777);
    }

    // make the directory hierarchy for path
    public int kfs_mkdirs(String path, int mode)
    {
        return mkdirs(cPtr, path, mode);
    }

    // remove the directory specified by path; remove will succeed only if path is empty.
    public int kfs_rmdir(String path)
    {
        return rmdir(cPtr, path);
    }

    // remove the directory tree specified by path; remove will succeed only if path is empty.
    public int kfs_rmdirs(String path)
    {
        return rmdirs(cPtr, path);
    }

    public String[] kfs_readdir(String path)
    {
        return kfs_readdir(path, false);
    }

    public String[] kfs_readdir(String path, boolean prefetchAttr)
    {
        return readdir(cPtr, path, prefetchAttr);
    }

    final public class DirectoryIterator
    {
        public long    modificationTime;
        public long    filesize;
        public int     replication;
        public boolean isDirectory;
        public int     numStripes;
        public int     numRecoveryStripes;
        public int     striperType;
        public int     stripeSize;
        public String  filename;
        public long    owner;
        public long    group;
        public int     mode;
        public String  ownerName;
        public String  groupName;
        public long    dirCount;
        public long    fileCount;
        public long    fileId;

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
                input = new KfsInputChannel(KfsAccess.this, fd);
            } finally {
                if (input == null) {
                    KfsAccess.this.close(cPtr, fd);
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
                final int onameLen = buf.getInt();
                final int gnameLen = buf.getInt();
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

    public KfsFileAttr[] kfs_readdirplus(String path)
    {
        DirectoryIterator itr = null;
        try {
            itr = new DirectoryIterator(path);
            final ArrayList<KfsFileAttr> ret = new ArrayList<KfsFileAttr>();
            while (itr.next()) {
                KfsFileAttr entry = new KfsFileAttr();
                entry.modificationTime   = itr.modificationTime;
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

    public KfsOutputChannel kfs_append(String path)
    {
        return kfs_append(path, DEFAULT_APPEND_REPLICATION);
    }

    public KfsOutputChannel kfs_append(String path, int numReplicas)
    {
        try {
            return kfs_append_ex(path, numReplicas, 0666);
        } catch (IOException ex) {
            return null;
        }
    }

    public KfsOutputChannel kfs_append_ex(String path, int numReplicas, int mode) throws IOException
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

    public KfsOutputChannel kfs_create(String path)
    {
        return kfs_create(path, DEFAULT_REPLICATION);
    }

    public KfsOutputChannel kfs_create(String path, int numReplicas)
    {
        return kfs_create(path, numReplicas, false);
    }

    // if exclusive is specified, then create will succeed only if the
    // doesn't already exist
    public KfsOutputChannel kfs_create(String path, int numReplicas, boolean exclusive)
    {
        return kfs_create(path, numReplicas, exclusive, -1, -1);
    }

    public KfsOutputChannel kfs_create(String path, int numReplicas, boolean exclusive,
        long bufferSize, long readAheadSize)
    {
        try {
            return kfs_create_ex(path, numReplicas, exclusive,
                bufferSize, readAheadSize, 0666);
        } catch (IOException ex) {
            return null;
        }
    }

    public KfsOutputChannel kfs_create_ex(String path, int numReplicas, boolean exclusive,
        long bufferSize, long readAheadSize, int mode) throws IOException
    {
        final boolean forceStriperType = false;
        KfsOutputChannel chan =  kfs_create_ex(
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
        if (getStripedType(cPtr, path) != DEFAULT_STRIPER_TYPE) {
            final short r = setReplication(cPtr, path, numReplicas);
            if (r < 0) {
                try {
                    chan.close();
                } catch (IOException ignored) {
                }
                kfs_retToIOException(r, path);
            }
        }
        return chan;
    }

    public void kfs_close(int fd) throws IOException
    {
        kfs_retToIOException(close(cPtr, fd));
    }

    public KfsOutputChannel kfs_create_ex(String path, int numReplicas, boolean exclusive,
            long bufferSize, long readAheadSize,
            int numStripes, int numRecoveryStripes, int stripeSize, int stripedType,
            boolean forceType, int mode) throws IOException
    {
        final int fd = create(cPtr, path, numReplicas, exclusive,
                numStripes, numRecoveryStripes, stripeSize, stripedType, forceType, mode);
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

    private final int kfs_open_ro(String path)
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

    public KfsInputChannel kfs_open(String path)
    {
        return kfs_open(path, -1, -1);
    }

    public KfsInputChannel kfs_open(String path, long bufferSize, long readAheadSize)
    {
        try {
            return kfs_open_ex(path, bufferSize, readAheadSize);
        } catch (IOException ex) {
            return null;
        }
    }

    public KfsInputChannel kfs_open_ex(String path, long bufferSize, long readAheadSize) throws IOException
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

    public int kfs_remove(String path)
    {
        return remove(cPtr, path);
    }

    public int kfs_rename(String oldpath, String newpath)
    {
        return rename(cPtr, oldpath, newpath, true);
    }

    // if overwrite is turned off, rename will succeed only if newpath
    // doesn't already exist
    public int kfs_rename(String oldpath, String newpath, boolean overwrite)
    {
        return rename(cPtr, oldpath, newpath, overwrite);
    }

    public boolean kfs_exists(String path)
    {
        return exists(cPtr, path) == 1;
    }

    public boolean kfs_isFile(String path)
    {
        return isFile(cPtr, path) == 1;
    }

    public boolean kfs_isDirectory(String path)
    {
        return isDirectory(cPtr, path) == 1;
    }

    public long kfs_filesize(String path)
    {
        return filesize(cPtr, path);
    }

    // Given a starting byte offset and a length, return the location(s)
    // of all the chunks that cover the region.
    public String[][] kfs_getDataLocation(String path, long start, long len)
    {
        return getDataLocation(cPtr, path, start, len);
    }

    // Return the degree of replication for this file
    public short kfs_getReplication(String path)
    {
        return getReplication(cPtr, path);
    }

    // Request a change in the degree of replication for this file
    // Returns the value that was set by the server for this file
    public short kfs_setReplication(String path, int numReplicas)
    {
        return setReplication(cPtr, path, numReplicas);
    }

    public long kfs_getModificationTime(String path)
    {
        return getModificationTime(cPtr, path);
    }

    public int kfs_setModificationTime(String path, long time)
    {
        return setModificationTime(cPtr, path, time);
    }

    public boolean kfs_compareChunkReplicas(
        String path, StringBuffer md5sum) throws IOException
    {
        final int ret = compareChunkReplicas(cPtr, path, md5sum);
        kfs_retToIOException(ret);
        return ret == 0;
    }

    public long kfs_setDefaultIoBufferSize(long size)
    {
        return setDefaultIoBufferSize(cPtr, size);
    }

    public long kfs_getDefaultIoBufferSize(long ptr)
    {
        return getDefaultIoBufferSize(cPtr);
    }

    public long kfs_setDefaultReadAheadSize(long size)
    {
        return setDefaultReadAheadSize(cPtr, size);
    }

    public long kfs_getDefaultReadAheadSize(long ptr)
    {
        return getDefaultReadAheadSize(cPtr);
    }

    public long kfs_setIoBufferSize(int fd, long size)
    {
        return setIoBufferSize(cPtr, fd, size);
    }

    public long kfs_getIoBufferSize(int fd)
    {
        return getIoBufferSize(cPtr, fd);
    }

    public long kfs_setReadAheadSize(int fd, long size)
    {
        return setReadAheadSize(cPtr, fd, size);
    }

    public long kfs_getReadAheadSize(int fd)
    {
        return getReadAheadSize(cPtr, fd);
    }

    public void kfs_setFileAttributeRevalidateTime(int secs)
    {
        setFileAttributeRevalidateTime(cPtr, secs);
    }

    public int kfs_chmod(String path, int mode)
    {
        return chmod(cPtr, path, mode);
    }

    public int kfs_chmodr(String path, int mode)
    {
        return chmodr(cPtr, path, mode);
    }

    public int kfs_chmod(int fd, int mode)
    {
        return fchmod(cPtr, fd, mode);
    }

    public int kfs_chown(String path, String user, String group)
    {
        return chowns(cPtr, path, user, group);
    }
    
    public int kfs_chownr(String path, String user, String group)
    {
        return chownsr(cPtr, path, user, group);
    }

    public int kfs_chown(String path, long user, long group)
    {
        return chown(cPtr, path, user, group);
    }

    public int kfs_chownr(String path, long user, long group)
    {
        return chownr(cPtr, path, user, group);
    }

    public int kfs_chown(int fd, String user, String group)
    {
        return fchowns(cPtr, fd, user, group);
    }

    public int kfs_chown(int fd, long user, long group)
    {
        return fchown(cPtr, fd, user, group);
    }

    public int kfs_setEUserAndEGroup(long user, long group, long[] groups)
    {
        return setEUserAndEGroup(cPtr, user, group, groups);
    }

    public int kfs_stat(String path, KfsFileAttr attr)
    {
        return stat(cPtr, path, attr);
    }

    public void kfs_retToIOException(int ret) throws IOException
    {
        kfs_retToIOException(ret, null);
    }

    public void kfs_retToIOException(int ret, String path) throws IOException
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

    public long kfs_seek(int fd, long offset) throws IOException
    {
        final long ret = seek(cPtr, fd, offset);
        if (ret < 0) {
            kfs_retToIOException((int)ret);
        }
        return ret;
    }

    public long kfs_tell(int fd) throws IOException
    {
        final long ret = tell(cPtr, fd);
        if (ret < 0) {
            kfs_retToIOException((int)ret);
        }
        return ret;
    }

    public int kfs_setUMask(int umask) throws IOException
    {
        final int ret = setUMask(cPtr, umask);
        if (ret < 0) {
            kfs_retToIOException((int)ret);
        }
        return ret;
    }

    public int kfs_getUMask() throws IOException
    {
        final int ret = getUMask(cPtr);
        if (ret < 0) {
            kfs_retToIOException((int)ret);
        }
        return ret;
    }

    protected void finalize() throws Throwable
    {
        try {
            if (cPtr != 0) {
                final long ptr = cPtr;
                cPtr = 0;
                destroy(ptr);
            }
        } finally {
            super.finalize();
        }
    }

    long getCPtr()
    {
        return cPtr;
    }
}


