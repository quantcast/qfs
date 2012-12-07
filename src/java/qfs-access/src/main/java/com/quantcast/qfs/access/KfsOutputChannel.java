/**
 * $Id$
 *
 * Created 2007/09/11
 *
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
 * \brief An output channel that does buffered I/O.  This is to reduce
 * the overhead of JNI calls.
 */

package com.quantcast.qfs.access;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class KfsOutputChannel implements WritableByteChannel, Positionable
{
    // To get to a byte-buffer from the C++ side as a pointer, need
    // the buffer to be direct memory backed buffer.  So, allocate one
    // for reading/writing.
    private ByteBuffer writeBuffer;
    private int kfsFd = -1;
    private KfsAccess kfsAccess;
    private final boolean append;
    private boolean returnBufferToPool;

    private final static native
    int write(long ptr, int fd, ByteBuffer buf, int begin, int end);

    private final static native
    int atomicRecordAppend(long ptr, int fd, ByteBuffer buf, int begin, int end);

    private final static native
    int sync(long ptr, int fd);

    KfsOutputChannel(KfsAccess kfsAccess, int fd, boolean append) 
    {
        this.writeBuffer = BufferPool.getInstance().getBuffer();
        this.returnBufferToPool = true;
        this.writeBuffer.clear();
        this.append = append;
        this.kfsFd = fd;
        this.kfsAccess = kfsAccess;
    }

    public synchronized boolean isOpen()
    {
        return kfsFd >= 0;

    }

    // Read/write from the specified fd.  The basic model is:
    // -- fill some data into a direct mapped byte buffer
    // -- send/receive to the other side (Jave->C++ or vice-versa)
    //
    public synchronized int write(ByteBuffer src) throws IOException
    {
        if (kfsFd < 0) {
            throw new IOException("File closed");
        }
        final int r0 = src.remaining();
        // While the src buffer has data, copy it in and flush
        while (src.hasRemaining()) {
            if (writeBuffer.remaining() < (append ? r0 : 1)) {
                syncSelf();
            }
            if (append) {
                final int spcAvail = writeBuffer.remaining();
                if (r0 > spcAvail) {
                    final int maxAppendSize = 64 << 10;
                    if (maxAppendSize < r0) {
                        throw new IOException(
                            r0 + " exceeds KFS append size limit of " +
                            maxAppendSize
                        );
                    }
                    final ByteBuffer buf = ByteBuffer.allocateDirect(
                        (r0 + BufferPool.BUFFER_SIZE - 1) /
                        BufferPool.BUFFER_SIZE * BufferPool.BUFFER_SIZE
                    );
                    releaseBuffer();
                    writeBuffer = buf;
                }
            }
            // Save end of input buffer
            final int lim = src.limit();
            // Copy in as much data we have space
            if (writeBuffer.remaining() < src.remaining()) {
                if (append) {
                        throw new IOException("KFS internal append error" +
                            " buffer space is not sufficient");
                }
                src.limit(src.position() + writeBuffer.remaining());
            }
            writeBuffer.put(src);
            // restore the limit to what it was
            src.limit(lim);
        }
        return r0 - src.remaining();
    }

    private void writeDirect(ByteBuffer buf) throws IOException
    {
        if (! buf.isDirect()) {
            throw new IllegalArgumentException("need direct buffer");
        }
        final int pos  = buf.position();
        final int last = buf.limit();
        if (pos < last) {
            final int sz = append ?
                atomicRecordAppend(
                    kfsAccess.getCPtr(), kfsFd, writeBuffer, pos, last) :
                write(
                    kfsAccess.getCPtr(), kfsFd, buf, pos, last);
            kfsAccess.kfs_retToIOException(sz);
            if (pos + sz != last) {
                throw new RuntimeException("KFS internal error:" +
                    (append ? "append" : "write") + "(" +
                    (last - pos) + ") != " + sz);
            }
        }
        buf.clear();
    }

    /** @deprecated Use write() instead */ @Deprecated
    public int atomicRecordAppend(ByteBuffer src) throws IOException
    {
        return write(src);
    }

    public synchronized int sync() throws IOException
    {
        if (kfsFd < 0) {
            throw new IOException("File closed");
        }
        if (append) {
            syncSelf();
        }
        return 0;
    }

    private synchronized void syncSelf() throws IOException
    {
        // flush everything
        writeBuffer.flip();
        boolean restore = true;
        try {
            writeDirect(writeBuffer);
            restore = false;
        } finally {
            if (restore) {
                writeBuffer.flip();
            }
        }
    }

    // is modeled after the seek of Java's RandomAccessFile; offset is
    // the offset from the beginning of the file.
    public synchronized long seek(long offset) throws IOException
    {
        if (kfsFd < 0) {
            throw new IOException("File closed");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("seek(" + kfsFd + ", " + offset + ")");
        }
        syncSelf();
        return kfsAccess.kfs_seek(kfsFd, offset);
    }

    public synchronized long tell() throws IOException
    {
        if (kfsFd < 0) {
            throw new IOException("File closed");
        }
        // similar issue as read: the position at which we are writing
        // needs to be offset by where the C++ code thinks we are and
        // how much we have buffered
        return kfsAccess.kfs_tell(kfsFd) + writeBuffer.remaining();
    }

    public synchronized void close() throws IOException
    {
        if (kfsFd < 0) {
            throw new IOException("File closed");
        }
        IOException origEx = null;
        try {
            syncSelf();
        } catch (IOException ex) {
            origEx = ex;
        } finally {
            final int fd = kfsFd;
            kfsFd = -1;
            KfsAccess ka = kfsAccess;
            kfsAccess = null;
            try {
                ka.kfs_close(fd);
            } finally {
                releaseBuffer();
                if (origEx != null) {
                    throw origEx;
                }
            }
        }
    }

    private void releaseBuffer()
    {
        if (returnBufferToPool) {
            BufferPool.getInstance().releaseBuffer(writeBuffer);
        }
        writeBuffer        = null;
        returnBufferToPool = false;
    }

    protected void finalize() throws Throwable
    {
        try {
            if (kfsFd >= 0 && kfsAccess != null) {
                final int fd = kfsFd;
                kfsFd = -1;
                KfsAccess ka = kfsAccess;
                kfsAccess = null;
                ka.kfs_close(fd);
            }
        } finally {
            super.finalize();
        }
    }
    
}
