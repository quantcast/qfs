/**
 * $Id$
 *
 * Created 2007/09/11
 *
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
 * \brief An output channel that does buffered I/O.  This is to reduce
 * the overhead of JNI calls.
 */
package com.quantcast.qfs.access;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

class KfsOutputChannelBase implements WritableByteChannel, Positionable {

    // To get to a byte-buffer from the C++ side as a pointer, need
    // the buffer to be direct memory backed buffer.  So, allocate one
    // for reading/writing.
    protected final static class State implements Runnable {

        ByteBuffer writeBuffer;
        boolean returnBufferToPool;
        int kfsFd;
        KfsAccessBase kfsAccess;

        State(int fd, KfsAccessBase ka) {
            writeBuffer = BufferPool.getInstance().getBuffer();
            returnBufferToPool = true;
            writeBuffer.clear();
            kfsFd = fd;
            kfsAccess = ka;
        }

        void releaseBuffer() {
            if (returnBufferToPool) {
                BufferPool.getInstance().releaseBuffer(writeBuffer);
            }
            writeBuffer = null;
            returnBufferToPool = false;
        }

        void release() throws IOException {
            if (kfsFd >= 0 && kfsAccess != null) {
                final int fd = kfsFd;
                kfsFd = -1;
                KfsAccessBase ka = kfsAccess;
                kfsAccess = null;
                try {
                    ka.kfs_close(fd);
                } finally {
                    releaseBuffer();
                }
            }
        }

        @Override
        public void run() {
            try {
                release();
            } catch (IOException ignored) {
                // Ignore
            }
        }
    }
    final protected State state;
    final private boolean append;

    private static native int write(
            long ptr, int fd, ByteBuffer buf, int begin, int end);

    private static native int atomicRecordAppend(
            long ptr, int fd, ByteBuffer buf, int begin, int end);

    // private static native
    // int sync(long ptr, int fd);
    KfsOutputChannelBase(KfsAccessBase kfsAccess, int fd, boolean append) {
        this.append = append;
        this.state = new State(fd, kfsAccess);
    }

    final public synchronized boolean isOpen() {
        return state.kfsFd >= 0;
    }

    // Read/write from the specified fd.  The basic model is:
    // -- fill some data into a direct mapped byte buffer
    // -- send/receive to the other side (Jave->C++ or vice-versa)
    //
    final public synchronized int write(ByteBuffer src) throws IOException {
        if (state.kfsFd < 0) {
            throw new IOException("File closed");
        }
        final int r0 = src.remaining();
        // While the src buffer has data, copy it in and flush
        while (src.hasRemaining()) {
            if (state.writeBuffer.remaining() < (append ? r0 : 1)) {
                syncSelf();
            }
            if (append) {
                final int spcAvail = state.writeBuffer.remaining();
                if (r0 > spcAvail) {
                    final int maxAppendSize = 64 << 10;
                    if (maxAppendSize < r0) {
                        throw new IOException(
                                r0 + " exceeds KFS append size limit of "
                                + maxAppendSize
                        );
                    }
                    final ByteBuffer buf = ByteBuffer.allocateDirect(
                            (r0 + BufferPool.BUFFER_SIZE - 1)
                            / BufferPool.BUFFER_SIZE * BufferPool.BUFFER_SIZE
                    );
                    releaseBuffer();
                    state.writeBuffer = buf;
                }
            }
            // Save end of input buffer
            final int lim = src.limit();
            // Copy in as much data we have space
            if (state.writeBuffer.remaining() < src.remaining()) {
                if (append) {
                    throw new IOException("KFS internal append error"
                            + " buffer space is not sufficient");
                }
                src.limit(src.position() + state.writeBuffer.remaining());
            }
            state.writeBuffer.put(src);
            // restore the limit to what it was
            src.limit(lim);
        }
        return r0 - src.remaining();
    }

    private void writeDirect(ByteBuffer buf) throws IOException {
        if (!buf.isDirect()) {
            throw new IllegalArgumentException("need direct buffer");
        }
        final int pos = buf.position();
        final int last = buf.limit();
        if (pos < last) {
            final int sz = append
                    ? atomicRecordAppend(
                            state.kfsAccess.getCPtr(),
                            state.kfsFd, state.writeBuffer, pos, last)
                    : write(
                            state.kfsAccess.getCPtr(),
                            state.kfsFd, buf, pos, last);
            state.kfsAccess.kfs_retToIOException(sz);
            if (pos + sz != last) {
                throw new RuntimeException("KFS internal error:"
                        + (append ? "append" : "write") + "("
                        + (last - pos) + ") != " + sz);
            }
        }
        buf.clear();
    }

    /**
     * @deprecated Use write() instead
     */
    @Deprecated
    final public int atomicRecordAppend(ByteBuffer src) throws IOException {
        return write(src);
    }

    final public synchronized int sync() throws IOException {
        if (state.kfsFd < 0) {
            throw new IOException("File closed");
        }
        if (append) {
            syncSelf();
        }
        return 0;
    }

    private synchronized void syncSelf() throws IOException {
        // flush everything
        state.writeBuffer.flip();
        boolean restore = true;
        try {
            writeDirect(state.writeBuffer);
            restore = false;
        } finally {
            if (restore) {
                state.writeBuffer.flip();
            }
        }
    }

    // is modeled after the seek of Java's RandomAccessFile; offset is
    // the offset from the beginning of the file.
    final public synchronized long seek(long offset) throws IOException {
        if (state.kfsFd < 0) {
            throw new IOException("File closed");
        }
        if (offset < 0) {
            throw new IllegalArgumentException(
                    "seek(" + state.kfsFd + ", " + offset + ")");
        }
        syncSelf();
        return state.kfsAccess.kfs_seek(state.kfsFd, offset);
    }

    final public synchronized long tell() throws IOException {
        if (state.kfsFd < 0) {
            throw new IOException("File closed");
        }
        // similar issue as read: the position at which we are writing
        // needs to be offset by where the C++ code thinks we are and
        // how much we have buffered
        return state.kfsAccess.kfs_tell(
                state.kfsFd) + state.writeBuffer.remaining();
    }

    public synchronized void close() throws IOException {
        if (state.kfsFd < 0) {
            return;
        }
        IOException origEx = null;
        try {
            syncSelf();
        } catch (IOException ex) {
            origEx = ex;
        } finally {
            try {
                state.release();
            } catch (IOException ex) {
                if (origEx == null) {
                    origEx = ex;
                }
            }
        }
        if (origEx != null) {
            throw origEx;
        }
    }

    private void releaseBuffer() {
        state.releaseBuffer();
    }

    final public void setIoBufferSize(long bufferSize) {
        if (bufferSize >= 0) {
            state.kfsAccess.kfs_setIoBufferSize(state.kfsFd, bufferSize);
        }
    }
}
