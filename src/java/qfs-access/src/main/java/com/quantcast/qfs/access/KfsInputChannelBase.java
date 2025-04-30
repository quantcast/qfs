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
 * \brief An input channel that does buffered I/O.  This is to reduce
 * the overhead of JNI calls.
 */
package com.quantcast.qfs.access;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/* A byte channel interface with seek support */
class KfsInputChannelBase implements ReadableByteChannel, Positionable {

    // To get to a byte-buffer from the C++ side as a pointer, need
    // the buffer to be direct memory backed buffer.  So, allocate one
    // for reading/writing.
    final protected static class State implements Runnable {

        ByteBuffer readBuffer;
        int kfsFd;
        KfsAccessBase kfsAccess;

        State(KfsAccessBase ka, int fd) {
            readBuffer = BufferPool.getInstance().getBuffer();
            readBuffer.flip();
            kfsFd = fd;
            kfsAccess = ka;
        }

        void releaseBuffer() {
            if (readBuffer != null) {
                BufferPool.getInstance().releaseBuffer(readBuffer);
                readBuffer = null;
            }
        }

        void release() {
            if (kfsFd >= 0 && kfsAccess != null) {
                final int fd = kfsFd;
                kfsFd = -1;
                final KfsAccessBase ka = kfsAccess;
                kfsAccess = null;
                try {
                    ka.kfs_close(fd);
                } catch (IOException ignored) {
                    // Ignore
                } finally {
                    releaseBuffer();
                }
            }
        }

        @Override
        public void run() {
            release();
        }
    }
    protected final State state;
    private boolean isReadAheadOff = false;

    private static native int read(long cPtr, int fd, ByteBuffer buf, int begin, int end);

    protected KfsInputChannelBase(KfsAccessBase ka, int fd) {
        state = new State(ka, fd);
    }

    final public synchronized boolean isOpen() {
        return state.kfsFd >= 0;
    }

    // Read/write from the specified fd.  The basic model is:
    // -- fill some data into a direct mapped byte buffer
    // -- send/receive to the other side (Jave->C++ or vice-versa)
    //
    final public synchronized int read(ByteBuffer dst) throws IOException {
        if (state.kfsFd < 0) {
            throw new IOException("File closed");
        }
        final int r0 = dst.remaining();

        // While the dst buffer has space for more data, fill
        while (dst.hasRemaining()) {
            // Fill input buffer if it's empty
            if (!state.readBuffer.hasRemaining()) {
                state.readBuffer.clear();
                readDirect(state.readBuffer, dst.remaining());
                state.readBuffer.flip();

                // If we failed to get anything, call that EOF
                if (!state.readBuffer.hasRemaining()) {
                    break;
                }
            }

            // Save end of input buffer
            final int lim = state.readBuffer.limit();

            // If dst buffer can't contain all of input buffer, limit
            // our copy size.
            if (dst.remaining() < state.readBuffer.remaining()) {
                state.readBuffer.limit(state.readBuffer.position() + dst.remaining());
            }
            // Copy into dst buffer
            dst.put(state.readBuffer);

            // Restore end of input buffer marker (maybe changed
            // earlier)
            state.readBuffer.limit(lim);
        }

        // If we copied anything into the dst buffer (or if there was
        // no space available to do so), return the number of bytes
        // copied.  Otherwise return -1 to indicate EOF.
        final int r1 = dst.remaining();
        if (r1 < r0 || r0 == 0) {
            return r0 - r1;
        }
        return -1;
    }

    final ByteBuffer readNext() throws IOException {
        state.readBuffer.clear();
        readDirect(state.readBuffer, 0);
        state.readBuffer.flip();
        return state.readBuffer;
    }

    private void readDirect(ByteBuffer buf, int remRequestedBytes) throws IOException {
        if (!buf.isDirect()) {
            throw new IllegalArgumentException("need direct buffer");
        }
        final int pos = buf.position();
        final int end = (isReadAheadOff && remRequestedBytes > 0)
                ? Math.min(buf.limit(), pos + remRequestedBytes) : buf.limit();
        final int sz = read(state.kfsAccess.getCPtr(), state.kfsFd, buf, pos, end);
        state.kfsAccess.kfs_retToIOException(sz);
        buf.position(pos + sz);
    }

    // is modeled after the seek of Java's RandomAccessFile; offset is
    // the offset from the beginning of the file.
    final public synchronized long seek(long offset) throws IOException {
        if (offset < 0) {
            throw new IllegalArgumentException("seek(" + state.kfsFd + "," + offset + ")");
        }
        if (state.kfsFd < 0) {
            throw new IOException("File closed");
        }
        state.readBuffer.clear();
        state.readBuffer.flip();
        return state.kfsAccess.kfs_seek(state.kfsFd, offset);
    }

    public synchronized long tell() throws IOException {
        if (state.kfsFd < 0) {
            throw new IOException("File closed");
        }
        // we keep some data buffered; so, we ask the C++ side where
        // we are in the file and offset that by the amount in our
        // buffer
        final long ret = state.kfsAccess.kfs_tell(state.kfsFd);
        final int rem = state.readBuffer.remaining();
        if (ret < rem) {
            throw new RuntimeException("KFS internal error: pos: " + ret
                    + " less than buffered: " + rem);
        }
        return ret - rem;
    }

    final public synchronized void close() throws IOException {
        if (state.kfsFd < 0) {
            return;
        }
        final int fd = state.kfsFd;
        state.kfsFd = -1;
        final KfsAccessBase ka = state.kfsAccess;
        state.kfsAccess = null;
        try {
            ka.kfs_close(fd);
        } finally {
            state.releaseBuffer();
        }
    }

    final public void setReadAheadSize(long readAheadSize) {
        if (readAheadSize >= 0) {
            state.kfsAccess.kfs_setReadAheadSize(state.kfsFd, readAheadSize);
            isReadAheadOff = readAheadSize == 0;
        }
    }
}
