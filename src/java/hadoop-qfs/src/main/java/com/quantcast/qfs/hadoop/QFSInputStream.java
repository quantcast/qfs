/**
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
 * Implements the Hadoop FSInputStream interfaces to allow applications to read
 * files in Quantcast File System (QFS), an extension of KFS.
 */

package com.quantcast.qfs.hadoop;

import java.io.*;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSInputStream;

import com.quantcast.qfs.access.KfsAccess;
import com.quantcast.qfs.access.KfsInputChannel;

class QFSInputStream extends FSInputStream {

  private final KfsInputChannel kfsChannel;
  private FileSystem.Statistics statistics;
  private final long fsize;

  public QFSInputStream(KfsAccess kfsAccess, String path,
                        FileSystem.Statistics stats) throws IOException {
    this.statistics = stats;
    this.kfsChannel = kfsAccess.kfs_open_ex(path, -1, -1);
    if (kfsChannel == null) {
      throw new IOException("QFS internal error -- null channel");
    }
    this.fsize = kfsAccess.kfs_filesize(path);
    if (this.fsize < 0) {
        kfsAccess.kfs_retToIOException((int)this.fsize);
    }
  }

  public long getPos() throws IOException {
    if (kfsChannel == null) {
      throw new IOException("File closed");
    }
    return kfsChannel.tell();
  }

  public synchronized int available() throws IOException {
    return (int) (this.fsize - getPos());
  }

  public synchronized void seek(long targetPos) throws IOException {
    kfsChannel.seek(targetPos);
  }

  public synchronized boolean seekToNewSource(long targetPos)
    throws IOException {
    return false;
  }

  public synchronized int read() throws IOException {
    byte b[] = new byte[1];
    int res = read(b, 0, 1);
    if (res == 1) {
      if (statistics != null) {
        statistics.incrementBytesRead(1);
      }
      return ((int) (b[0] & 0xff));
    }
    return -1;
  }

  public synchronized int read(byte b[], int off, int len) throws IOException {
    final int res = kfsChannel.read(ByteBuffer.wrap(b, off, len));
    // Use -1 to signify EOF
    if (res == 0) {
        return -1;
    }
    if (statistics != null) {
      statistics.incrementBytesRead(res);
    }
    return res;
  }

  public synchronized void close() throws IOException {
    kfsChannel.close();
  }

  public boolean markSupported() {
    return false;
  }

  public void mark(int readLimit) {
    // Do nothing
  }

  public void reset() throws IOException {
    throw new IOException("Mark not supported");
  }
}
