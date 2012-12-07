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
 * Implements the Hadoop FSOutputStream interfaces to allow applications
 * to write to files in Quantcast File System (QFS).
 */

package com.quantcast.qfs.hadoop;

import java.io.*;
import java.net.*;
import java.util.*;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.Progressable;

import com.quantcast.qfs.access.KfsAccess;
import com.quantcast.qfs.access.KfsOutputChannel;

class QFSOutputStream extends OutputStream {

  private final KfsOutputChannel kfsChannel;

  public QFSOutputStream(KfsAccess kfsAccess, String path, short replication,
    boolean overwrite, boolean append, int mode) throws IOException {
    if (append) {
      this.kfsChannel = kfsAccess.kfs_append_ex(path, (int)replication, mode);
    } else {
      final long    bufferSize    = -1;
      final long    readAheadSize = -1;
      final boolean exclusive     = ! overwrite;
      this.kfsChannel = kfsAccess.kfs_create_ex(
        path, replication, exclusive, bufferSize, readAheadSize, mode);
    }
    if (kfsChannel == null) {
      throw new IOException("QFS internal error -- null channel");
    }
  }

  public long getPos() throws IOException {
    return kfsChannel.tell();
  }

  public void write(int v) throws IOException {
    byte[] b = new byte[1];
    b[0] = (byte) v;
    write(b, 0, 1);
  }

  public void write(byte b[], int off, int len) throws IOException {
    kfsChannel.write(ByteBuffer.wrap(b, off, len));
  }

  public void flush() throws IOException {
    kfsChannel.sync();
  }

  public synchronized void close() throws IOException {
    flush();
    kfsChannel.close();
  }
}
