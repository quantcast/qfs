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
 * Provide the implementation of QFS which turn into calls to KfsAccess.
 */

package com.quantcast.qfs.hadoop;

import java.io.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import com.quantcast.qfs.access.KfsAccess;
import com.quantcast.qfs.access.KfsFileAttr;

import java.util.ArrayList;

class QFSImpl implements IFSImpl {
  private KfsAccess kfsAccess = null;
  private FileSystem.Statistics statistics;
  private final long BLOCK_SIZE  = 1 << 26;
  private final long ACCESS_TIME = 0;

  public QFSImpl(String metaServerHost, int metaServerPort,
                 FileSystem.Statistics stats) throws IOException {
    kfsAccess = new KfsAccess(metaServerHost, metaServerPort);
    statistics = stats;
  }

  public boolean exists(String path) throws IOException {
    return kfsAccess.kfs_exists(path);
  }

  public boolean isDirectory(String path) throws IOException {
    return kfsAccess.kfs_isDirectory(path);
  }

  public boolean isFile(String path) throws IOException {
    return kfsAccess.kfs_isFile(path);
  }

  public String[] readdir(String path) throws IOException {
    return kfsAccess.kfs_readdir(path);
  }

  public FileStatus[] readdirplus(Path path) throws IOException {
    KfsAccess.DirectoryIterator itr = null;
    try {
      itr = kfsAccess.new DirectoryIterator(path.toUri().getPath());
      final ArrayList<FileStatus> ret = new ArrayList<FileStatus>();
      String prefix = path.toString();
      if (! prefix.endsWith("/")) {
        prefix += "/";
      }
      while (itr.next()) {
        if (itr.filename.compareTo(".") == 0 ||
            itr.filename.compareTo("..") == 0) {
          continue;
        }
        ret.add(new FileStatus(
          itr.isDirectory ? 0L : itr.filesize,
          itr.isDirectory,
          itr.isDirectory ? 1 : itr.replication,
          itr.isDirectory ? 0 : BLOCK_SIZE,
          itr.modificationTime,
          ACCESS_TIME,
          FsPermission.createImmutable((short)itr.mode),
          itr.ownerName,
          itr.groupName,
          new Path(prefix + itr.filename)
        ));
      }
      return ret.toArray(new FileStatus[0]);
    } finally {
      if (itr != null) {
        itr.close();
      }
    }
  }

  public FileStatus stat(Path path) throws IOException {
    final KfsFileAttr fa  = new KfsFileAttr();
    final String      pn  = path.toUri().getPath();
    kfsAccess.kfs_retToIOException(kfsAccess.kfs_stat(pn, fa), pn);
    return new FileStatus(
      fa.isDirectory ? 0L : fa.filesize,
      fa.isDirectory,
      fa.isDirectory ? 1 : fa.replication,
      fa.isDirectory ? 0 : BLOCK_SIZE,
      fa.modificationTime,
      ACCESS_TIME,
      FsPermission.createImmutable((short)fa.mode),
      fa.ownerName,
      fa.groupName,
      path
    );
  }

  public KfsFileAttr fullStat(Path path) throws IOException {
    final KfsFileAttr fa  = new KfsFileAttr();
    final String      pn  = path.toUri().getPath();
    kfsAccess.kfs_retToIOException(kfsAccess.kfs_stat(pn, fa), pn);
    return fa;
  }

  public int mkdirs(String path, int mode) throws IOException {
    return kfsAccess.kfs_mkdirs(path, mode);
  }

  public int rename(String source, String dest) throws IOException {
    // QFS rename does not have mv semantics.
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
    return kfsAccess.kfs_rename(source, renameTarget);
  }

  public int rmdir(String path) throws IOException {
    return kfsAccess.kfs_rmdir(path);
  }

  public int rmdirs(String path) throws IOException {
    return kfsAccess.kfs_rmdirs(path);
  }

  public int remove(String path) throws IOException {
    return kfsAccess.kfs_remove(path);
  }

  public long filesize(String path) throws IOException {
    return kfsAccess.kfs_filesize(path);
  }

  public short getReplication(String path) throws IOException {
    return kfsAccess.kfs_getReplication(path);
  }

  public short setReplication(String path, short replication)
    throws IOException {
    return kfsAccess.kfs_setReplication(path, replication);
  }

  public String[][] getDataLocation(String path, long start, long len)
    throws IOException {
    return kfsAccess.kfs_getDataLocation(path, start, len);
  }

  public long getModificationTime(String path) throws IOException {
    return kfsAccess.kfs_getModificationTime(path);
  }

  public FSDataOutputStream create(String path, short replication,
                                   int bufferSize, boolean overwrite,
                                   int mode) throws IOException {
    final boolean append = false;
    return new FSDataOutputStream(new QFSOutputStream(
      kfsAccess, path, replication, overwrite, append, mode), statistics);
  }

  public FSDataInputStream open(String path, int bufferSize)
    throws IOException {
      return new FSDataInputStream(new QFSInputStream(kfsAccess, path,
                                                      statistics));
  }

  public FSDataOutputStream append(String path, short replication,
                                   int bufferSize) throws IOException {
    final boolean append    = true;
    final boolean overwrite = false;
    final int     mode      = 0666;
    return new FSDataOutputStream(new QFSOutputStream(
      kfsAccess, path, replication, overwrite, append, mode), statistics);
  }

  public void setPermission(String path, int mode) throws IOException {
    kfsAccess.kfs_retToIOException(kfsAccess.kfs_chmod(path, mode), path);
  }

  public void setOwner(String path, String username, String groupname)
    throws IOException {
    kfsAccess.kfs_retToIOException(kfsAccess.kfs_chown(
      path, username, groupname), path);
  }
}
