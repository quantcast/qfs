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
 * Implements the Hadoop FS interfaces to allow applications to store files in
 * Quantcast File System (QFS). This is an extension of KFS.
 */

package com.quantcast.qfs.hadoop;

import java.io.*;
import java.net.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.BlockLocation;

import com.quantcast.qfs.access.KfsFileAttr;

public class QuantcastFileSystem extends FileSystem {

  private FileSystem localFs;
  private IFSImpl qfsImpl = null;
  private URI uri;
  private Path workingDir = new Path("/");

  public QuantcastFileSystem() {

  }

  QuantcastFileSystem(IFSImpl fsimpl) {
    this.qfsImpl = fsimpl;
  }

  public URI getUri() {
    return uri;
  }

  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    try {
      if (qfsImpl == null) {
        if (uri.getHost() == null) {
          qfsImpl = new QFSImpl(conf.get("fs.qfs.metaServerHost", ""),
                                conf.getInt("fs.qfs.metaServerPort", -1),
                                statistics);
        } else {
          qfsImpl = new QFSImpl(uri.getHost(), uri.getPort(), statistics);
        }
      }

      this.localFs = FileSystem.getLocal(conf);
      this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
      this.workingDir = new Path("/user", System.getProperty("user.name")
                                ).makeQualified(this);
      setConf(conf);

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Unable to initialize QFS");
      System.exit(-1);
    }
  }

  public Path getWorkingDirectory() {
    return workingDir;
  }

  public void setWorkingDirectory(Path dir) {
    workingDir = makeAbsolute(dir);
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  public boolean mkdirs(Path path, FsPermission permission)
    throws IOException {
    return qfsImpl.mkdirs(makeAbsolute(path).toUri().getPath(),
                          permission.toShort()) == 0;
  }

  @Deprecated
  public boolean isDirectory(Path path) throws IOException {
    Path absolute = makeAbsolute(path);
    String srep = absolute.toUri().getPath();
    return qfsImpl.isDirectory(srep);
  }

  @Deprecated
  public boolean isFile(Path path) throws IOException {
    Path absolute = makeAbsolute(path);
    String srep = absolute.toUri().getPath();
    return qfsImpl.isFile(srep);
  }

  public FileStatus[] listStatus(Path path) throws IOException {
    final Path absolute = makeAbsolute(path);
    final FileStatus fs = qfsImpl.stat(absolute);
    return fs.isDir() ?
      qfsImpl.readdirplus(absolute) :
      new FileStatus[] { fs };
  }

  public FileStatus getFileStatus(Path path) throws IOException {
    return qfsImpl.stat(makeAbsolute(path));
  }

  public FSDataOutputStream append(Path path, int bufferSize,
                                   Progressable progress) throws IOException {
    return qfsImpl.append(
          makeAbsolute(path).toUri().getPath(), (short)-1, bufferSize);
  }

  public FSDataOutputStream create(Path file, FsPermission permission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize,
                                   Progressable progress)
    throws IOException {
    Path parent = file.getParent();
    if (parent != null && !mkdirs(parent)) {
      throw new IOException("Mkdirs failed to create " + parent);
    }
    return qfsImpl.create(makeAbsolute(file).toUri().getPath(),
      replication, bufferSize, overwrite, permission.toShort());
  }

  public FSDataOutputStream createNonRecursive(Path file,
                                   FsPermission permission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize,
                                   Progressable progress)
    throws IOException {
    return qfsImpl.create(makeAbsolute(file).toUri().getPath(),
      replication, bufferSize, overwrite, permission.toShort());
  }

  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    return qfsImpl.open(makeAbsolute(path).toUri().getPath(), bufferSize);
  }

  public boolean rename(Path src, Path dst) throws IOException {
    Path absoluteS = makeAbsolute(src);
    String srepS = absoluteS.toUri().getPath();
    Path absoluteD = makeAbsolute(dst);
    String srepD = absoluteD.toUri().getPath();

    return qfsImpl.rename(srepS, srepD) == 0;
  }

  // recursively delete the directory and its contents
  public boolean delete(Path path, boolean recursive) throws IOException {
    final Path absolute = makeAbsolute(path);
    try {
      final KfsFileAttr fa   = qfsImpl.fullStat(absolute);
      final String      srep = absolute.toUri().getPath();
      if (! fa.isDirectory) {
        return qfsImpl.remove(srep) == 0;
      }
      if (recursive) {
        return qfsImpl.rmdirs(srep) == 0;
      }
      boolean notEmptyFlag = fa.fileCount > 0 || fa.dirCount > 0;
      if (! notEmptyFlag && (fa.fileCount < 0 || fa.dirCount < 0)) {
        // Backward compatibility: handle the case if sub counts are not
        // available.
        final FileStatus[] dirEntries = qfsImpl.readdirplus(absolute);
        notEmptyFlag = dirEntries != null && dirEntries.length > 0;
      }
      if (notEmptyFlag) {
        throw new IOException("Directory " + path.toString() + " is not empty.");
      }
      return qfsImpl.rmdir(srep) == 0;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  public short getDefaultReplication() {
    return 3;
  }

  public boolean setReplication(Path path, short replication)
    throws IOException {

    Path absolute = makeAbsolute(path);
    String srep = absolute.toUri().getPath();

    int res = qfsImpl.setReplication(srep, replication);
    return res >= 0;
  }

  // 64MB is the QFS block size
  public long getDefaultBlockSize() {
    return 1 << 26;
  }

  /**
   * Return null if the file doesn't exist; otherwise, get the
   * locations of the various chunks of the file file from QFS.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file,
    long start, long len) throws IOException {

    if (file == null) {
      return null;
    }
    String srep = makeAbsolute(file.getPath()).toUri().getPath();
    String[][] hints = qfsImpl.getDataLocation(srep, start, len);
    if (hints == null) {
      return null;
    }
    BlockLocation[] result = new BlockLocation[hints.length];
    long blockSize = getDefaultBlockSize();
    long length = len;
    long blockStart = start;
    for(int i=0; i < result.length; ++i) {
      result[i] = new BlockLocation(
                      null,
                      hints[i],
                      blockStart,
                      length < blockSize ? length : blockSize);
      blockStart += blockSize;
      length -= blockSize;
    }
    return result;
  }

  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    FileUtil.copy(localFs, src, this, dst, delSrc, getConf());
  }

  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    FileUtil.copy(this, src, localFs, dst, delSrc, getConf());
  }

  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return tmpLocalFile;
  }

  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    moveFromLocalFile(tmpLocalFile, fsOutputFile);
  }

  public void setPermission(Path path, FsPermission permission)
    throws IOException {
    qfsImpl.setPermission(makeAbsolute(path).toUri().getPath(),
                          permission.toShort());
  }

  public void setOwner(Path path, String username, String groupname)
    throws IOException {
    qfsImpl.setOwner(makeAbsolute(path).toUri().getPath(),
      username, groupname);
  }

  // The following is to get du and dus working without implementing file
  // and directory counts on in the meta server.
  private class ContentSummaryProxy extends ContentSummary
  {
    private ContentSummary cs;
    private final Path path;

    private ContentSummaryProxy(Path path, long len) {
      super(len, -1, -1);
      this.path = path;
    }

    private ContentSummary get() {
      if (cs == null) {
        try {
          cs = getContentSummarySuper(path);
        } catch (IOException ex) {
          cs = this;
        }
      }
      return cs;
    }

    public long getDirectoryCount() {
      return get().getDirectoryCount();
    }

    public long getFileCount() {
      return get().getFileCount();
    }

    public void write(DataOutput out) throws IOException {
      get().write(out);
    }

    public String toString(boolean qOption) {
      return get().toString(qOption);
    }
  }

  private ContentSummary getContentSummarySuper(Path path) throws IOException {
    return super.getContentSummary(path);
  }

  public ContentSummary getContentSummary(Path path) throws IOException {
    // since QFS stores sizes at each level of the dir tree, we can
    // just stat the dir.
    final Path absolute = makeAbsolute(path);
    final KfsFileAttr stat = qfsImpl.fullStat(absolute);
    if (stat.isDirectory) {
      final long len = stat.filesize;
      if (len < 0) {
        return getContentSummarySuper(absolute);
      }
      if (stat.dirCount < 0) {
        return new ContentSummaryProxy(absolute, len);
      }
      return new ContentSummary(len, stat.fileCount, stat.dirCount + 1);
    }
    return new ContentSummary(stat.filesize, 1, 0);
  }
}
