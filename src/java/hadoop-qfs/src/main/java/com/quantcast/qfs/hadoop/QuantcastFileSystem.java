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
 *
 * Use this class for Hadoop 0.2x and Hadoop 1.x. QuantcastFileSystem2 should
 * be used for Hadoop 2.x.
 */

package com.quantcast.qfs.hadoop;

import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import com.quantcast.qfs.access.KfsFileAttr;

public class QuantcastFileSystem extends FileSystem {

  protected FileSystem localFs    = null;
  protected IFSImpl    qfsImpl    = null;
  protected URI        uri        = null;
  protected Path       workingDir = null;

  public QuantcastFileSystem() {
  }

  QuantcastFileSystem(IFSImpl fsimpl, URI uri) {
    this.qfsImpl = fsimpl;
    this.uri     = uri;
  }

  public URI getUri() {
    return uri;
  }

  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    try {
      if (qfsImpl == null) {
        if (uri.getHost() == null) {
          qfsImpl = createIFSImpl(conf.get("fs.qfs.metaServerHost", ""),
                                conf.getInt("fs.qfs.metaServerPort", -1),
                                statistics);
        } else {
          qfsImpl = createIFSImpl(uri.getHost(), uri.getPort(), statistics);
        }
      }

      this.localFs = FileSystem.getLocal(conf);
      this.uri = URI.create(uri.getScheme() + "://" +
          (uri.getAuthority() == null ? "/" : uri.getAuthority()));
      this.workingDir = new Path("/user", System.getProperty("user.name")
                                ).makeQualified(uri, null);
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Unable to initialize QFS");
      System.exit(-1);
    }
  }

  protected IFSImpl createIFSImpl(String metaServerHost, int metaServerPort,
                               FileSystem.Statistics stats) throws IOException {
    return new QFSImpl(metaServerHost, metaServerPort, stats);
  }

  public Path getWorkingDirectory() {
    return workingDir;
  }

  public void setWorkingDirectory(Path dir) {
    try {
        workingDir = makeAbsolute(dir).makeQualified(uri, null);
    } catch (IOException ex) {
    }
  }

  protected Path makeAbsolute(Path path) throws IOException {
    if (path.isAbsolute()) {
      return path;
    }
    if (null == workingDir) {
      throw new IOException(path + ": absolute path required");
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

  // Internal implementation of listStatus. Will throw FileNotFounException
  // if path does not exist
  public FileStatus[] listStatusInternal(Path path) throws IOException {
    final Path absolute = makeAbsolute(path).makeQualified(uri, null);
    final FileStatus fs = qfsImpl.stat(absolute);
    return fs.isDir() ?
      qfsImpl.readdirplus(absolute) :
      new FileStatus[] { fs };
  }

  public FileStatus[] listStatus(Path path) throws IOException {
    try {
      return listStatusInternal(path);
    }
    catch (FileNotFoundException e) {
      return null;
    }
  }

  public FileStatus getFileStatus(Path path) throws IOException {
    return qfsImpl.stat(makeAbsolute(path).makeQualified(uri, null));
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
    if (file == null || start < 0 || len < 0) {
      throw new IllegalArgumentException(
        "file: "    + file +
        " start: "  + start +
        " length: " + len
      );
    }
    final String srep = makeAbsolute(file.getPath()).toUri().getPath();
    if (file.isDir()) {
      throw new IOException(srep + ": is a directory");
    }
    final String[][] hints = qfsImpl.getBlocksLocation(srep, start, len);
    if (hints == null || hints.length < 1 || hints[0].length != 1) {
      throw new Error(srep + ": getBlocksLocation internal error");
    }
    final long blockSize = Long.parseLong(hints[0][0], 16);
    if (blockSize < 0) {
      try {
        qfsImpl.retToIoException((int)blockSize);
      } catch (FileNotFoundException ex) {
      }
      return null;
    }
    if (blockSize == 0) {
      throw new Error(srep +
        ": getBlocksLocation internal error: 0 block size");
    }
    final long end = Math.min(file.getLen(), start + len);
    if (hints.length <= 1 || end <= start) {
      // Return an emtpy host list, as hadoop expects at least one location.
      final BlockLocation[] result = new BlockLocation[1];
      result[0] = new BlockLocation(
        null, null, start, Math.max(0L, end - start));
      return result;
    }
    final int               blkcnt =
        (int)((end - 1) / blockSize - start / blockSize + 1);
    final BlockLocation[]   result = new BlockLocation[blkcnt];
    final ArrayList<String> hlist  = new ArrayList<String>();
    long                    pos    = start - start % blockSize;
    for(int i = 0, m = 1; i < blkcnt; ++i) {
      hlist.clear();
      if (m < hints.length) {
        final String[] locs = hints[m++];
        hlist.ensureCapacity(locs.length);
        for(int k = 0; k < locs.length; ++k) {
          final int    idx  = locs[k].lastIndexOf(':');
          final String host = 0 < idx ? locs[k].substring(0, idx) : locs[k];
          if (! hlist.contains(host)) {
              hlist.add(host);
          }
        }
      }
      final long lpos = pos < start ? start : pos;
      final long bend = pos + blockSize;
      final int  hsz  = hlist.size();
      result[i] = new BlockLocation(
        null,
        hsz <= 0 ? null : hlist.toArray(new String[hsz]),
        lpos,
        (bend < end ? bend : end) - lpos
      );
      pos = bend;
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

  // Returns an iterator that returns each directory entry as a FileStatus with
  // a fully qualified Path. Call close() when done with the iterator.
  // throws FileNotFoundException if path does not exist
  // throws IOException if path is not a directory
  public CloseableIterator<FileStatus> getFileStatusIterator(Path path)
    throws IOException {
    return qfsImpl.getFileStatusIterator(this, path);
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

  public Token<?> getDelegationToken(String renewer) throws IOException {
    return null;
  }

  // The following two methods are needed to compile Qfs.java with hadoop 0.23.x
  public FileStatus getFileLinkStatus(Path path)
      throws IOException {
    return getFileStatus(path);
  }

  public boolean supportsSymlinks() {
    return false;
  }

}
