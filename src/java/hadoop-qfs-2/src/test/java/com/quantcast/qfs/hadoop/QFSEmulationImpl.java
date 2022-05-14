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
 * We need to provide the ability to the code in fs/kfs without really
 * having a KFS deployment.  For this purpose, use the LocalFileSystem
 * as a way to "emulate" KFS.
 */

package com.quantcast.qfs.hadoop;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.permission.FsPermission;

import com.quantcast.qfs.access.KfsFileAttr;

import java.lang.reflect.Method;


public class QFSEmulationImpl implements IFSImpl {
  FileSystem localFS;
  int umask;

  public QFSEmulationImpl(Configuration conf) throws IOException {
    localFS = FileSystem.getLocal(conf);
    umask = FsPermission.getUMask(conf).toShort();
  }

  public boolean exists(String path) throws IOException {
    return localFS.exists(new Path(path));
  }

  public boolean isDirectory(String path) throws IOException {
    return localFS.isDirectory(new Path(path));
  }

  public boolean isFile(String path) throws IOException {
    return localFS.isFile(new Path(path));
  }

  public String[] readdir(String path) throws IOException {
    FileStatus[] p = localFS.listStatus(new Path(path));
    String[] entries = null;

    if (p == null) {
      return null;
    }

    entries = new String[p.length];
    for (int i = 0; i < p.length; i++) {
      entries[i] = p[i].getPath().toString();
    }
    return entries;
  }

  public FileStatus[] readdirplus(Path path) throws IOException {
    return localFS.listStatus(new Path(path.toUri().getPath()));
  }

  public FileStatus stat(Path path) throws IOException {
    return localFS.getFileStatus(new Path(path.toUri().getPath()));
  }

  public FileStatus lstat(Path path) throws IOException {
    Method getFileLinkStatusMethod;
    try {
      getFileLinkStatusMethod = FileSystem.class.getMethod(
        "getFileLinkStatus", Path.class);
    } catch (Exception ex) {
        throw new IOException("symlink should not be invoked," +
            " as getFileLinkStatus() is not defined: " + ex.getMessage());
    }
    try {
      return (FileStatus)getFileLinkStatusMethod.invoke(
        localFS, new Path(path.toUri().getPath()));
    } catch (Exception ex) {
        if (ex.getCause().getClass().isInstance(IOException.class)) {
            throw (IOException)ex.getCause();
        }
        throw new IOException("getFileLinkStatus method invocation: " +
          ex.getMessage(), ex);
    }
  }

  public KfsFileAttr fullStat(Path path) throws IOException {
    FileStatus fs = localFS.getFileStatus(new Path(path.toUri().getPath()));
    KfsFileAttr fa = new KfsFileAttr();
    fa.filename = fs.getPath().toUri().getPath();
    fa.isDirectory = fs.isDir();
    fa.filesize = fs.getLen();
    fa.replication = fs.getReplication();
    fa.modificationTime = fs.getModificationTime();
    return fa;
  }

  public int mkdir(String path, int mode) throws IOException {
    if (localFS.mkdirs(new Path(path))) {
      return 0;
    }
    return -1;
  }

  public int mkdirs(String path, int mode) throws IOException {
    if (localFS.mkdirs(new Path(path))) {
      return 0;
    }
    return -1;
  }

  public int rename(String source, String dest) throws IOException {
    if (localFS.rename(new Path(source), new Path(dest))) {
      return 0;
    }
    return -1;
  }

  public int rename2(String source, String dest,
      boolean overwrite) throws IOException {
    if (localFS.rename(new Path(source), new Path(dest))) {
      return 0;
    }
    return -1;
  }

  public int rmdir(String path) throws IOException {
    if (isDirectory(path)) {
      // the directory better be empty
      String[] dirEntries = readdir(path);
      if ((dirEntries.length <= 2) && (localFS.delete(new Path(path), true))) {
        return 0;
      }
    }
    return -1;
  }

  public int remove(String path) throws IOException {
    if (isFile(path) && (localFS.delete(new Path(path), true))) {
      return 0;
    }
    return -1;
  }

  public int rmdirs(String path) throws IOException {
    if (isFile(path)) {
      return -1;
    }
    return localFS.delete(new Path(path), true) ? 0 : -1;
  }

  public long filesize(String path) throws IOException {
    return localFS.getLength(new Path(path));
  }

  public short getReplication(String path) throws IOException {
    return 1;
  }

  public short setReplication(String path, short replication)
    throws IOException {
    return 1;
  }

  public String[][] getDataLocation(String path, long start, long len)
    throws IOException {
    final BlockLocation[] blkLocations = localFS.getFileBlockLocations(
      localFS.getFileStatus(new Path(path)), start, len);
    if ((blkLocations == null) || (blkLocations.length == 0)) {
      return new String[0][];
    }
    final int blkCount = blkLocations.length;
    final String[][]hints = new String[blkCount][];
    for (int i=0; i < blkCount ; i++) {
      hints[i] = blkLocations[i].getHosts();;
    }
    return hints;
  }

  public String[][] getBlocksLocation(String path, long start, long len)
    throws IOException {
    final BlockLocation[] blkLocations = localFS.getFileBlockLocations(
      localFS.getFileStatus(new Path(path)), start, len);
    if ((blkLocations == null) || (blkLocations.length == 0)) {
      return new String[0][];
    }
    final int blkCount = blkLocations.length + 1;
    final String[][]hints = new String[blkCount][];
    hints[0]    = new String[1];
    hints[0][0] =  Long.toHexString(
      blkCount <= 0 ? 1L : blkLocations[0].getLength());
    for (int i=1; i < blkCount ; i++) {
      hints[i] = blkLocations[i].getHosts();;
    }
    return hints;
  }

  public long getModificationTime(String path) throws IOException {
    FileStatus s = localFS.getFileStatus(new Path(path));
    if (s == null) {
      return 0;
    }
    return s.getModificationTime();
  }

  public FSDataOutputStream create(String path, short replication,
    int bufferSize, boolean overwrite, int mode, boolean append) throws IOException {
    // besides path/overwrite, the other args don't matter for
    // testing purposes.
    return localFS.create(new Path(path));
  }

  public FSDataOutputStream create(String path, short replication,
    int bufferSize, boolean overwrite, int mode) throws IOException {
    // besides path/overwrite, the other args don't matter for
    // testing purposes.
    return localFS.create(new Path(path));
  }

  public FSDataOutputStream create(String path, boolean overwrite,
          String createParams, int mode, boolean forceType) throws IOException {
      // besides path/overwrite, the other args don't matter for
      // testing purposes.
      return localFS.create(new Path(path));
  }

  public FSDataInputStream open(String path, int bufferSize)
    throws IOException {
    return localFS.open(new Path(path));
  }

  public FSDataOutputStream append(String path, short replication,
    int bufferSize) throws IOException {
    return localFS.create(new Path(path));
  }

  public void setPermission(String path, int mode) throws IOException {
    localFS.setPermission(new Path(path), new FsPermission((short)mode));
  }

  public void setOwner(String path, String username, String groupname)
    throws IOException {
    localFS.setOwner(new Path(path), username, groupname);
  }

  public void setUMask(int mask)
    throws IOException {
    umask = mask;
  }

  public int getUMask()
    throws IOException {
    return umask;
  }

  public CloseableIterator<FileStatus> getFileStatusIterator(FileSystem fs, Path path)
    throws IOException {
    return null;
  }

  public void retToIoException(int status)
    throws IOException {
    if (status < 0) {
      throw new IOException("IO exception status: " + status);
    }
  }

  public void symlink(String target, String link, int mode, boolean overwrite)
    throws IOException {
    Method createSymlinkMethod;
    try {
      createSymlinkMethod = FileSystem.class.getMethod(
        "createSymlink", Path.class, Path.class, boolean.class);
    } catch (Exception ex) {
        throw new IOException("symlink should not be invoked," +
            " as symlink() is not definedL " + ex.getMessage());
    }
    try {
      final boolean createParent = false;
      createSymlinkMethod.invoke(
        localFS, new Path(target), new Path(link), createParent);
      return;
    } catch (Exception ex) {
        if (ex.getCause().getClass().isInstance(IOException.class)) {
            throw (IOException)ex.getCause();
        }
        throw new IOException("symlink method invocation: " +
          ex.getMessage(), ex);
    }
  }

  public Path getLinkTarget(Path path) throws IOException {
    Method getLinkTargetMethod;
    try {
      getLinkTargetMethod = FileSystem.class.getMethod(
        "getLinkTarget", Path.class);
    } catch (Exception ex) {
        throw new IOException("getLinkTarget should not be invoked," +
            " as getLinkTarget is not definedL " + ex.getMessage());
    }
    try {
      return (Path)getLinkTargetMethod.invoke(localFS, path);
    } catch (Exception ex) {
        if (ex.getCause().getClass().isInstance(IOException.class)) {
            throw (IOException)ex.getCause();
        }
        throw new IOException("getLinkTarget method invocation: " +
          ex.getMessage(), ex);
    }
  }
}
