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

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.fs.local.LocalConfigKeys;

import com.quantcast.qfs.access.KfsFileAttr;

public class Qfs extends AbstractFileSystem {

  private final IFSImpl             qfsImpl;
  private final QuantcastFileSystem qfs;

  private static final String  kUriScheme              = "qfs";
  private static final boolean kUriAuthorityNeededFlag = true;
  private static final int     kDefaultPort            = 20000;

  public Qfs(URI uri, Configuration conf)
      throws IOException, URISyntaxException {
    super(uri, kUriScheme, kUriAuthorityNeededFlag, kDefaultPort);
    final String scheme = uri.getScheme();
    if (! scheme.equalsIgnoreCase(kUriScheme)) {
      throw new IllegalArgumentException(
        "requested URI scheme: " + scheme + " is not for QFS");
    }
    if (uri.getHost() == null) {
      this.qfsImpl = new QFSImpl(
        conf.get("fs.qfs.metaServerHost", ""),
        conf.getInt("fs.qfs.metaServerPort", -1),
        getStatistics()
      );
    } else {
      this.qfsImpl = new QFSImpl(
        uri.getHost(), uri.getPort(), getStatistics());
    }
    this.qfs = new QuantcastFileSystem2(this.qfsImpl, uri);
  }

  @Override
  public int getUriDefaultPort() {
    return kDefaultPort;
  }

  @Override
  public FSDataOutputStream createInternal(
    Path                path,
    EnumSet<CreateFlag> createFlag,
    FsPermission        absolutePermission,
    int                 bufferSize,
    short               replication,
    long                blockSize,
    Progressable        progress,
    ChecksumOpt         checksumOpt,
    boolean             createParent)
      throws IOException {
    CreateFlag.validate(createFlag);
    if (createParent) {
      mkdir(path.getParent(), absolutePermission, createParent);
    }
    return qfsImpl.create(
      getUriPath(path),
      replication,
      bufferSize,
      createFlag.contains(CreateFlag.OVERWRITE),
      absolutePermission.toShort(),
      createFlag.contains(CreateFlag.APPEND)
    );
  }

  @Override
  public boolean delete(Path path, boolean recursive)
      throws IOException, UnresolvedLinkException {
    return qfs.delete(path, recursive);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path path, long start, long len)
      throws IOException, UnresolvedLinkException {
    return qfs.getFileBlockLocations(qfs.getFileStatus(path), start, len);
  }

  @Override
  public FileChecksum getFileChecksum(Path path)
      throws IOException, UnresolvedLinkException {
    return qfs.getFileChecksum(path);
  }

  @Override
  public FileStatus getFileStatus(Path path)
      throws IOException, UnresolvedLinkException {
    final Path       qp = makeQualified(path);
    final FileStatus fi = qfs.getFileStatus(qp);
    if (fi == null) {
      throw new FileNotFoundException(qp.toUri() +
        ": No such file or directory");
    }
    return fi;
  }

  @Override
  public FileStatus getFileLinkStatus(Path path)
      throws IOException, UnresolvedLinkException {
    final Path       qp = makeQualified(path);
    final FileStatus fi = qfs.getFileLinkStatus(qp);
    if (fi == null) {
      throw new FileNotFoundException(qp.toUri() +
        ": No such file or directory");
    }
    return fi;
  }

  @Override
  public FsStatus getFsStatus()
      throws IOException {
    return new FsStatus(0, 0, 0);
  }

  @Override
  public FsServerDefaults getServerDefaults()
      throws IOException {
    return LocalConfigKeys.getServerDefaults();
  }

  @Override
  public FileStatus[] listStatus(Path path)
      throws IOException, UnresolvedLinkException {
    final Path         qp = makeQualified(path);
    final FileStatus[] ls = qfs.listStatus(qp);
    if (ls == null) {
      throw new FileNotFoundException(qp.toUri() +
        ": No such file or directory");
    }
    return ls;
  }

  @Override
  public void mkdir(Path dir, FsPermission permission, boolean createParent)
      throws IOException, UnresolvedLinkException {
    qfsImpl.retToIoException(
      createParent ?
        qfsImpl.mkdirs(getUriPath(dir), permission.toShort()) :
        qfsImpl.mkdir(getUriPath(dir), permission.toShort())
    );
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize)
      throws IOException, UnresolvedLinkException {
    return qfs.open(path, bufferSize);
  }

  @Override
  public void renameInternal(Path src, Path dst)
      throws IOException, UnresolvedLinkException {
    final boolean kOverwriteDestinationFlag = false;
    qfsImpl.retToIoException(
      qfsImpl.rename2(getUriPath(src), getUriPath(dst),
        kOverwriteDestinationFlag)
    );
  }

  @Override
  public void setOwner(Path path, String username, String groupname)
      throws IOException, UnresolvedLinkException {
    qfs.setOwner(path, username, groupname);
  }

  @Override
  public void setPermission(Path path, FsPermission permission)
      throws IOException, UnresolvedLinkException {
    qfs.setPermission(path, permission);
  }

  @Override
  public boolean setReplication(Path path, short replication)
      throws IOException, UnresolvedLinkException {
    return qfs.setReplication(path, replication);
  }

  @Override
  public void setTimes(Path path, long mtime, long atime)
      throws IOException, UnresolvedLinkException {
    qfs.setTimes(path, mtime, atime);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum)
      throws IOException {
    qfs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public boolean supportsSymlinks() {
    return qfs.supportsSymlinks();
  }

  @Override // AbstractFileSystem
  public List<Token<?>> getDelegationTokens(String renewer)
      throws IOException {
    Token<?> result = qfs.getDelegationToken(renewer);
    List<Token<?>> tokenList = new ArrayList<Token<?>>();
    if (result != null) {
      tokenList.add(result);
    }
    return tokenList;
  }
}
