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
 * Unit tests for testing the KosmosFileSystem API implementation.
 */

package com.quantcast.qfs.hadoop;

import java.io.*;
import java.net.*;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.quantcast.qfs.hadoop.QuantcastFileSystem;

public class TestQuantcastFileSystem extends TestCase {

  QuantcastFileSystem quantcastFileSystem;
  QFSEmulationImpl qfsEmul;
  Path baseDir;

  @Override
  protected void setUp() throws IOException {
    Configuration conf = new Configuration();

    qfsEmul = new QFSEmulationImpl(conf);
    quantcastFileSystem = new QuantcastFileSystem(qfsEmul, null);
    // a dummy URI; we are not connecting to any setup here
    quantcastFileSystem.initialize(URI.create("qfs:///"), conf);
    baseDir = new Path(System.getProperty("test.build.data", "/tmp" ) +
                                          "/qfs-test");
  }

  @Override
  protected void tearDown() throws Exception {

  }

  // @Test
  public void testMakeQualified() throws Exception {
    assertEquals("qfs:/", new Path("/").makeQualified(quantcastFileSystem).toString());
  }

  // @Test
  // Check all the directory API's in QFS
  public void testDirs() throws Exception {
    Path subDir1 = new Path("dir.1");

    // make the dir
    quantcastFileSystem.mkdirs(baseDir);
    assertTrue(quantcastFileSystem.isDirectory(baseDir));
    quantcastFileSystem.setWorkingDirectory(baseDir);

    quantcastFileSystem.mkdirs(subDir1);
    assertTrue(quantcastFileSystem.isDirectory(subDir1));

    assertFalse(quantcastFileSystem.exists(new Path("test1")));
    assertFalse(quantcastFileSystem.isDirectory(new Path("test/dir.2")));

    FileStatus[] p = quantcastFileSystem.listStatus(baseDir);
    assertEquals(p.length, 1);

    quantcastFileSystem.delete(baseDir, true);
    assertFalse(quantcastFileSystem.exists(baseDir));
  }

  // @Test
  // Check the file API's
  public void testFiles() throws Exception {
    Path subDir1 = new Path("dir.1");
    Path file1 = new Path("dir.1/foo.1");
    Path file2 = new Path("dir.1/foo.2");

    quantcastFileSystem.mkdirs(baseDir);
    assertTrue(quantcastFileSystem.isDirectory(baseDir));
    quantcastFileSystem.setWorkingDirectory(baseDir);

    quantcastFileSystem.mkdirs(subDir1);

    FSDataOutputStream s1 = quantcastFileSystem.create(
      file1, true, 4096, (short) 1, (long) 4096, null);
    FSDataOutputStream s2 = quantcastFileSystem.create(
      file2, true, 4096, (short) 1, (long) 4096, null);

    s1.close();
    s2.close();

    FileStatus[] p = quantcastFileSystem.listStatus(subDir1);
    assertEquals(p.length, 2);

    quantcastFileSystem.delete(file1, true);
    p = quantcastFileSystem.listStatus(subDir1);
    assertEquals(p.length, 1);

    quantcastFileSystem.delete(file2, true);
    p = quantcastFileSystem.listStatus(subDir1);
    assertEquals(p.length, 0);

    quantcastFileSystem.delete(baseDir, true);
    assertFalse(quantcastFileSystem.exists(baseDir));
  }

  // @Test
  // Check file/read write
  public void testFileIO() throws Exception {
    Path subDir1 = new Path("dir.1");
    Path file1 = new Path("dir.1/foo.1");

    quantcastFileSystem.mkdirs(baseDir);
    assertTrue(quantcastFileSystem.isDirectory(baseDir));
    quantcastFileSystem.setWorkingDirectory(baseDir);

    quantcastFileSystem.mkdirs(subDir1);

    FSDataOutputStream s1 = quantcastFileSystem.create(
      file1, true, 4096, (short) 1, (long) 4096, null);

    int bufsz = 4096;
    byte[] data = new byte[bufsz];

    for (int i = 0; i < data.length; i++)
        data[i] = (byte) (i % 16);

    // write 4 bytes and read them back; read API should return a byte per call
    s1.write(32);
    s1.write(32);
    s1.write(32);
    s1.write(32);
    // write some data
    s1.write(data, 0, data.length);
    // flush out the changes
    s1.close();

    // Read the stuff back and verify it is correct
    FSDataInputStream s2 = quantcastFileSystem.open(file1, 4096);
    int v;

    v = s2.read();
    assertEquals(v, 32);
    v = s2.read();
    assertEquals(v, 32);
    v = s2.read();
    assertEquals(v, 32);
    v = s2.read();
    assertEquals(v, 32);

    assertEquals(s2.available(), data.length);

    byte[] buf = new byte[bufsz];
    s2.read(buf, 0, buf.length);
    for (int i = 0; i < data.length; i++)
        assertEquals(data[i], buf[i]);

    assertEquals(s2.available(), 0);

    s2.close();

    quantcastFileSystem.delete(file1, true);
    assertFalse(quantcastFileSystem.exists(file1));
    quantcastFileSystem.delete(subDir1, true);
    assertFalse(quantcastFileSystem.exists(subDir1));
    quantcastFileSystem.delete(baseDir, true);
    assertFalse(quantcastFileSystem.exists(baseDir));
  }
}
