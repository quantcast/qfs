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

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import com.quantcast.qfs.hadoop.QuantcastFileSystem2;


public class TestQuantcastFileSystem2 extends TestQuantcastFileSystem {

  @Override
  protected void setUp() throws IOException {
    Configuration conf = new Configuration();

    qfsEmul = new QFSEmulationImpl(conf);
    quantcastFileSystem = new QuantcastFileSystem2(qfsEmul, null);
    // a dummy URI; we are not connecting to any setup here
    quantcastFileSystem.initialize(URI.create("qfs:///"), conf);
    baseDir = new Path(System.getProperty("test.build.data", "/tmp" ) +
                                          "/qfs-test");
  }

  // @Test
  // Enasure HDFS compatibility
  public void testHDFSCompatibility() throws Exception {
    assertEquals(quantcastFileSystem.getScheme(), "qfs");
  }
}
