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
 * Extends QuantcastFileSystem as needed to be compatible with Hadoop 2.x.
 */

package com.quantcast.qfs.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class QuantcastFileSystem2 extends QuantcastFileSystem {
  
  public QuantcastFileSystem2() {
    super();
  }

  QuantcastFileSystem2(IFSImpl fsimpl, URI uri) {
    super(fsimpl, uri);
  }

  public FileStatus[] listStatus(Path path) throws IOException {
    return listStatusInternal(path);
  }

}
