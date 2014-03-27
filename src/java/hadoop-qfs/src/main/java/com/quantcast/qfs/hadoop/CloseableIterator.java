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

import java.io.Closeable;
import java.util.Iterator;

/**
 * A {@link Iterator} which is also {@link Closeable}.
 *
 * A Iterator that provides a close() to release resources.
 *
 * This can be useful if the Iterator holds resources that should be
 * released when the Iterator is no longer needed.
 */

public interface CloseableIterator<T> extends Iterator<T>, Closeable {
}
