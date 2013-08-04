/**
 * $Id$
 *
 * Created 2007/08/24
 * @author: Sriram Rao (Kosmix Corp.)
 *
 * Copyright 2007 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
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
 * \brief Java buffer pool for KFS client.
 */

package com.quantcast.qfs.access;

import java.nio.ByteBuffer;
import java.util.AbstractQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class BufferPool {
	public static final int BUFFER_SIZE = Integer.getInteger("qfs.access.buffer.size", 1 << 20);

	private final AbstractQueue<ByteBuffer> buffers = new ConcurrentLinkedQueue<ByteBuffer>();

	private static final BufferPool INSTANCE = new BufferPool();

	private BufferPool() {
	};

	public static BufferPool getInstance() {
		return INSTANCE;
	}

	public ByteBuffer getBuffer() {
		ByteBuffer ret = buffers.poll();
		if (ret == null) {
			ret = ByteBuffer.allocateDirect(BUFFER_SIZE);
		}
		return ret;
	}

	public void releaseBuffer(ByteBuffer b) {
		b.clear();
		buffers.add(b);
	}
}
