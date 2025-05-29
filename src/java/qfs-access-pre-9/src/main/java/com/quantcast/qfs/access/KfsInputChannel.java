/**
 * $Id$
 *
 * Created 2007/09/11
 *
 * @author: Sriram Rao (Kosmix Corp.)
 *
 * Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
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
 * \brief An input channel that does buffered I/O.  This is to reduce
 * the overhead of JNI calls.
 */
package com.quantcast.qfs.access;

/* A byte channel interface with seek support */
final public class KfsInputChannel extends KfsInputChannelBase {

    KfsInputChannel(KfsAccessBase ka, int fd) {
        super(ka, fd);
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            state.release();
        } finally {
            super.finalize();
        }
    }
}
