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
 * \brief An output channel pre java 9 style cleanup.
 */
package com.quantcast.qfs.access;

final public class KfsOutputChannel extends KfsOutputChannelBase {

    KfsOutputChannel(KfsAccessBase kfsAccess, int fd, boolean append) {
        super(kfsAccess, fd, append);
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            state.run();
        } finally {
            super.finalize();
        }
    }
}
