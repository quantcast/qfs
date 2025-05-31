/**
 * $Id$
 *
 * Created 2025/04/20
 *
 * @author: Mike Ovsiannikov (Quantcast Corporation)
 *
 * Copyright 2025 Quantcast Corporation. All rights reserved.
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
 * \brief Input channel java 9 style cleanup.
 */
package com.quantcast.qfs.access;

import java.io.IOException;
import java.lang.ref.Cleaner;

final public class KfsInputChannel extends KfsInputChannelBase {

    final private Cleaner.Cleanable cleanable;

    KfsInputChannel(KfsAccessBase ka, int fd) {
        super(ka, fd);
        cleanable = registerCleanup();
    }

    private Cleaner.Cleanable registerCleanup() {
        return KfsAccess.registerCleanup(this, state);
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            cleanable.clean();
        }
    }
}
