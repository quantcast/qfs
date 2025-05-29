/**
 * $Id$
 *
 * Created 2025/04/20
 *
 * @author: Mike Ovsiannikov (Quantcast Corporation)
 *
 * Copyright 2025 Quantcast Corporation. All rights reserved. Copyright 2007
 * Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * \brief QFS access java 9 style cleanup.
 */
package com.quantcast.qfs.access;

import java.io.IOException;
import java.lang.ref.Cleaner;

final public class KfsAccess extends KfsAccessBase {

    private static Cleaner cleaner = Cleaner.create();

    static Cleaner.Cleanable registerCleanup(Object obj, Runnable action) {
        return cleaner.register(obj, action);
    }

    private static void registerCleanupSelf(KfsAccess ka) {
        // Ensure that the native resource is cleaned up when this object is
        // garbage collected.
        // Make sure that this and ka are not referenced by the cleaner closure
        // otherwise it will never be cleaned up.
        final long ptr = ka.getCPtr();
        registerCleanup(ka, () -> {
            destroy(ptr);
        });
    }

    private void registerCleanupConstructed() {
        registerCleanupSelf(this);
    }

    public KfsAccess(String configFn) throws IOException {
        super(configFn);
        registerCleanupConstructed();
    }

    public KfsAccess(String metaServerHost,
                     int metaServerPort) throws IOException {
        super(metaServerHost, metaServerPort);
        registerCleanupConstructed();
    }
}
