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
 * \brief Java wrappers to get to the KFS client.
 */
package com.quantcast.qfs.access;

import java.io.IOException;

final public class KfsAccess extends KfsAccessBase {

    public KfsAccess(String configFn) throws IOException {
        super(configFn);
    }

    public KfsAccess(String metaServerHost,
            int metaServerPort) throws IOException {
        super(metaServerHost, metaServerPort);
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            kfs_destroy();
        } finally {
            super.finalize();
        }
    }
}
