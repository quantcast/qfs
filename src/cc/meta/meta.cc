/*!
 * $Id$
 *
 * \file meta.cc
 * \brief Operations on the various metadata types.
 * \author Blake Lewis (Kosmix Corp.)
 *
 * Copyright 2008-2012 Quantcast Corp.
 * Copyright 2006-2008 Kosmix Corp.
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
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include "meta.h"
#include "kfstree.h"
#include "kfstypes.h"
#include "util.h"
#include "LayoutManager.h"

namespace KFS
{
using std::ostringstream;

/*
 * Seed the unique id generators for files/chunks to start at 2
 */
UniqueID fileID(0, ROOTFID);
UniqueID chunkID(1, ROOTFID);

ostream&
MetaDentry::show(ostream& os) const
{
    return (os <<
    "dentry/name/" << name <<
    "/id/"         << id() <<
    "/parent/"     << dir
    );
}

bool
MetaDentry::match(const Meta *m) const
{
    // Try not to fetch name, save 1 dram miss by comparing hash instead.
    return (m->metaType() == KFS_DENTRY &&
        hash == refine<MetaDentry>(m)->hash &&
        refine<MetaDentry>(m)->compareName(name) == 0);
}

ostream&
MetaFattr::show(ostream& os) const
{
    static const char* const fname[] = { "empty", "file", "dir" };

    os <<
    "fattr/"         << fname[type] <<
    "/id/"           << id() <<
    "/chunkcount/"   << (type == KFS_DIR ? 0 : chunkcount()) <<
    "/numReplicas/"  << numReplicas <<
    "/mtime/"        << ShowTime(mtime) <<
    "/ctime/"        << ShowTime(ctime) <<
    "/crtime/"       << ShowTime(crtime) <<
    "/filesize/"     << filesize;
    if (IsStriped()) {
        os <<
            "/striperType/"        << striperType <<
            "/numStripes/"         << numStripes <<
            "/numRecoveryStripes/" << numRecoveryStripes <<
            "/stripeSize/"         << stripeSize;
    }
        os <<
        "/user/"  << user <<
        "/group/" << group <<
        "/mode/"  << mode;
    return os;
}

void
MetaChunkInfo::DeleteChunk()
{
    // Update the metatree to reflect chunk deletion.  Since we got
    // this MetaChunkInfo by retrieving the allocation information,
    // deletion from the metatree should not fail.
    if (metatree.del(this)) {
        panic("deleteChunk", false);
    }
}

ostream&
MetaChunkInfo::show(ostream& os) const
{
    return (os <<
    "chunkinfo/fid/" << id() <<
    "/chunkid/"      << chunkId <<
    "/offset/"       << offset <<
    "/chunkVersion/" << chunkVersion
    );
}
} // namespace KFS
