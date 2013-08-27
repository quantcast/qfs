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

#include "meta.h"
#include "kfstree.h"
#include "CSMap.h"
#include "util.h"

#include <iostream>
#include <fstream>
#include <sstream>

namespace KFS
{
using std::ostringstream;

/*
 * Seed the unique id generators for files/chunks to start at 2
 */
UniqueID fileID(0, ROOTFID);
UniqueID chunkID(1, ROOTFID);

inline ostream&
MetaDentry::showSelf(ostream& os) const
{
    return (os <<
    "dentry/name/" << name <<
    "/id/"         << id() <<
    "/parent/"     << dir
    );
}

inline bool
MetaDentry::matchSelf(const Meta *m) const
{
    // Try not to fetch name, save 1 dram miss by comparing hash instead.
    return (m->metaType() == KFS_DENTRY &&
        hash == refine<MetaDentry>(m)->hash &&
        refine<MetaDentry>(m)->compareName(name) == 0);
}

inline ostream&
MetaFattr::showSelf(ostream& os) const
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
    if (minSTier < kKfsSTierMax) {
        os <<
            "/minTier/" << (int)minSTier <<
            "/maxTier/" << (int)maxSTier;
    }
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

inline ostream&
MetaChunkInfo::showSelf(ostream& os) const
{
    return (os <<
    "chunkinfo/fid/" << id() <<
    "/chunkid/"      << chunkId <<
    "/offset/"       << offset <<
    "/chunkVersion/" << chunkVersion
    );
}

void
MetaNode::destroy()
{
    switch (nodetype) {
        case KFS_INTERNAL:
            static_cast<Node*>(this)->destroySelf();
            return;
        case KFS_FATTR:
            static_cast<MetaFattr*>(this)->destroySelf();
            return;
        case KFS_CHUNKINFO:
            CSMap::Entry::GetCsEntry(this)->destroySelf();
            return;
        case KFS_DENTRY:
            static_cast<MetaDentry*>(this)->destroySelf();
            return;
        default:
            panic("Meta::destroy: invalid node type");
    }
}

Key
MetaNode::key() const
{
    switch (nodetype) {
        case KFS_INTERNAL:
            return static_cast<const Node*>(this)->keySelf();
        case KFS_FATTR:
            return static_cast<const MetaFattr*>(this)->keySelf();
        case KFS_CHUNKINFO:
            return static_cast<const MetaChunkInfo*>(this)->keySelf();
        case KFS_DENTRY:
            return static_cast<const MetaDentry*>(this)->keySelf();
        default:
            panic("Meta::key: invalid node type");
    }
    return Key(KFS_UNINIT, -1);
}

std::ostream&
MetaNode::show(std::ostream& os) const
{
    switch (nodetype) {
        case KFS_INTERNAL:
            return static_cast<const Node*>(this)->showSelf(os);
        case KFS_FATTR:
            return static_cast<const MetaFattr*>(this)->showSelf(os);
        case KFS_CHUNKINFO:
            return static_cast<const MetaChunkInfo*>(this)->showSelf(os);
        case KFS_DENTRY:
            return static_cast<const MetaDentry*>(this)->showSelf(os);
        default:
            panic("Meta::show: invalid node type");
    }
    return os;
}

bool
Meta::match(const Meta* test) const
{
    switch (metaType()) {
        case KFS_FATTR:
            return static_cast<const MetaFattr*>(this)->matchSelf(test);
        case KFS_CHUNKINFO:
            return static_cast<const MetaChunkInfo*>(this)->matchSelf(test);
        case KFS_DENTRY:
            return static_cast<const MetaDentry*>(this)->matchSelf(test);
        default:
            panic("Meta::match: invalid node type");
    }
    return false;
}

} // namespace KFS
