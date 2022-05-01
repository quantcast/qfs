/*!
 * $Id$
 *
 * \file meta.cc
 * \brief Operations on the various metadata types.
 * \author Blake Lewis (Kosmix Corp.)
 *
 * Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
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
#include "LayoutManager.h"
#include "util.h"

#include "common/LinearHash.h"
#include "common/StringIo.h"

#include <iostream>
#include <fstream>
#include <sstream>

namespace KFS
{
using std::ostringstream;

inline ostream&
MetaDentry::showSelf(ostream& os) const
{
    return (os <<
    "d"
    "/n/" << name <<
    "/i/" << id() <<
    "/p/" << dir
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
    "a/"  << fname[type] <<
    "/i/" << id() <<
    "/c/" << (type == KFS_DIR ? 0 : chunkcount()) <<
    "/r/" << numReplicas <<
    "/m/" << ShowTime(mtime) <<
    "/c/" << ShowTime(ctime) <<
    "/C/" << ShowTime(atime) <<
    "/e/" << filesize;
    if (IsStriped()) {
        os <<
            "/s/" << striperType <<
            "/N/" << numStripes <<
            "/R/" << numRecoveryStripes <<
            "/S/" << stripeSize;
    }
    os <<
        "/u/" << user <<
        "/g/" << group <<
        "/M/" << mode;
    if (minSTier < kKfsSTierMax) {
        os <<
            "/t/" << (int)minSTier <<
            "/T/" << (int)maxSTier;
    }
    if (KFS_FILE == type && 0 == numReplicas) {
        os << "/o/" << nextChunkOffset();
    }
    if (HasExtAttrs()) {
        os <<
            "/a/" << fattrExtTypes <<
            "/"   << MakeEscapedStringInserter(GetExtAttributesSelf());
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
    os <<
    "c"
    "/i/" << id() <<
    "/c/" << chunkId <<
    "/o/" << offset <<
    "/v/" << chunkVersion <<
    "/s/";
    return gLayoutManager.Checkpoint(os, *this);
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

typedef KVPair<fid_t, string> MetaFattrExtEntry;
typedef LinearHash<
    MetaFattrExtEntry,
    KeyCompare<fid_t>,
    DynamicArray<
        SingleLinkedList<MetaFattrExtEntry>*,
        19 // 2^19 * sizeof(void*) => 4 MB
    >,
    PoolAllocatorAdapter<
        MetaFattrExtEntry,
        size_t(1)   << 20, // size_t TMinStorageAlloc,
        size_t(128) << 20, // size_t TMaxStorageAlloc,
        false              // bool   TForceCleanupFlag
    >
> MetaFattrExtAttributes;

static const MetaFattrExtAttributes* sMetaFattrExtAttributesForDebugPtr = 0;

static MetaFattrExtAttributes&
GetMetaFattrExtAttributesTable()
{
    static MetaFattrExtAttributes sMetaFattrExtAttributes;

    if (! sMetaFattrExtAttributesForDebugPtr) {
        sMetaFattrExtAttributesForDebugPtr = &sMetaFattrExtAttributes;
    }
    return sMetaFattrExtAttributes;
}

static const string&
GetEmptyString()
{
    static const string sEmptyString;
    return sEmptyString;
}

const string&
MetaFattr::GetExtAttributesSelf() const
{
    const string* const ret = GetMetaFattrExtAttributesTable().Find(id());
    return ret ? *ret : GetEmptyString();
}

void
MetaFattr::ExtAttributesClear()
{
    GetMetaFattrExtAttributesTable().Erase(id());
    fattrExtTypes = kFileAttrExtTypeNone;
}

void
MetaFattr::SetExtAttributes(FileAttrExtTypes types, const string& attrs)
{
    if (HasExtAttrs() && fattrExtTypes != types) {
        panic("not yet implemented");
    }
    if (attrs.empty()) {
        ExtAttributesClear();
    } else {
        bool insertedFlag = false;
        string* const val = GetMetaFattrExtAttributesTable().Insert(
                id(), attrs, insertedFlag);
        if (! insertedFlag)  {
            *val = attrs;
        }
        fattrExtTypes = types;
    }
}

void
MetaFattr::Init()
{
    GetMetaFattrExtAttributesTable();
    GetEmptyString();
}

} // namespace KFS
