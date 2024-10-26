/*!
 * $Id$
 *
 * \file kfsops.cc
 * \brief KFS file system operations.
 * \author Blake Lewis, Sriram Rao, Mike Ovsiannikov
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
#include "kfstypes.h"
#include "kfstree.h"
#include "util.h"
#include "LayoutManager.h"

#include <algorithm>
#include <functional>
#include "common/MsgLogger.h"
#include "common/time.h"
#include "kfsio/Globals.h"
#include <boost/bind.hpp>

namespace KFS
{

using boost::bind;
using std::for_each;
using std::lower_bound;
using std::set;
using std::max;
using std::make_pair;

inline void
Tree::FindChunk(int64_t chunkCount, fid_t fid, chunkOff_t pos,
    ChunkIterator& cit, MetaChunkInfo*& ci) const
{
    const int kLSearchThreshold = 32;
    if (kLSearchThreshold < chunkCount &&
            ci->offset + kLSearchThreshold * (chunkOff_t)CHUNKSIZE < pos) {
        int         kp;
        Node* const l = lowerBound(Key(KFS_CHUNKINFO, fid, pos), kp);
        cit = ChunkIterator(l, kp, fid);
        ci = cit.next();
    } else if (ci->offset < pos) {
        ci = cit.lowerBound(Key(KFS_CHUNKINFO, fid, pos));
    }
}

static inline time_t
TimeNow()
{
    return libkfsio::globalNetManager().Now();
}

// Linux style directory sticky bit handling: "restricted delete".
static inline bool
IsDeleteRestricted(const MetaFattr* parent, const MetaFattr* fa, kfsUid_t euser)
{
    return (euser != kKfsUserRoot &&
        parent->IsSticky() && (parent->user != euser && fa->user != euser));
}

/*!
 * \brief Make a dumpster directory into which we can rename busy
 * files.  When the file is non-busy, it is nuked from the dumpster.
 */
void
Tree::makeDumpsterDir()
{
    fid_t fid = 0;
    int const status = mkdir(ROOTFID, DUMPSTERDIR,
        kKfsUserRoot, kKfsGroupRoot, 0700,
        kKfsUserRoot, kKfsGroupRoot,
        &fid, 0, microseconds()
    );
    KFS_LOG_STREAM_DEBUG <<
        "mkdir: "   << DUMPSTERDIR <<
        " fid: "    << fid <<
        " / "       << getDumpsterDirIdSelf() <<
        " status: " << status <<
        " "         << ErrorCodeToString(status) <<
    KFS_LOG_EOM;
    ensureChunkDeleteQueueExists();
}

int
Tree::checkDumpsterExists()
{
    MetaFattr* fa;
    int status = lookup(
            ROOTFID, DUMPSTERDIR, kKfsUserRoot, kKfsGroupRoot, fa);
    if (status == 0 && fa->type == KFS_DIR) {
        return 0;
    }
    if (status == 0) {
        status = -ENOTDIR;
    }
    KFS_LOG_STREAM_ERROR <<  "/" << DUMPSTERDIR << ": " <<  status <<
    KFS_LOG_EOM;
    return status;
}

/*!
 * \brief check file name for legality
 *
 * Legal means nonempty and not containing any slashes.
 *
 * \param[in]   name to check
 * \return  true if name is legal
 */
static bool
legalname(const string& name)
{
    const size_t len = name.length();
    return (
        len > 0 &&
        len <= MAX_FILE_NAME_LENGTH &&
        name.find_first_of("/\n") == string::npos
    );
}

/*!
 * \brief see whether path is absolute
 */
static bool
absolute(const string& path)
{
    return (path[0] == '/');
}

/*!
 * \brief common code for create and mkdir
 * \param[in] dir   fid of parent directory
 * \param[in] fname name of object to be created
 * \param[in] type  file or directory
 * \param[in] myID  fid of new object
 * \param[in] numReplicas desired degree of replication for file
 *
 * Create a directory entry and file attributes for the new object.
 * But don't create attributes for "." and ".." links, since these
 * share the directory's attributes.
 */
int
Tree::link(fid_t dir, const string& fname, FileType type, fid_t myID,
        int16_t numReplicas,
        int32_t striperType, int32_t numStripes,
        int32_t numRecoveryStripes, int32_t stripeSize,
        kfsUid_t user, kfsGid_t group, kfsMode_t mode,
        MetaFattr* parent, MetaFattr** newFattr, int64_t mtime)
{
    assert(legalname(fname));
    MetaFattr* fattr;
    if (fname != kThisDir && fname != kParentDir) {
        fattr = MetaFattr::create(type, myID, numReplicas,
            user, group, mode, mtime);
        if (! fattr->SetStriped(striperType, numStripes,
                numRecoveryStripes, stripeSize)) {
            fattr->destroy();
            return -EINVAL;
        }
        assert(fattr->filesize == 0);
    } else {
        fattr = 0;
    }
    MetaDentry* const dentry = MetaDentry::create(dir, fname, myID,
        fattr ? fattr : parent);
    insert(dentry);
    if (fattr) {
        assert(parent);
        fattr->parent = parent;
        parent->mtime = mtime;
        insert(fattr);
    }
    if (newFattr) {
        *newFattr = fattr;
    }
    return 0;
}

/*!
 * \brief create a new file
 * \param[in] dir   file id of the parent directory
 * \param[in] fname file name
 * \param[out] newFid   id of new file
 * \param[in] numReplicas desired degree of replication for file
 * \param[in] exclusive  model the O_EXCL flag of open()
 *
 * \return      status code (zero on success)
 */
int
Tree::create(fid_t dir, const string& fname, fid_t *newFid,
    int16_t numReplicas, bool exclusive,
    int32_t striperType, int32_t numStripes,
    int32_t numRecoveryStripes, int32_t stripeSize,
    kfsUid_t user, kfsGid_t group, kfsMode_t mode,
    kfsUid_t euser, kfsGid_t egroup,
    MetaFattr** newFattr,
    int64_t mtime, bool const todumpster)
{
    if (!legalname(fname)) {
        KFS_LOG_STREAM_WARN << "Bad file name " << fname <<
        KFS_LOG_EOM;
        return -EINVAL;
    }

    if (numReplicas < 0) {
        KFS_LOG_STREAM_DEBUG << "Bad # of replicas (" <<
            numReplicas << ") for " << fname << KFS_LOG_EOM;
        return -EINVAL;
    }
    MetaFattr* const parent = getFattr(dir);
    if (! parent) {
        return -ENOENT;
    }
    if (parent->type != KFS_DIR) {
        return -ENOTDIR;
    }
    if (! parent->CanWrite(euser, egroup)) {
        return -EACCES;
    }
    if (mEnforceDumpsterRulesFlag && getDumpsterDirId() == dir) {
        KFS_LOG_STREAM_DEBUG <<
            fname << ": attempt to create file in dumpster denied" <<
        KFS_LOG_EOM;
        return -EPERM;
    }

    MetaFattr* fa     = 0;
    int        status = lookup(dir, fname, euser, egroup, fa);
    if (status != -ENOENT && status != 0) {
        return status;
    }
    if (fa) {
        if (KFS_FILE != fa->type) {
            return -EISDIR;
        }
        // Model O_EXECL behavior in create: if the file exists
        // and exclusive is specified, fail the create.
        if (exclusive) {
            return -EEXIST;
        }
        if (IsDeleteRestricted(parent, fa, euser)) {
            return -EPERM;
        }
        const int status = remove(dir, fname, "",
            euser, egroup, mtime, todumpster);
        if (status != 0) {
            assert(status == -EBUSY || todumpster);
            KFS_LOG_STREAM_ERROR << "remove failed: " <<
                " dir: "        << dir <<
                " file: "       << fname <<
                " status: "     << status <<
                " todumpster: " << todumpster <<
            KFS_LOG_EOM;
            return status;
        }
    }

    if (*newFid == 0) {
        *newFid = fileID.genid();
    }
    fa = 0;
    status = link(dir, fname, KFS_FILE, *newFid, numReplicas,
        striperType, numStripes, numRecoveryStripes, stripeSize,
        user, group, mode, parent, &fa, mtime);
    if (status != 0) {
        return status;
    }
    UpdateNumFiles(1);
    updateCounts(fa, 0, 1, 0);
    fa->minSTier = 0 == numReplicas ? parent->maxSTier : parent->minSTier;
    fa->maxSTier = parent->maxSTier;
    if (newFattr) {
        *newFattr = fa;
    }
    return status;
}

struct MetaDentrySt : public MetaDentry
{
    MetaDentrySt(fid_t parent, const string& fname, fid_t myID)
        : MetaDentry(parent, fname, myID, 0)
        {}
};
/*!
 * \brief common code for remove and rmdir
 * \param[in] dir   fid of parent directory
 * \param[in] fname name of item to be removed
 * \param[in] fa    attributes for removed item
 * \pamam[in] save_fa   don't delete attributes if true
 *
 * save_fa prevents multiple deletions when removing
 * the "." and ".." links to a directory.
 */
void
Tree::unlink(fid_t dir, const string& fname, MetaFattr *fa, bool save_fa)
{
    MetaDentrySt dentry(dir, fname, fa->id());
    const int status = del(&dentry);
    if (status != 0) {
        panic("unlink: entry delete failed");
    }
    if (! save_fa) {
        const int status = del(fa);
        if (status != 0) {
            panic("unlink: attribute delete failed");
        }
    }
}

void Tree::invalidatePathCache(const string& pathname, const string& name,
    const MetaFattr* fa, bool removeDirPrefixFlag)
{
    if (mPathToFidCache.empty()) {
        return;
    }
    string pn;
    if (! pathname.empty() && pn[0] == '/') {
        pn = pathname;
    }
    if (pn.empty()) {
        if (! fa->parent && fa->id() != ROOTFID) {
            panic("invalid file attribute");
            return;
        }
        pn = fa->parent ? getPathname(fa->parent) : string("/");
        if (! pn.empty()) {
            if (pn == "/") {
                pn =  pn + name;
            } else {
                pn =  pn + "/" + name;
            }
        }
    }
    if (pn.empty()) {
        mPathToFidCache.clear();
        return;
    }
    mPathToFidCache.erase(pn);
    if (! removeDirPrefixFlag || fa->type != KFS_DIR) {
        return;
    }
    if (*pn.rbegin() != '/') {
        pn += "/";
    }
    const size_t len     = pn.length();
    int          maxScan = 1 << 10;
    for (PathToFidCacheMap::iterator it = mPathToFidCache.lower_bound(pn);
            it != mPathToFidCache.end();
            ) {
        const string& cp = it->first;
        if (cp.length() < len || cp.compare(0, len, pn) != 0) {
            break;
        }
        if (--maxScan < 0) {
            mPathToFidCache.clear();
            break;
        }
        mPathToFidCache.erase(it++);
    }
}

void
Tree::setFileSize(MetaFattr* fa, chunkOff_t size, int64_t nfiles, int64_t ndirs)
{
    if (fa->filesize < 0 && KFS_FILE != fa->type) {
        panic("invalid size attribute");
        return;
    }
    if (size < 0) {
        panic("invalid size");
        return;
    }
    updateCounts(fa, size - getFileSize(fa), nfiles, ndirs);
    fa->filesize = size;
}

int
Tree::removeFromDumpster(fid_t fid, const string& name, int64_t mtime,
    int entriesCount, bool& outCleanupDoneFlag)
{
    outCleanupDoneFlag = false;
    const fid_t dir    = getDumpsterDirId();
    MetaFattr*  fa     = 0;
    MetaFattr*  parent = 0;
    const int   status = lookup(
        dir, name, kKfsUserRoot, kKfsGroupRoot, fa, &parent);
    if (0 != status) {
        return status;
    }
    if (KFS_FILE != fa->type || fa->id() != fid ||
            mChunksDeleteQueueFattr == fa) {
        return -EINVAL;
    }
    if (0 < fa->chunkcount()) {
        StTmp<vector<MetaChunkInfo*> > cinfoTmp(mChunkInfosTmp);
        vector<MetaChunkInfo*>&        chunkInfo = cinfoTmp.Get();
        if (0 < entriesCount && entriesCount < fa->chunkcount()) {
            MetaFattr* ffa = 0;
            const int  status = getalloc(fid, ffa, chunkInfo, entriesCount);
            if (0 != status) {
                return status;
            }
            if (ffa != fa) {
                panic("remove from dumpster get alloc failure");
                return -EFAULT;
            }
        } else {
            getalloc(fid, chunkInfo);
            assert(fa->chunkcount() == (int64_t)chunkInfo.size());
        }
        const int64_t cnt = (int64_t)chunkInfo.size();
        fa->chunkcount() -= cnt;
        UpdateNumChunks(-cnt);
        // fire-away...
        for_each(chunkInfo.begin(), chunkInfo.end(),
             bind(&MetaChunkInfo::DeleteChunk, _1));
        if (0 < fa->chunkcount()) {
            // Update modification time and set file size to 0 in order
            // to signal not to start recovery, not report file as abandoned,
            // prevent size update attempts. Clear permissions in order to turn
            // off non root access, and mark file as being deleted.
            fa->mtime = mtime;
            fa->mode &= ~kfsMode_t(MetaFattr::kFileModeMask);
            if (0 < getFileSize(fa)) {
                setFileSize(fa, 0);
            }
            return 0;
        }
    } else if (0 == fa->numReplicas) {
        gLayoutManager.DeleteFile(*fa);
    }
    UpdateNumFiles(-1);
    parent->mtime = mtime;
    setFileSize(fa, 0, -1, 0);
    unlink(dir, name, fa, false);
    outCleanupDoneFlag = true;
    return 0;
}

/*!
 * \brief remove a file
 * \param[in] dir   file id of the parent directory
 * \param[in] fname file name
 * \return      status code (zero on success)
 */
int
Tree::remove(fid_t dir, const string& fname, const string& pathname,
    kfsUid_t euser, kfsGid_t egroup, int64_t mtime,
    bool const todumpster)
{
    MetaFattr* fa     = 0;
    MetaFattr* parent = 0;
    const int  status = lookup(dir, fname, euser, egroup, fa, &parent);
    if (status != 0) {
        return status;
    }
    if (! fa || ! parent) {
        panic("remove: null file or parent attribute");
        return -EFAULT;
    }
    if (KFS_FILE != fa->type) {
        return -EISDIR;
    }
    if (! parent->CanWrite(euser, egroup)) {
        return -EACCES;
    }
    if (IsDeleteRestricted(parent, fa, euser)) {
        return -EPERM;
    }
    invalidatePathCache(pathname, fname, fa);
    if (fa->IsSymLink()) {
        if (0 != fa->chunkcount()) {
            panic("symbolic link with chunks");
        }
        if (0 != fa->filesize) {
            panic("symbolic link non zero size");
        }
        if (1 != fa->numReplicas) {
            panic("symbolic link replication other than 1");
        }
    } else if (todumpster) {
        // put the file into dumpster
        const int status = moveToDumpster(dir, fname, *fa, mtime);
        KFS_LOG_STREAM_DEBUG <<
            "move " << fname << " to dumpster" <<
            " fid: "    << fa->id() <<
            " status: " << status <<
        KFS_LOG_EOM;
        return status;
    }
    if (0 < fa->chunkcount()) {
        StTmp<vector<MetaChunkInfo*> > cinfoTmp(mChunkInfosTmp);
        vector<MetaChunkInfo*>&        chunkInfo = cinfoTmp.Get();
        getalloc(fa->id(), chunkInfo);
        assert(fa->chunkcount() == (int64_t)chunkInfo.size());
        UpdateNumChunks(-fa->chunkcount());
        // fire-away...
        for_each(chunkInfo.begin(), chunkInfo.end(),
             bind(&MetaChunkInfo::DeleteChunk, _1));
    } else if (0 == fa->numReplicas) {
        gLayoutManager.DeleteFile(*fa);
    }
    UpdateNumFiles(-1);
    parent->mtime = mtime;
    setFileSize(fa, 0, -1, 0);

    unlink(dir, fname, fa, false);
    return 0;
}

/*!
 * \brief create a new directory
 * \param[in] dir   file id of the parent directory
 * \param[in] dname name of new directory
 * \param[out] newFid   id of new directory
 * \return      status code (zero on success)
 */
int
Tree::mkdir(fid_t dir, const string& dname,
    kfsUid_t user, kfsGid_t group, kfsMode_t mode,
    kfsUid_t euser, kfsGid_t egroup,
    fid_t* newFid, MetaFattr** newFattr, int64_t mtime)
{
    if (! legalname(dname) && (dir != ROOTFID || dname != "/")) {
        return -EINVAL;
    }
    if (mEnforceDumpsterRulesFlag && getDumpsterDirId() == dir) {
        KFS_LOG_STREAM_DEBUG <<
            dname << ": attempt to create directory in dumpster denied" <<
        KFS_LOG_EOM;
        return -EPERM;
    }
    MetaFattr* const parent = getFattr(dir);
    if ((! parent || parent->type != KFS_DIR) &&
            (dir != ROOTFID && dname != "/")) {
        return -ENOTDIR;
    }
    if (getDentry(dir, dname)) {
        return -EEXIST;
    }
    if (parent && ! parent->CanWrite(euser, egroup)) {
        return -EACCES;
    }
    fid_t myID = *newFid;
    if (myID == 0) {
        myID = (dname == "/") ? dir : fileID.genid();
    }
    MetaFattr* const fattr  = MetaFattr::create(KFS_DIR, myID, 1,
        user, group, mode, mtime);
    MetaDentry* const dentry = MetaDentry::create(dir, dname, myID, fattr);
    fattr->parent = parent;
    int status;
    if ((status = insert(dentry))) {
        panic("mkdir dir entry insert failure");
        dentry->destroy();
        fattr->destroy();
        return status;
    }
    if ((status = insert(fattr))) {
        panic("mkdir dir entry insert failure");
        fattr->destroy();
        return status;
    }
    status = link(myID, kThisDir, KFS_DIR, myID, 1,
        KFS_STRIPED_FILE_TYPE_NONE, 0, 0, 0,
        kKfsUserNone, kKfsGroupNone, 0, fattr, 0, mtime);
    if (status != 0) {
        panic("mkdir link(.)");
        return status;
    }
    status = link(myID, kParentDir, KFS_DIR, dir, 1,
        KFS_STRIPED_FILE_TYPE_NONE, 0, 0, 0,
        kKfsUserNone, kKfsGroupNone, 0, parent, 0, mtime);
    if (status != 0) {
        panic("mkdir link(..)");
        return status;
    }
    updateCounts(fattr, 0, 0, 1);
    if (parent) {
        parent->mtime = mtime;
        fattr->minSTier = parent->minSTier;
        fattr->maxSTier = parent->maxSTier;
    }
    UpdateNumDirs(1);

    *newFid = myID;
    if (newFattr) {
        *newFattr = fattr;
    }
    return 0;
}

/*!
 * \brief check whether a directory is empty
 * \param[in] dir   file ID of the directory
 */
bool
Tree::emptydir(fid_t dir)
{
    const PartialMatch key(KFS_DENTRY, dir);
    int                p;
    const Node*        n = findLeaf(key, p);
    if (! n) {
        return false;
    }
    int kNumEmptyDirEntries = 2;
    int k = 0;
    while (n && key == n->getkey(p) && ++k <= kNumEmptyDirEntries) {
        if (++p == n->children()) {
            p = 0;
            n = n->peer();
        }
    }
    return (k == kNumEmptyDirEntries);
}

/*!
 * \brief remove a directory
 * \param[in] dir   file id of the parent directory
 * \param[in] dname name of directory
 * \param[in] pathname  fully qualified path to dname
 * \return      status code (zero on success)
 */
int
Tree::rmdir(fid_t dir, const string& dname, const string& pathname,
    kfsUid_t euser, kfsGid_t egroup, int64_t mtime)
{
    MetaFattr* fa     = 0;
    MetaFattr* parent = 0;
    const int  status = lookup(dir, dname, euser, egroup, fa, &parent);
    if (status != 0) {
        return status;
    }
    if (! fa || ! parent) {
        panic("mkdir: null file or parent attribute");
        return -EFAULT;
    }

    if (dir == ROOTFID && (dname == DUMPSTERDIR || dname == "/")) {
        KFS_LOG_STREAM_DEBUG << "attempt to delete: /" <<
            dname << KFS_LOG_EOM;
        return -EPERM;
    }
    if (dname == kThisDir) {
        return -EINVAL;
    }
    if (dname == kParentDir) {
        return -ENOTEMPTY;
    }
    if (fa->type != KFS_DIR) {
        return -ENOTDIR;
    }
    if (! parent->CanWrite(euser, egroup)) {
        return -EACCES;
    }
    if (IsDeleteRestricted(parent, fa, euser)) {
        return -EPERM;
    }
    const fid_t myID = fa->id();
    if (! emptydir(myID)) {
        return -ENOTEMPTY;
    }
    invalidatePathCache(pathname, dname, fa);
    UpdateNumDirs(-1);
    parent->mtime = mtime;
    setFileSize(fa, 0, 0, -1);
    unlink(myID, kThisDir, fa, true);
    unlink(myID, kParentDir, fa, true);
    unlink(dir, dname, fa, false);
    return 0;
}

/*!
 * \brief return attributes for the specified object
 * \param[in] fid   the object's file id
 * \return      pointer to the attributes
 */
MetaFattr *
Tree::getFattr(fid_t fid)
{
    const Key    fkey(KFS_FATTR, fid);
    int          p;
    Node * const l = findLeaf(fkey, p);
    return (l ? l->extractMeta<MetaFattr>(p) : 0);
}

MetaDentry *
Tree::getDentry(fid_t dir, const string& fname)
{
    const KeyData hash = MetaDentry::nameHash(fname);
    const Key     key(KFS_DENTRY, dir, hash);
    int           p;
    const Node*   n = findLeaf(key, p);
    if (! n) {
        return 0;
    }
    while (n && key == n->getkey(p)) {
        MetaDentry* const de = refine<MetaDentry>(n->leaf(p));
        if (de->getHash() == hash && de->getName() == fname) {
            return de;
        }
        if (++p == n->children()) {
            p = 0;
            n = n->peer();
        }
    }
    return 0;
}

/*
 * Do a depth first dir listing of the tree.  This can be useful for debugging
 * purposes.
 */
int
Tree::listPaths(ostream& ofs)
{
    return listPaths(ofs, set<fid_t>());
}

class PathLister
{
public:
    PathLister(ostream& os, const set<fid_t>& ids)
        : mOs(os),
          mFids(ids),
          mListAllFlag(mFids.empty()),
          mCount(0)
        {}
    bool operator()(const string&     dirpath,
            const MetaDentry& de,
            const MetaFattr&  fa,
            size_t            /* depth */)
    {
        if (fa.type == KFS_DIR) {
            if (! mListAllFlag) {
                return true;
            }
            mOs << dirpath <<
                " <dir> " << fa.id() <<
                ' ' << DisplayIsoDateTime(fa.mtime) <<
                "\n";
            mCount++;
            return true;
        }
        if (! mListAllFlag && mFids.find(fa.id()) == mFids.end()) {
            return true;
        }
        mOs << dirpath << de.getName() <<
            ' ' << fa.id() <<
            ' ' << fa.filesize <<
            ' ' << DisplayIsoDateTime(fa.mtime) <<
            "\n";
        mCount++;
                return true;
    }
    int getCount() const { return mCount; }
private:
    ostream&          mOs;
    const set<fid_t>& mFids;
    const bool        mListAllFlag;
    int               mCount;
private:
    PathLister(const PathLister&);
    PathLister& operator=(const PathLister&);
};

int
Tree::listPaths(ostream &ofs, const set<fid_t> specificIds)
{
    PathLister              lister(ofs, specificIds);
    PathListerT<PathLister> plister(lister);
    iterateDentries(plister);
    ofs.flush();
    ofs << '\n';
    return lister.getCount();
}

/*
 * For fast "du", we store the size of a directory tree in the Fattr for that
 * tree id.  This method should be called whenever the size values need to be
 * recomputed for accuracy.  This is an expensive operation: we have to traverse
 * from root to each leaf in the tree.  When recomputing the dir. size, we also
 * update the mtime to the root of the tree.
 */
void
Tree::recomputeDirSize()
{
    MetaFattr* fa     = 0;
    const int  status = lookup(
        ROOTFID, "/", kKfsUserRoot, kKfsGroupRoot, fa);
    if (status != 0) {
        return;
    }
    recomputeDirSize(fa);
}

/*
 * A simple depth first traversal of the directory tree starting at the root
 * @param[in] dirattr  The directory we are processing
 */
void
Tree::recomputeDirSize(MetaFattr* dirattr)
{
    dirattr->filesize    = 0;
    dirattr->dirCount()  = 0;
    dirattr->fileCount() = 0;
    const fid_t        dir = dirattr->id();
    const PartialMatch dkey(KFS_DENTRY, dir);
    int                kp;
    Node* const        l = findLeaf(dkey, kp);
    if (! l) {
        return;
    }
    LeafIter it(l, kp);
    for (Node* p;
            (p = it.parent()) && p->getkey(it.index()) == dkey;
            it.next()) {
        MetaDentry&   entry     = *refine<MetaDentry>(it.current());
        const string& entryname = entry.getName();
        if (entry.id() == dir ||
                entryname == kThisDir ||
                entryname == kParentDir) {
            MetaFattr* const fa = entry.id() == dir ?
                dirattr : dirattr->parent;
            if (! fa) {
                panic("invalid null parent dir entry "
                    "attribute pointer");
            }
            if (entry.getFattr() != fa) {
                if (entry.getFattr()) {
                    panic("invalid non null dir entry "
                        "attribute pointer");
                }
                entry.setFattr(fa);
            }
            continue;
        }
        MetaFattr* const fa = getFattr(entry.id());
        if (! fa) {
            continue;
        }
        if (! fa->parent) {
            fa->parent = dirattr;
            if (fa != entry.getFattr()) {
                if (entry.getFattr()) {
                    panic("invalid non null dir entry "
                        "attribute pointer");
                }
                entry.setFattr(fa);
            }
        } else if (fa->parent != dirattr) {
            panic("invalid parent pointer");
        } else if (fa != entry.getFattr()) {
            panic("invalid dir entry attribute pointer");
        }
        if (fa->type == KFS_DIR) {
            // Do a depth first traversal
            recomputeDirSize(fa);
            dirattr->filesize += fa->filesize;
            dirattr->dirCount()  += fa->dirCount() + 1;
            dirattr->fileCount() += fa->fileCount();
        } else {
            dirattr->filesize += getFileSize(fa);
            dirattr->fileCount()++;
        }
    }
}

/*
 * Given a dir, do a depth first traversal updating the replication count for
 * all files in the dir. tree to the specified value.
 * @param[in] dirattr  The directory we are processing
 */
int
Tree::changeDirReplication(MetaFattr* dirattr, int16_t numReplicas,
    kfsSTier_t minSTier, kfsSTier_t maxSTier)
{
    if (! dirattr || dirattr->type != KFS_DIR) {
        return -ENOTDIR;
    }
    fid_t dir = dirattr->id();
    StTmp<vector<MetaDentry*> > dentriesTmp(mDentriesTmp);
    vector<MetaDentry*>&        entries = dentriesTmp.Get();
    readdir(dir, entries);
    for (uint32_t i = 0; i < entries.size(); i++) {
        const string& entryname = entries[i]->getName();
        if (entryname == kThisDir || entryname == kParentDir ||
                entries[i]->id() == dir) {
            continue;
        }
        MetaFattr *fa = getFattr(entries[i]->id());
        if (! fa) {
            continue;
        }
        if (fa->type == KFS_DIR) {
            // Do a depth first traversal
            changeDirReplication(fa, numReplicas, minSTier, maxSTier);
            continue;
        }
        changeFileReplication(fa, numReplicas, minSTier, maxSTier);
    }
    return 0;
}

/*
 * Given a file-id, returns its fully qualified pathname.  This involves
 * recursively traversing the metatree until the root directory.
 */
string
Tree::getPathname(const MetaFattr* fa)
{
    if (! allowFidToPathConversion || ! fa) {
        return string();
    }
    string path;
    fid_t curid;
    for (const MetaFattr* parent = fa; ; ) {
        curid  = parent->id();
        parent = parent->parent;
        if (! parent) {
            break;
        }
        if (parent->type != KFS_DIR) {
            panic("invalid parent pointer");
            return string();
        }
        const PartialMatch dkey(KFS_DENTRY, parent->id());
        int                p;
        const Node*        n = findLeaf(dkey, p);
        if (! n) {
            // panic("unconnected node, or invalid parent");
            return string();
        }
        while (n && dkey == n->getkey(p)) {
            const MetaDentry* const de =
                refine<MetaDentry>(n->leaf(p));
            if (de->id() == curid) {
                const string& name = de->getName();
                if (name != kThisDir && name != kParentDir) {
                    if (path.empty()) {
                        path = name;
                    } else {
                        path = name + "/" + path;
                    }
                    break;
                }
            }
            if (++p == n->children()) {
                p = 0;
                n = n->peer();
            }
        }
    }
    if (curid != ROOTFID) {
        panic("parent pointer chain does end at root");
        return string();
    }
    return ("/" + path);
}

inline static bool
canAccess(Tree& tree, fid_t dir, MetaFattr& fa,
        kfsUid_t euser, kfsGid_t egroup, MetaFattr** outParent)
{
    if (euser == kKfsUserRoot && ! outParent) {
        return true;
    }
    MetaFattr* parent = fa.parent;
    if (! parent) {
        parent = (dir == ROOTFID && fa.id() == ROOTFID) ?
            &fa : tree.getFattr(dir);
        if (! parent) {
            panic("canAccess: no parent attribute");
            return false;
        }
    }
    if (outParent) {
        *outParent = parent;
    }
    return (euser == kKfsUserRoot || parent->CanSearch(euser, egroup));
}

/*!
 * \brief look up a file name and return its attributes
 * \param[in] dir   file id of the parent directory
 * \param[in] fname file name that we are looking up
 * \return      file attributes or NULL if not found
 */
int
Tree::lookup(fid_t dir, const string& fname,
    kfsUid_t euser, kfsGid_t egroup, MetaFattr*& fa, MetaFattr** outParent,
    MetaDentry** outDentry)
{
    MetaDentry* const d = getDentry(dir, fname);
    if (outDentry) {
        *outDentry = d;
    }
    if (! d) {
        if (outParent) {
            *outParent = 0;
        }
        return -ENOENT;
    }
    fa = d->getFattr();
    if (! fa) {
        fa = getFattr(d->id());
    }
    if (! fa) {
        panic("lookup: no directory attribute");
        return -EFAULT;
    }
    if (! canAccess(*this, dir, *fa, euser, egroup, outParent)) {
        fa = 0;
        return -EACCES;
    }
    return 0;
}

/*!
 * \brief repeatedly apply Tree::lookup to an entire path
 * \param[in] rootdir   file id of starting directory
 * \param[in] path  the path to look up
 * \return attributes of the last component (or NULL)
 */
int
Tree::lookupPath(fid_t rootdir, const string& path,
        kfsUid_t euser, kfsGid_t egroup, MetaFattr*& fa)
{
    fa = 0;
    if (path.empty()) {
        return -ENOENT;
    }
    const bool        isabs    = absolute(path);
    const fid_t       cdir     = (rootdir == 0 || isabs) ? ROOTFID : rootdir;
    string::size_type cstart   = isabs ? path.find_first_not_of('/', 1) : 0;
    bool              usecache =
        mIsPathToFidCacheEnabled && isabs && cstart == 1;

    if (cstart == string::npos) {
        return lookup(cdir, "/", euser, egroup, fa);
    }

    if (usecache) {
        PathToFidCacheMap::iterator const iter = mPathToFidCache.find(path);
        if (iter != mPathToFidCache.end()) {
            // NOTE: We use the fid to extract the fa
            // and validate that the fa matches. This works because
            // the fid isn't re-used.  This means that if the
            // file got deleted and the FA pointer got reused, we
            // won't find a match for the fid in the tree.
            // For now do not check access -- path cache should only
            // be used / enabled for sorter.
            if (getFattr(iter->second.fid) == iter->second.fa) {
                UpdatePathToFidCacheHit(1);
                iter->second.lastAccessTime = TimeNow();
                KFS_LOG_STREAM_DEBUG << "cache hit for " << path <<
                    "->" << iter->second.fid <<
                KFS_LOG_EOM;
                fa = iter->second.fa;
                return 0;
            }
            mPathToFidCache.erase(iter);
        }
    }

    fid_t             dir = cdir;
    string            component;
    string::size_type slash ;
    while ((slash = path.find('/', cstart)) != string::npos) {
        component.assign(path, cstart, slash - cstart);
        MetaDentry* const d = getDentry(dir, component);
        if (! d) {
            return -ENOENT;
        }
        MetaFattr* da = d->getFattr();
        if (! da && ! (da = getFattr(d->id()))) {
            panic("dentry with no attribute");
            return -EFAULT;
        }
        if (da->type != KFS_DIR) {
            return -ENOTDIR;
        }
        string::size_type const n = path.find_first_not_of('/', slash);
        if (n == string::npos) {
            // Trailing slash -- directory, do not cache.
            fa = da;
            return 0;
        }
        if (euser != kKfsUserRoot && ! da->CanSearch(euser, egroup)) {
            return -EACCES;
        }
        usecache = usecache && n == slash + 1 &&
            component != "." && component != "..";
        cstart = n;
        dir = d->id();
    }

    component.assign(path, cstart,
        (slash == string::npos ? path.size() : slash) - cstart);
    const int status = lookup(dir, component,
        cdir == dir ? euser  : kKfsUserRoot,
        cdir == dir ? egroup : kKfsGroupRoot, fa);
    if (usecache && status == 0 && fa) {
        UpdatePathToFidCacheMiss(1);
        if (mIsPathToFidCacheEnabled) {
            PathToFidCacheEntry fce;

            fce.fid = fa->id();
            fce.fa  = fa;
            fce.lastAccessTime = TimeNow();
            mPathToFidCache.insert(make_pair(path, fce));
        }
    }
    return status;
}

void
Tree::cleanupPathToFidCache(
    int64_t startTime)
{
    const time_t now = (time_t)startTime;

    if (now - mLastPathToFidCacheCleanupTime <
            (FID_CACHE_CLEANUP_INTERVAL + 1) / 2) {
        return;
    }
    mLastPathToFidCacheCleanupTime = now;
    PathToFidCacheMap::iterator iter = mPathToFidCache.begin();
    while (iter != mPathToFidCache.end()) {
        if (now <= iter->second.lastAccessTime +
                FID_CACHE_ENTRY_EXPIRE_INTERVAL) {
            iter++;
            continue;
        }
        KFS_LOG_STREAM_DEBUG << "Clearing out cache entry: " <<
            iter->first <<
        KFS_LOG_EOM;
        mPathToFidCache.erase(iter++);
    }
}

/*
 * At each level of the directory tree, we'd like to record the space used by
 * that subtree.  Then, on a stat of directory, we can provide "du" results for
 * the subtree.
 * To update space usage, start at the root and work down till the parent
 * directory where the file lives and update space used at each level by nbytes.
 */
void
Tree::updateCounts(MetaFattr* fa, chunkOff_t nbytes, int64_t nfiles, int64_t ndirs)
{
    if (! fa) {
        panic("invalid file attribute");
        return;
    }
    if (! mUpdatePathSpaceUsage ||
            (nbytes == 0 && nfiles == 0 && ndirs == 0)) {
        return;
    }
    for (MetaFattr* parent = fa; ; ) {
        if (! parent->parent) {
            if (parent->id() != ROOTFID) {
                panic("parent pointer chain does end at root",
                    false);
            }
            break;
        }
        parent = parent->parent;
        if (parent->type != KFS_DIR) {
            panic("invalid parent pointer");
        }
        parent->filesize    += nbytes;
        parent->fileCount() += nfiles;
        parent->dirCount()  += ndirs;
        if (parent->filesize < 0) {
            panic("invalid size delta");
            parent->filesize = 0;
        }
        if (parent->fileCount() < 0) {
            panic("invalid file delta");
            parent->fileCount() = 0;
        }
        if (parent->dirCount() < 0) {
            panic("invalid dir delta");
            parent->dirCount() = 0;
        }
    }
}

/*!
 * \brief read the contents of a directory
 * \param[in] dir   file id of directory
 * \param[out] v    vector of directory entries
 * \return      status code
 */
int
Tree::readdir(fid_t dir, vector<MetaDentry*>& v,
    int maxEntries /* = 0 */, bool* moreEntriesFlag /* = 0 */)
{
    if (moreEntriesFlag) {
        *moreEntriesFlag = false;
    }
    const PartialMatch dkey(KFS_DENTRY, dir);
    int                kp;
    Node* const        l = findLeaf(dkey, kp);
    if (! l) {
        return -ENOENT;
    }
    int      maxRet = maxEntries <= 0 ? -1 : maxEntries;
    LeafIter it(l, kp);
    Node*    p;
    while ((p = it.parent()) && p->getkey(it.index()) == dkey) {
        if (maxRet-- == 0) {
            if (moreEntriesFlag) {
                *moreEntriesFlag = true;
            }
            break;
        }
        v.push_back(refine<MetaDentry>(it.current()));
        it.next();
    }

    if (maxEntries != 1 && v.size() < 2) {
        panic("invalid directory");
        return -EINVAL;
    }
    return 0;
}

int
Tree::readdir(fid_t dir, const string& fnameStart,
        vector<MetaDentry*>& v, int maxEntries, bool& moreEntriesFlag)
{
    moreEntriesFlag = false;
    const KeyData hash = MetaDentry::nameHash(fnameStart);
    const Key     key(KFS_DENTRY, dir, hash);
    int           kp;
    Node* const   l = findLeaf(key, kp);
    if (! l) {
        return -ENOENT;
    }
    LeafIter it(l, kp);
    bool     foundFlag = false;
    Node*    p;
    while ((p = it.parent()) && p->getkey(it.index()) == key) {
        MetaDentry* const de = refine<MetaDentry>(it.current());
        if (de->getHash() == hash && de->getName() == fnameStart) {
            it.next();
            foundFlag = true;
            break;
        }
        it.next();
    }
    if (! foundFlag) {
        return -ENOENT;
    }
    const PartialMatch dkey(KFS_DENTRY, dir);
    int                maxRet = maxEntries <= 0 ? -1 : maxEntries;
    while ((p = it.parent()) && p->getkey(it.index()) == dkey) {
        if (maxRet-- == 0) {
            moreEntriesFlag = true;
            break;
        }
        v.push_back(refine<MetaDentry>(it.current()));
        it.next();
    }
    return 0;
}

/*!
 * \brief return a file's chunk information (if any)
 * \param[in] fid   file id for the file
 * \param[out] v    vector of MetaChunkInfo results
 * \return      status code
 */
int
Tree::getalloc(fid_t fid, vector<MetaChunkInfo*>& v)
{
    const PartialMatch ckey(KFS_CHUNKINFO, fid);
    int   kp;
    Node *l = findLeaf(ckey, kp);
    if (l) {
        extractAll(l, kp, ckey, v);
    }
    return 0;
}

/*!
 * \brief return a file's chunk information (if any)
 * \param[in] fid   file id for the file
 * \param[out] fa   file attribute
 * \param[out] v    vector of MetaChunkInfo results
 * \return      status code
 */
int
Tree::getalloc(fid_t fid, MetaFattr*& fa, vector<MetaChunkInfo*>& v, int maxChunks)
{
    const Key   fkey(KFS_FATTR, fid);
    int         kp;
    Node* const l = findLeaf(fkey, kp);
    if (! l) {
        fa = 0;
        return -ENOENT;
    }
    LeafIter it(l, kp);
    fa = refine<MetaFattr>(it.current());
    if (KFS_FILE != fa->type) {
        fa = 0;
        return -EISDIR;
    }
    if (fa->IsSymLink()) {
        fa = 0;
        return -ENXIO;
    }
    // Chunk attributes follow the file attribute: they have same fid, and
    // KFS_FATTR < KFS_CHUNKINFO
    int maxRet = max(0, maxChunks);
    it.next();
    const PartialMatch ckey(KFS_CHUNKINFO, fid);
    Node* p;
    while ((p = it.parent()) && p->getkey(it.index()) == ckey) {
        v.push_back(refine<MetaChunkInfo>(it.current()));
        if (--maxRet == 0) {
            break;
        }
        it.next();
    }
    return 0;
}

ChunkIterator
Tree::getAlloc(fid_t fid) const
{
    const PartialMatch key(KFS_CHUNKINFO, fid);
    int                kp;
    Node* const        l = findLeaf(key, kp);
    return ChunkIterator(l, kp, key);
}

ChunkIterator
Tree::getAlloc(fid_t fid, MetaFattr*& fa) const
{
    const Key   fkey(KFS_FATTR, fid);
    int         kp;
    Node* const l = findLeaf(fkey, kp);
    if (! l) {
        fa = 0;
        return ChunkIterator();
    }
    LeafIter it(l, kp);
    fa = refine<MetaFattr>(it.current());
    if (KFS_FILE != fa->type || fa->IsSymLink()) {
        fa = 0;
        return ChunkIterator();
    }
    // Chunk attributes follow the file attribute: they have same fid, and
    // KFS_FATTR < KFS_CHUNKINFO
    it.next();
    return ChunkIterator(it.parent(), it.index(), fid);
}

DentryIterator
Tree::readDir(fid_t dir) const
{
    const PartialMatch key(KFS_DENTRY, dir);
    int                kp;
    Node* const        l = findLeaf(key, kp);
    return DentryIterator(l, kp, key);
}

/*!
 * \brief return a file's chunk information (if any)
 * \param[in] fid     file id for the file
 * \param[in] offset      offset in the file
 * \param[out] fa     file attribute
 * \param[out] c      chunk attribute
 * \param[out] chunkBlock vector of MetaChunkInfo for striped file chunk block
 * \return        status code
 */
int
Tree::getalloc(fid_t fid, chunkOff_t& offset,
    MetaFattr*& fa, MetaChunkInfo*& c, vector<MetaChunkInfo*>* chunkBlock,
    chunkOff_t* chunkBlockStartPos)
{
    fa = 0;
    c  = 0;
    if (offset < 0 && offset != (chunkOff_t) -1) {
        return -EINVAL;
    }
    const Key fkey(KFS_FATTR, fid);
    int   kp;
    Node* n = findLeaf(fkey, kp);
    if (! n) {
        return -ENOENT;
    }
    LeafIter it(n, kp);
    fa = refine<MetaFattr>(it.current());
    if (KFS_FILE != fa->type) {
        fa = 0;
        return -EISDIR;
    }
    if (fa->IsSymLink()) {
        fa = 0;
        return -ENXIO;
    }
    if (offset < 0) {
        offset = fa->nextChunkOffset();
    }
    // Chunk attributes follow the file attribute: they have same fid, and
    // KFS_FATTR < KFS_CHUNKINFO
    it.next();
    ChunkIterator  cit(it.parent(), it.index(), fid);
    MetaChunkInfo* ci = cit.next();
    if (! ci) {
        return -ENOENT;
    }
    const chunkOff_t boundary  = chunkStartOffset(offset);
    const chunkOff_t bstart    = fa->ChunkPosToChunkBlkStartPos(boundary);
    const chunkOff_t bend      = bstart +
        fa->ChunkBlkSize() - (chunkOff_t)CHUNKSIZE;
    FindChunk(fa->chunkcount(), fid, chunkBlock ? bstart : boundary, cit, ci);
    while (ci && ci->offset <= bend) {
        if (ci->offset == boundary) {
            c = ci;
            if (! chunkBlock || ! fa->IsStriped()) {
                break;
            }
        }
        if (chunkBlock) {
            chunkBlock->push_back(ci);
        }
        if (ci->offset == bend) {
            break;
        }
        ci = cit.next();
    }
    if (chunkBlockStartPos) {
        *chunkBlockStartPos = bstart;
    }
    return (c ? 0 : -ENOENT);
}

/*!
 * \brief return a file's last chunk information (if any)
 * \param[in] file    file id for the file
 * \param[out] fa     file attribute
 * \param[out] c      chunk attribute
 * \return        status code
 */
int
Tree::getLastChunkInfo(fid_t fid, MetaFattr*& fa, MetaChunkInfo*& c)
{
    fa = 0;
    ChunkIterator cit = getAlloc(fid, fa);
    c = cit.next();
    if (! fa) {
        return -ENOENT;
    }
    if (! c) {
        return 0;
    }
    if (8 < fa->chunkcount() &&
            c->offset + 8 * (chunkOff_t)CHUNKSIZE < fa->nextChunkOffset()) {
        int         kp;
        Node* const l = lowerBound(
            Key(KFS_CHUNKINFO, fid, fa->nextChunkOffset() - CHUNKSIZE), kp);
        if (l) {
            cit = ChunkIterator(l, kp, fid);
        }
    }
    MetaChunkInfo* ci;
    while ((ci = cit.next())) {
        c = ci;
    }
    return 0;
}

/*!
 * \brief return chunk information from a file starting at offset
 * \param[in] file  file id for the file
 * \param[in] offset    offset in the file
 * \param[out] v    vector of MetaChunkInfo results
 * \param[in] maxChunks max number of chunks to return, <= 0 -- no limit
 * \return      status code
 */
int
Tree::getalloc(fid_t fid, chunkOff_t offset, vector<MetaChunkInfo*>& v, int maxChunks)
{
    int   kp;
    Node* l = lowerBound(Key(KFS_CHUNKINFO, fid, chunkStartOffset(offset)), kp);
    if (! l) {
        return -ENOENT;
    }
    int            ret    = -ENOENT;
    int            maxRet = max(0, maxChunks);
    ChunkIterator  cit(l, kp, fid);
    MetaChunkInfo* ci;
    while ((ci = cit.next())) {
        ret = 0;
        v.push_back(ci);
        if (--maxRet == 0) {
            break;
        }
    }
    return ret;
}

/*!
 * \brief return the specific chunk information from a file
 * \param[in] file  file id for the file
 * \param[in] offset    offset in the file
 * \param[out] c    MetaChunkInfo
 * \return      status code
 */
int
Tree::getalloc(fid_t fid, chunkOff_t offset, MetaChunkInfo **c)
{
    // Allocation information is stored for offset's in the file that
    // correspond to chunk boundaries.
    int         kp;
    Node* const l = findLeaf(
        Key(KFS_CHUNKINFO, fid, chunkStartOffset(offset)), kp);
    if (! l) {
        return -ENOENT;
    }
    *c = l->extractMeta<MetaChunkInfo>(kp);
    return 0;
}

/*!
 * \brief allocate a chunk id for a file.
 * \param[in] file  file id for the file
 * \param[in] offset    offset in the file
 * \param[out] chunkId  chunkId that is (pre) allocated.  Allocation
 * is a two-step process: we grab a chunkId and then try to place the
 * chunk on a chunkserver; only when placement succeeds can the
 * chunkId be assigned to the file.  This function does the part of
 * grabbing the chunkId.
 * \param[out] chunkVersion  The version # assigned to the chunk
 * \return      status code
 */
int
Tree::allocateChunkId(fid_t file, chunkOff_t& offset, chunkId_t* chunkId,
    seq_t *chunkVersion, int16_t *numReplicas, bool *stripedFileFlag,
    vector<MetaChunkInfo*>* chunkBlock      /* = 0 */,
    chunkOff_t*             chunkBlockStart /* = 0 */,
    kfsUid_t                euser           /* = kKfsUserRoot */,
    kfsGid_t                egroup          /* = kKfsGroupRoot */,
    MetaFattr**             ofa             /* = 0 */)
{
    if (offset > 0 && (offset % CHUNKSIZE) != 0) {
        return -EINVAL;
    }
    MetaFattr*     lfa;
    MetaFattr*&    fa = ofa ? *ofa : lfa;
    MetaChunkInfo* ci = 0;
    fa = 0;
    const int res = getalloc(
        file, offset, fa, ci, chunkBlock, chunkBlockStart);
    if (! fa || (res != 0 && res != -ENOENT)) {
        return res;
    }
    if (fa->IsSymLink()) {
        panic("symbolic link: invalid getalloc return code");
        return -ENXIO;
    }
    if (! fa->CanWrite(euser, egroup)) {
        return -EACCES;
    }
    if (numReplicas) {
        *numReplicas = fa->numReplicas;
    }
    if (stripedFileFlag) {
        *stripedFileFlag = fa->IsStriped();
    }
    if (0 == fa->numReplicas) {
        if (-ENOENT != res || ci) {
            panic("chunk entry in object sotre file");
            return -EFAULT;
        }
        if (0 != *chunkId && file != *chunkId) {
            return -EINVAL; // Prevent chunk assignment in replay.
        }
        *chunkId      = file;
        *chunkVersion = chunkStartOffset(offset) + fa->maxSTier;
        if (offset < fa->nextChunkOffset()) {
            return -EEXIST;
        }
        return 0;
    }
    // check if an id has already been assigned to this offset
    if (res != -ENOENT && ci) {
        *chunkId      = ci->chunkId;
        *chunkVersion = ci->chunkVersion;
        return -EEXIST;
    }

    // during replay chunkId will be non-zero.  In such cases,
    // don't do new allocation.
    if (*chunkId == 0) {
        *chunkId      = chunkID.genid();
        *chunkVersion = 1;
    }
    return 0;
}

/*!
 * \brief update the metatree to link an allocated a chunk id with
 * its associated file.
 * \param[in] file  file id for the file
 * \param[in] offset    offset in the file
 * \param[in] chunkId   chunkId that is (pre) allocated.  Allocation
 * is a two-step process: we grab a chunkId and then try to place the
 * chunk on a chunkserver; only when placement succeeds can the
 * chunkId be assigned to the file.  This function does the part of
 * assinging the chunkId to the file.
 * \param[in] chunkVersion chunkVersion that is (pre) assigned.
 * \return      status code
 */
int
Tree::assignChunkId(fid_t file, chunkOff_t offset,
    chunkId_t chunkId, seq_t chunkVersion, int64_t mtime,
    chunkOff_t* appendOffset, chunkId_t* curChunkId, bool appendReplayFlag,
    const MetaFattr** outFa)
{
    MetaFattr * const fa = getFattr(file);
    if (outFa) {
        *outFa = fa;
    }
    if (! fa) {
        return -ENOENT;
    }
    chunkOff_t boundary = chunkStartOffset(offset);
    if (0 != fa->numReplicas) {
        // check if an id has already been assigned to this chunk
        const Key    ckey(KFS_CHUNKINFO, file, boundary);
        int          kp;
        Node * const l = findLeaf(ckey, kp);
        if (l) {
            if (! appendOffset) {
                MetaChunkInfo* const c = l->extractMeta<MetaChunkInfo>(kp);
                if (curChunkId) {
                    *curChunkId = c->chunkId;
                }
                if (c->chunkId != chunkId || c->chunkVersion == chunkVersion) {
                    return -EEXIST;
                }
                gLayoutManager.SetChunkVersion(*c, chunkVersion);
                // c->chunkVersion = chunkVersion;
                if (appendReplayFlag && ! fa->IsStriped()) {
                    const chunkOff_t size = max(
                        fa->nextChunkOffset(), boundary +
                        chunkOff_t(CHUNKSIZE));
                    if (fa->filesize < size) {
                        setFileSize(fa, size);
                    }
                } else if (boundary + chunkOff_t(CHUNKSIZE) >=
                            fa->nextChunkOffset() &&
                        ! fa->IsStriped() &&
                        fa->filesize >= 0) {
                    invalidateFileSize(fa);
                }
                fa->mtime = mtime;
                return 0;
            }
            boundary      = fa->nextChunkOffset();
            *appendOffset = boundary;
        }

        bool newEntryFlag = false;
        MetaChunkInfo* const m = gLayoutManager.AddChunkToServerMapping(
                fa, boundary, chunkId, chunkVersion, newEntryFlag);
        if (! m || ! newEntryFlag) {
            panic("duplicate chunk mapping");
            return -EFAULT;
        }
        if (insert(m)) {
            // insert failed
            m->destroy();
            panic("assignChunk");
            return -EFAULT;
        }
    }
    // insert succeeded; so, bump the chunkcount.
    if (0 != fa->numReplicas) {
        fa->chunkcount()++;
    }
    if (boundary >= fa->nextChunkOffset()) {
        if (0 != fa->numReplicas && ! fa->IsStriped() && 0 <= fa->filesize &&
                ! appendOffset && ! appendReplayFlag) {
            // We will know the size of the file only when the write to
            // this chunk is finished. Invalidate the size now.
            invalidateFileSize(fa);
        }
        fa->nextChunkOffset() = boundary + CHUNKSIZE;
    }
    if ((appendReplayFlag || appendOffset) && ! fa->IsStriped()) {
        const chunkOff_t size = fa->nextChunkOffset();
        if (fa->filesize < size) {
            setFileSize(fa, size);
        }
    }
    if (0 != fa->numReplicas) {
        UpdateNumChunks(1);
    }
    fa->mtime = mtime;
    if (curChunkId) {
        *curChunkId = chunkId;
    }
    return 0;
}

int
Tree::coalesceBlocks(const string& srcPath, const string& dstPath,
    fid_t& srcFid, fid_t& dstFid, chunkOff_t& dstStartOffset,
    const int64_t mtime, size_t& numChunksMoved,
    kfsUid_t euser, kfsGid_t egroup)
{
    MetaFattr* src = 0;
    int status = 0;
    if (srcPath.empty()) {
        if (! (src = getFattr(srcFid))) {
            return -ENOENT;
        }
    } else {
        if ((status = lookupPath(ROOTFID, srcPath, euser, egroup, src)) != 0) {
            return status;
        }
    }
    MetaFattr* dst = 0;
    if (dstPath.empty()) {
        if (! (dst = getFattr(dstFid))) {
            return -ENOENT;
        }
    } else {
        if ((status = lookupPath(ROOTFID, dstPath, euser, egroup, dst)) != 0) {
            return status;
        }
    }
    return coalesceBlocks(
        src, dst,
        srcFid, dstFid, dstStartOffset, mtime, numChunksMoved,
        euser, egroup
    );
}

int
Tree::coalesceBlocks(MetaFattr* srcFa, MetaFattr* dstFa,
    fid_t &srcFid, fid_t &dstFid, chunkOff_t &dstStartOffset,
    const int64_t mtime, size_t& numChunksMoved,
    kfsUid_t euser, kfsGid_t egroup)
{
    numChunksMoved = 0;
    if (! srcFa || ! dstFa) {
        return -ENOENT;
    }
    if (srcFa == dstFa) {
        return -EINVAL;
    }
    if (KFS_FILE != srcFa->type || KFS_FILE != dstFa->type) {
        return -EISDIR;
    }
    if (srcFa->IsSymLink() || dstFa->IsSymLink()) {
        return -ENXIO;
    }
    if (! srcFa->CanWrite(euser, egroup) ||
            ! dstFa->CanWrite(euser, egroup)) {
        return -EACCES;
    }
    if (0 == srcFa->numReplicas || 0 == dstFa->numReplicas) {
        return -ENOSYS;
    }
    // If files are striped, both have to have the same stripe parameters,
    // and the last chunk blocks should be complete.
    if ((srcFa->IsStriped() || dstFa->IsStriped()) && (
            srcFa->striperType != dstFa->striperType ||
            srcFa->numStripes != dstFa->numStripes ||
            srcFa->numRecoveryStripes != dstFa->numRecoveryStripes ||
            srcFa->stripeSize != dstFa->stripeSize ||
            (srcFa->chunkcount() > 0 && srcFa->filesize < 0) ||
            (dstFa->chunkcount() > 0 && dstFa->filesize < 0) ||
            (srcFa->ChunkPosToChunkBlkStartPos(
                    srcFa->nextChunkOffset()) !=
                srcFa->nextChunkOffset() ||
                dstFa->ChunkPosToChunkBlkStartPos(
                    dstFa->nextChunkOffset()) !=
                dstFa->nextChunkOffset())
            )) {
        return -EINVAL;
    }
    StTmp<vector<MetaChunkInfo*> > cinfoTmp(mChunkInfosTmp);
    vector<MetaChunkInfo*>&        chunkInfo = cinfoTmp.Get();
    getalloc(srcFa->id(), chunkInfo);
    srcFid = srcFa->id();
    dstFid = dstFa->id();
    const chunkOff_t dstStartPos = dstFa->nextChunkOffset();
    if (! chunkInfo.empty()) {
        // Flush the fid cache.
        const int status = gLayoutManager.ChangeChunkFid(srcFa, dstFa, 0);
        if (0 != status) {
            return status;
        }
    }
    for (vector<MetaChunkInfo*>::const_iterator it = chunkInfo.begin();
            it !=  chunkInfo.end();
            ++it) {
            const chunkOff_t boundary = chunkStartOffset(
            dstStartPos + (*it)->offset);
#ifdef COALESCE_BLOCKS_DEBUG
        int kp;
        assert(! findLeaf(Key(KFS_CHUNKINFO, dstFa->id(), boundary)), kp);
#endif
        gLayoutManager.ChangeChunkFid(srcFa, dstFa, *it);
        // ChangeChunkFid() ensures that *it remains valid after the
        // following del
        if (del(*it)) {
            panic("coalesce block failed to delete chunk");
        }
        (*it)->offset = boundary;
        if (insert(*it)) {
            (*it)->destroy();
            panic("coalesce block failed to insert chunk");
        }
#ifdef COALESCE_BLOCKS_DEBUG
        assert(findLeaf(Key(KFS_CHUNKINFO, dstFa->id(), boundary)), kp);
#endif
        if (boundary >= dstFa->nextChunkOffset()) {
            dstFa->nextChunkOffset() = boundary + CHUNKSIZE;
        }
        dstFa->chunkcount()++;
        numChunksMoved++;
    }
    if (numChunksMoved > 0) {
        gLayoutManager.ChangeChunkFid(0, 0, 0);
    }
    dstStartOffset = dstFa->IsStriped() ?
        dstFa->ChunkPosToChunkBlkFileStartPos(dstStartPos) : dstStartPos;
    // Update file size if needed. The file size includes "holes":
    if (dstFa->nextChunkOffset() > dstStartPos) {
        setFileSize(dstFa, dstStartOffset + getFileSize(srcFa));
        if (srcFa->filesize < 0) {
            invalidateFileSize(dstFa);
        }
    }
#ifdef COALESCE_BLOCKS_DEBUG
    chunkInfo.clear();
    getalloc(dstFa->id(), chunkInfo);
    assert(dstFa->chunkcount() == chunkInfo.size());
    chunkInfo.clear();
    getalloc(srcFa->id(), chunkInfo);
    assert(chunkInfo.empty());
#endif
    srcFa->nextChunkOffset() = 0;
    srcFa->chunkcount() = 0;
    setFileSize(srcFa, 0);
    srcFa->mtime = mtime;
    dstFa->mtime = mtime;
    return 0;
}

/*
 * During a file truncation, blks from a specified offset to the end of the file
 * are deleted.  In contrast, this operation does the opposite---delete blks from
 * the head of the file to the specified offset.
 */
int
Tree::pruneFromHead(fid_t file, chunkOff_t offset, const int64_t mtime,
    kfsUid_t euser, kfsGid_t egroup, int maxDeleteCount, int maxQueueCount,
    string* statusMsg)
{
    if (offset < 0) {
        return -EINVAL;
    }
    const bool kSetEofHintFlag = false;
    return truncate(file, 0, mtime, euser, egroup,
        chunkStartOffset(offset), kSetEofHintFlag, maxDeleteCount,
        maxQueueCount, statusMsg);
}

int
Tree::truncate(fid_t file, chunkOff_t offset, const int64_t mtime,
    kfsUid_t euser, kfsGid_t egroup, chunkOff_t endOffset, bool setEofHintFlag,
    int maxChunkDelete, int maxQueueCount, string* statusMsg)
{
    if (endOffset >= 0 &&
            (endOffset < offset || endOffset % CHUNKSIZE != 0)) {
        return -EINVAL;
    }

    MetaFattr*       fa         = 0;
    const chunkOff_t lco        = chunkStartOffset(offset);
    MetaChunkInfo*   ci         = 0;
    const bool       searchFlag = (! setEofHintFlag || endOffset >= 0) &&
        (chunkOff_t)CHUNKSIZE < offset;
    ChunkIterator    cit;
    if (searchFlag) {
        int   kp;
        Node* const n = lowerBound(Key(KFS_CHUNKINFO, file, lco), kp);
        cit = ChunkIterator(n, kp, file);
        ci  = cit.next();
        if (ci) {
            fa = ci->getFattr();
        }
    }
    if (! fa) {
        cit = getAlloc(file, fa);
    }

    if (! fa) {
        return -ENOENT;
    }
    if (KFS_FILE != fa->type) {
        return -EISDIR;
    }
    if (fa->IsSymLink()) {
        return -ENXIO;
    }
    if (fa == mChunksDeleteQueueFattr) {
        return -EPERM;
    }
    if (offset < 0) {
        return -EINVAL;
    }
    if (! fa->CanWrite(euser, egroup)) {
        return -EACCES;
    }
    if (fa->filesize == offset &&
            (0 < offset || ! fa->IsStriped() || fa->chunkcount() <= 0)) {
        return 0;
    }
    if (mEnforceDumpsterRulesFlag &&
            0 < fa->numReplicas &&
            0 == fa->filesize &&
            0 == (fa->mode & MetaFattr::kFileModeMask) &&
            fa->parent &&
            fa->parent->id() == getDumpsterDirId()) {
        return -EPERM;
    }
    if (0 == fa->numReplicas) {
        if (0 <= endOffset ||
                (0 < offset && fa->nextChunkOffset() <= 0) ||
                fa->FilePosToChunkBlkIndex(offset - 1) !=
                fa->LastChunkBlkIndex() ||
                offset < fa->filesize) {
            // Truncate is not supported with object store files.
            // Only setting logic EOF is allowed.
            return -ENOSYS;
        }
        setFileSize(fa, offset);
        fa->mtime = mtime;
        return 0;
    }
    if (fa->IsStriped() && (0 < offset || 0 <= endOffset)) {
        // For now do not allow truncation of striped files, and do not
        // allow to create trailing hole.
        // Use truncate only to set the logical eof.
        // The eof should always be in the the last block.
        if (endOffset >= 0 || offset < fa->filesize ||
                fa->FilePosToChunkBlkIndex(offset - 1) !=
                fa->LastChunkBlkIndex()) {
            return -ENOSYS;
        }
        setFileSize(fa, offset);
        fa->mtime = mtime;
        gLayoutManager.UpdateDelayedRecovery(*fa);
        return 0;
    }
    if (! searchFlag && (ci = cit.next())) {
        FindChunk(fa->chunkcount(), file, lco, cit, ci);
    }
    if (ci && ci->offset < offset && offset < fa->filesize) {
        // For now do not support chunk truncation in meta server.
        // Probably the simplest way to implement this is to do this in
        // the client using standard write protocol: get write lease
        // then truncate the chunk. The lease relinquish will update the
        // file size.
        //
        // Issuing the truncate command here doesn't guarantee that
        // all the chunk servers will ever receive this request. With
        // no chunk version change it isn't possible to detect lost
        // truncate.
        // Lost truncate creates a problem the case where the truncate
        // reduces the file size then increases it, effectively
        // creating a hole (or changing hole boundaries). The hole has
        // be zero filled, it can not contain the previous / stale data.
        return -EACCES;
    }
    StTmp<vector<MetaChunkInfo*> > cinfoTmp(mChunkInfosTmp);
    vector<MetaChunkInfo*>&        chunkInfo = cinfoTmp.Get();
    int64_t                        rem       = 0 <= maxQueueCount ?
        int64_t(max(0, maxChunkDelete)) + maxQueueCount : fa->chunkcount() + 1;
    while (ci && (endOffset < 0 || ci->offset < endOffset)) {
        if (--rem < 0) {
            break;
        }
        chunkInfo.push_back(ci);
        ci = cit.next();
    }
    if (rem < 0) {
        if (statusMsg) {
            *statusMsg = "exceeded truncate blocks limit";
        }
        return -EPERM;
    }
    // Delete chunks.
    int cnt = maxChunkDelete;
    while (! chunkInfo.empty()) {
        if (0 <= cnt && --cnt < 0) {
            break;
        }
        chunkInfo.back()->DeleteChunk();
        chunkInfo.pop_back();
        fa->chunkcount()--;
        UpdateNumChunks(-1);
    }
    if (! chunkInfo.empty()) {
        ensureChunkDeleteQueueExists();
        const int status = gLayoutManager.ChangeChunkFid(
            fa, mChunksDeleteQueueFattr, 0);
        if (0 != status) {
            panic("change chunk fid failure");
            return status;
        }
        for (vector<MetaChunkInfo*>::const_iterator it = chunkInfo.begin();
                it !=  chunkInfo.end();
                ++it) {
            gLayoutManager.ChangeChunkFid(fa, mChunksDeleteQueueFattr, *it);
            // ChangeChunkFid() ensures that *it remains valid after the
            // following del
            if (del(*it)) {
                panic("truncate failed to delete chunk");
            }
            (*it)->offset = mChunksDeleteQueueFattr->nextChunkOffset();
            if (insert(*it)) {
                (*it)->destroy();
                panic("truncate failed to insert chunk");
            }
            mChunksDeleteQueueFattr->nextChunkOffset() += CHUNKSIZE;
            fa->chunkcount()--;
            mChunksDeleteQueueFattr->chunkcount()++;
        }
        gLayoutManager.ChangeChunkFid(0, 0, 0);
        mChunksDeleteQueueFattr->mtime = mtime;
    }
    if (endOffset < 0) {
        if (lco < offset) {
            fa->nextChunkOffset() = lco + CHUNKSIZE;
        } else {
            fa->nextChunkOffset() = lco;
        }
        setFileSize(fa, offset);
    } else {
        if (fa->filesize < endOffset) {
            // endOffset % CHUNKSIZE == 0 see the above.
            fa->nextChunkOffset() = endOffset;
            setFileSize(fa, endOffset);
        }
    }
    fa->mtime = mtime;
    return 0;
}

int
Tree::cleanupChunks(int maxChunkDelete, int64_t mtime)
{
    if (! getChunkDeleteQueue() || maxChunkDelete <= 0) {
        return 0;
    }
    StTmp<vector<MetaChunkInfo*> > cinfoTmp(mChunkInfosTmp);
    vector<MetaChunkInfo*>&        chunkInfo = cinfoTmp.Get();
    MetaFattr*                     ffa       = 0;
    const int status = getalloc(
        mChunksDeleteQueueFattr->id(), ffa, chunkInfo, maxChunkDelete);
    if (0 != status) {
        panic("cleanup chunks get alloc failure");
        return status;
    }
    if (ffa != mChunksDeleteQueueFattr) {
        panic("cleanup chunks get alloc file attribute mismatch");
        return -EFAULT;
    }
    // Delete chunks.
    while (! chunkInfo.empty()) {
        chunkInfo.back()->DeleteChunk();
        chunkInfo.pop_back();
        mChunksDeleteQueueFattr->chunkcount()--;
        UpdateNumChunks(-1);
    }
    mChunksDeleteQueueFattr->mtime = mtime;
    if (mChunksDeleteQueueFattr->chunkcount() <= 0) {
        mChunksDeleteQueueFattr->nextChunkOffset() = 0;
        if (mChunksDeleteQueueFattr->chunkcount() < 0) {
            panic("cleanup chunks file attribute invalid chunk count");
            return -EFAULT;
        }
    }
    return 0;
}

/*!
 * \brief check whether one directory is a descendant of another
 * \param[in] src file ID of possible ancestor
 * \param[in] dst file ID of possible descendant
 *
 * Check dst and each of its ancestors to see whether src is
 * among them; used to avoid making a directory into its own
 * child via rename.
 */
bool
Tree::is_descendant(fid_t src, fid_t dst, const MetaFattr* dstFa)
{
    const MetaFattr* dotdot = dstFa;
    while (src != dst && dst != ROOTFID) {
        if (dotdot && dotdot->parent) {
            dotdot = dotdot->parent;
        } else {
            MetaFattr* fa = 0;
            lookup(dst, kParentDir,
                kKfsUserRoot, kKfsGroupRoot, fa);
            if (! fa) {
                return false;
            }
            dotdot = fa;
        }
        dst = dotdot->id();
    }

    return (src == dst);
}

/*!
 * \brief rename a file or directory
 * \param[in]   parent  file id of parent directory
 * \param[in]   oldname the file's current name
 * \param[in]   newname the new name for the file
 * \param[in]   oldpath the fully qualified path for the file's current name
 * \param[in]   overwrite when set, overwrite the dest if it exists
 * \return      status code
 */
int
Tree::rename(fid_t parent, const string& oldname, const string& newname,
    const string& oldpath, bool overwrite,
    kfsUid_t euser, kfsGid_t egroup, int64_t mtime, fid_t* outSrcFid,
    bool const todumpster)
{
    int status;

    MetaFattr*  sfattr  = 0;
    MetaFattr*  sdfattr = 0;
    MetaDentry* src     = 0;
    status = lookup(parent, oldname, euser, egroup, sfattr, &sdfattr, &src);
    if (status != 0) {
        return status;
    }
    if (! sdfattr || ! src || ! sfattr) {
        panic("rename: null src node attribute");
        return -EFAULT;
    }
    if (! sdfattr->CanWrite(euser, egroup)) {
        return -EACCES;
    }
    if (outSrcFid) {
        *outSrcFid = sfattr->id();
    }
    fid_t  ddir;
    string dname;
    const string::size_type rslash = newname.rfind('/');
    MetaFattr* ddfattr;
    if (rslash == string::npos) {
        if (todumpster && mEnforceDumpsterRulesFlag &&
                getDumpsterDirId() == parent) {
            KFS_LOG_STREAM_DEBUG <<
                newname << ": attempt to rename in dumpster denied" <<
            KFS_LOG_EOM;
            return -EPERM;
        }
        ddir  = parent;
        dname = newname;
        // Parent doesn't change.
        ddfattr = 0;
    } else {
        if ((status = lookupPath(parent,
                newname.substr(0, max(size_t(1), rslash)),
                euser, egroup, ddfattr)) != 0) {
            return status;
        }
        if (ddfattr->type != KFS_DIR) {
            return -ENOTDIR;
        }
        if (todumpster && mEnforceDumpsterRulesFlag &&
                getDumpsterDirId() == ddfattr->id()) {
            KFS_LOG_STREAM_DEBUG <<
                newname << ": attempt to move to dumpster denied" <<
            KFS_LOG_EOM;
            return -EPERM;
        }
        if (! ddfattr->CanWrite(euser, egroup)) {
            return -EACCES;
        }
        ddir  = ddfattr->id();
        dname = newname.substr(rslash + 1);
    }
    if (! legalname(dname)) {
        return -EINVAL;
    }
    if (ddir == parent && dname == oldname) {
        return 0;
    }
    MetaFattr* dfattr = 0;
    status = lookup(ddir, dname, euser, egroup, dfattr);
    if (status != 0 && status != -ENOENT) {
        return status;
    }
    const fid_t    srcfid  = sfattr->id();
    const FileType t       = sfattr->type;
    const bool     dexists = dfattr != 0;
    if (! overwrite && dexists) {
        return -EEXIST;
    }
    if (dexists && t != dfattr->type) {
        return (t == KFS_DIR) ? -ENOTDIR : -EISDIR;
    }
    if (dexists && t == KFS_DIR && ! emptydir(dfattr->id())) {
        return -ENOTEMPTY;
    }
    if (t == KFS_DIR && is_descendant(srcfid, ddir, dfattr)) {
        return -EINVAL;
    }
    if (dexists) {
        status = (t == KFS_DIR) ?
            rmdir(ddir, dname, newname, euser, egroup, mtime) :
            remove(ddir, dname, newname, euser, egroup, mtime, todumpster);
        if (status != 0) {
            return status;
        }
    }
    if (mEnforceDumpsterRulesFlag && KFS_FILE == t &&
            getDumpsterDirId() == src->getDir() &&
            mChunksDeleteQueueFattr == sfattr) {
        KFS_LOG_STREAM_DEBUG <<
            newname << ": attempt to move from dumpster denied" <<
        KFS_LOG_EOM;
        return -EPERM;
    }

    // invalidate the path->fid cache mappings
    const bool kRemoveDirPrefixFlag = true;
    invalidatePathCache(oldpath, oldname, sfattr, kRemoveDirPrefixFlag);
    sdfattr->mtime = mtime;
    if (t == KFS_DIR && ddfattr) {
        // get rid of the linkage of the "old" ..
        unlink(srcfid, kParentDir, sfattr, true);
    }
    if ((status = del(src))) {
        panic("rename delete souce node failed");
        return status;
    }
    MetaDentry* const newSrc = MetaDentry::create(
        ddir, dname, srcfid, sfattr);
    if ((status = insert(newSrc))) {
        panic("rename insert souce node failed");
        newSrc->destroy();
        return status;
    }
    if (ddfattr) {
        const chunkOff_t size    = sfattr->filesize;
        const int64_t    fileCnt = t == KFS_DIR ?
            sfattr->fileCount() : 1;
        const int64_t    dirCnt  = t == KFS_DIR ?
            sfattr->dirCount() + 1 : 0;
        setFileSize(sfattr, 0, -fileCnt, -dirCnt);
        sfattr->parent = ddfattr;
        ddfattr->mtime = mtime;
        // Set both parent and dentry attribute, ensuring that dentry
        // attribute is setup, in order to make consistency check in
        // recomputeDirSize() work.
        setFileSize(sfattr,
            size >= 0 ? size : chunkOff_t(-1) - size, fileCnt, dirCnt);
        sfattr->filesize = size;
    }
    if (t == KFS_DIR && ddfattr) {
        // create a new linkage for ..
        status = link(srcfid, kParentDir, KFS_DIR, ddir, 1,
            KFS_STRIPED_FILE_TYPE_NONE, 0, 0, 0,
            kKfsUserNone, kKfsGroupNone, 0, ddfattr, 0, mtime);
        assert(status == 0);
    }
    return 0;
}


/*!
 * \brief Change the degree of replication for a file.
 * \param[in] dir   file id of the file
 * \param[in] numReplicas   desired degree of replication
 * \return      status code (-errno on failure)
 */
int
Tree::changePathReplication(fid_t fid, int16_t numReplicas,
    kfsSTier_t minSTier, kfsSTier_t maxSTier)
{
    MetaFattr *fa = getFattr(fid);

    if (! fa) {
        return -ENOENT;
    }
    if (fa->type == KFS_DIR) {
        return changeDirReplication(fa, numReplicas, minSTier, maxSTier);
    }
    return changeFileReplication(fa, numReplicas, minSTier, maxSTier);

}

int
Tree::changeFileReplication(MetaFattr* fa, int16_t numReplicas,
    kfsSTier_t minSTier, kfsSTier_t maxSTier)
{
    if (KFS_FILE != fa->type && 0 < numReplicas) {
        // Allow to change directory tiers, sot that the tiers can be
        // "inherited" by sub directories and files, but do not allow to change
        // directory replication.
        return -EISDIR;
    }
    if (fa->IsSymLink()) {
        return -ENXIO;
    }
    if ((minSTier != kKfsSTierUndef && ! IsValidSTier(minSTier)) ||
            (maxSTier != kKfsSTierUndef && ! IsValidSTier(maxSTier))) {
        return -EINVAL;
    }
    if (minSTier != kKfsSTierUndef) {
        fa->minSTier = minSTier;
        if (fa->maxSTier < minSTier) {
            fa->maxSTier = minSTier;
        }
    }
    if (maxSTier != kKfsSTierUndef) {
        fa->maxSTier = maxSTier;
        if (maxSTier < fa->minSTier) {
            fa->minSTier = maxSTier;
        }
    }
    // For now storage tiers change has no effect on the chunks that were
    // previously allocated.
    if (numReplicas <= 0) {
        return ((minSTier != kKfsSTierUndef || maxSTier != kKfsSTierUndef) ?
            0 : -EINVAL);
    }
    if ((int)fa->numReplicas == numReplicas) {
        return 0;
    }
    if (KFS_FILE == fa->type && 0 == fa->numReplicas) {
        // Do not allow to change object store file replication.
        return -EINVAL;
    }
    fa->setReplication(numReplicas);
    StTmp<vector<MetaChunkInfo*> > cinfoTmp(mChunkInfosTmp);
    vector<MetaChunkInfo*>&        chunkInfo = cinfoTmp.Get();
    getalloc(fa->id(), chunkInfo);
    for (vector<ChunkLayoutInfo>::size_type i = 0; i < chunkInfo.size(); ++i) {
        gLayoutManager.ChangeChunkReplication(chunkInfo[i]->chunkId);
    }
    return 0;
}

/*!
 * \brief  A file that has to be removed is currently busy.  So, rename the
 * file to the dumpster and we'll clean it up later.
 * \param[in] dir   file id of the parent directory
 * \param[in] fname file name
 * \return      status code (zero on success)
 */
int
Tree::moveToDumpster(fid_t dir, const string& fname, MetaFattr& fa,
    int64_t mtime)
{
    string tempname = "/" + DUMPSTERDIR + "/";
    const fid_t ddir = getDumpsterDirId();
    if (ddir < 0) {
        panic("move to dumpster: no dumpster");
        return -EINVAL;
    }
    // can't move something in the dumpster back to dumpster
    if (ddir == dir) {
        return -EEXIST;
    }
    // Generate unique name by appending i-node number.
    // Use source directory i-node number to allow to recover files from
    // dumpster.
    // Note that changing the name generation here results in breaking
    // transaction log replay backward compatibility as remove from dumpster
    // op stores this name in the transaction log.
    const fid_t  fid           = fa.id();
    const size_t plen          = tempname.size();
    const size_t kMaxSuffixLen = 2 * (sizeof(fid) * 2 + 1);
    if (kMaxSuffixLen <= MAX_FILE_NAME_LENGTH) {
        AppendHexIntToString(tempname, dir);
        tempname += ".";
    }
    if (kMaxSuffixLen + fname.length() <= MAX_FILE_NAME_LENGTH) {
        tempname += fname;
        tempname += ".";
    }
    AppendHexIntToString(tempname, fid);

    const bool   kToDumpsterFlag = false;
    const bool   kOverwriteFlag  = false;
    const string kOldPath;      // Path cache must be already invalidated.
    const int    ret            = rename(dir, fname, tempname, kOldPath,
        kOverwriteFlag, kKfsUserRoot, kKfsGroupRoot, mtime, 0, kToDumpsterFlag);
    if (ret == 0) {
        tempname.erase(0, plen);
        gLayoutManager.ScheduleDumpsterCleanup(fa, tempname);
    }
    return ret;
}

fid_t
Tree::getDumpsterDirIdSelf()
{
    if (mDumpsterDirId < 0) {
        MetaFattr* fa     = 0;
        int const  status = lookup(
            ROOTFID, DUMPSTERDIR, kKfsUserRoot, kKfsGroupRoot, fa);
        if (0 != status || ! fa || fa->type != KFS_DIR) {
            return -1;
        }
        mDumpsterDirId = fa->id();
    }
    return mDumpsterDirId;
}

const MetaFattr*
Tree::getChunkDeleteQueueSelf()
{
    if (mChunksDeleteQueueFattr) {
        return mChunksDeleteQueueFattr;
    }
    const fid_t ddir = getDumpsterDirId();
    if (ddir < 0) {
        panic("get chunk delete queue: no dumpster");
        return mChunksDeleteQueueFattr;
    }
    int status = lookup(ddir, CHUNKDELQUEUE,
        kKfsUserRoot, kKfsGroupRoot, mChunksDeleteQueueFattr);
    if (0 == status && KFS_FILE == mChunksDeleteQueueFattr->type &&
            ! mChunksDeleteQueueFattr->HasExtAttrs()) {
        return mChunksDeleteQueueFattr;
    }
    if (-ENOENT != status) {
        panic("invalid chunk delete queue lookup status");
    }
    return mChunksDeleteQueueFattr;
}

void
Tree::ensureChunkDeleteQueueExists()
{
    if (getChunkDeleteQueue()) {
        return;
    }
    const fid_t     ddir               = getDumpsterDirId();
    const int16_t   numReplicas        = 1;
    const int32_t   numStripes         = 0;
    const int32_t   numRecoveryStripes = 0;
    const int32_t   stripeSize         = 0;
    const kfsMode_t mode               = 0;
    const int status = link(
        ddir, CHUNKDELQUEUE, KFS_FILE, fileID.genid(),
        numReplicas,
        KFS_STRIPED_FILE_TYPE_NONE, numStripes, numRecoveryStripes, stripeSize,
        kKfsUserRoot, kKfsGroupRoot, mode,
        getFattr(ddir),
        &mChunksDeleteQueueFattr,
        microseconds()
    );
    if (0 != status) {
        panic("invalid chunk delete queue link status");
        return;
    }
    UpdateNumFiles(1);
    updateCounts(mChunksDeleteQueueFattr, 0, 1, 0);
    mChunksDeleteQueueFattr->minSTier = kKfsSTierMax;
    mChunksDeleteQueueFattr->maxSTier = kKfsSTierMax;
}

/*!
 * \brief Periodically, cleanup the dumpster and reclaim space.  If
 * the lease issued on a file has expired, then the file can be nuked.
 */
void
Tree::cleanupDumpster()
{
    const fid_t ddir = getDumpsterDirId();
    if (ddir < 0) {
        panic("cleanup dumpster: no dumpster");
        return;
    }
    getChunkDeleteQueue();
    DentryIterator it = readDir(ddir);
    const MetaDentry* e;
    while ((e = it.next())) {
        const string&    name = e->getName();
        const MetaFattr* fa   = e->getFattr();
        if (! fa) {
            if (name == kThisDir || name == kParentDir) {
                continue;
            }
            fa = getFattr(e->id());
            if (! fa) {
                continue;
            }
        }
        if (KFS_FILE != fa->type ||
                mChunksDeleteQueueFattr == fa ||
                CHUNKDELQUEUE == name) {
            continue;
        }
        gLayoutManager.ScheduleDumpsterCleanup(*fa, name);
    }
}

void
Tree::removeSubTree(fid_t dir, int64_t mtime)
{
    StTmp<vector<MetaDentry*> > dentriesTmp(mDentriesTmp);
    vector<MetaDentry*>&        entries = dentriesTmp.Get();
    MetaFattr*                  fa = 0;
    removeSubTree(dir, entries, &fa);
    if (fa) {
        fa->mtime = mtime;
    }
}

void
Tree::removeSubTree(fid_t dir, vector<MetaDentry*>& entries, MetaFattr** dfa)
{
    DentryIterator it = readDir(dir);
    MetaDentry*    e;
    while ((e = it.next())) {
        MetaFattr* const fa = getFattr(e);
        if (! fa) {
            continue;
        }
        if (KFS_DIR == fa->type) {
            const string& name = e->getName();
            if (kThisDir == name) {
                if (dfa) {
                    *dfa = fa;
                }
                continue;
            }
            if (kParentDir == name || ROOTFID == e->id()) {
                continue;
            }
            removeFiles(dir, entries);
            removeSubTree(e->id(), entries, 0);
            UpdateNumDirs(-1);
            setFileSize(fa, 0, 0, -1);
            const fid_t id = fa->id();
            unlink(id, kThisDir, fa, true);
            unlink(id, kParentDir, fa, true);
            unlink(dir, e->getName(), fa, false);
            // Tree has changed, start over.
            it = readDir(dir);
            continue;
        }
        entries.push_back(e);
    }
    removeFiles(dir, entries);
    entries.clear();
}

void
Tree::removeFiles(fid_t dir, vector<MetaDentry*>& entries)
{
    for (vector<MetaDentry*>::const_iterator it = entries.begin();
            it != entries.end();
            ++it) {
        MetaFattr* const fa = getFattr(*it);
        assert(KFS_FILE == fa->type);
        if (0 < fa->chunkcount()) {
            StTmp<vector<MetaChunkInfo*> > cinfoTmp(mChunkInfosTmp);
            vector<MetaChunkInfo*>&        chunkInfo = cinfoTmp.Get();
            getalloc(fa->id(), chunkInfo);
            assert(fa->chunkcount() == (int64_t)chunkInfo.size());
            UpdateNumChunks(-fa->chunkcount());
            for_each(chunkInfo.begin(), chunkInfo.end(),
                 bind(&MetaChunkInfo::DeleteChunk, _1));
        } else if (0 == fa->numReplicas) {
            gLayoutManager.DeleteFile(*fa);
        }
        UpdateNumFiles(-1);
        setFileSize(fa, 0, -1, 0);
        unlink(dir, (*it)->getName(), fa, false);
    }
    entries.clear();
}

static void
deleteNode(MetaNode& node)
{
    if (node.metaType() == KFS_INTERNAL) {
        Node& cur = static_cast<Node&>(node);
        int pos = cur.children();
        while (0 <= --pos) {
            MetaNode* const child = cur.child(pos);
            if (child) {
                deleteNode(*child);
            }
        }
    }
    node.destroy();
}

Tree::~Tree()
{
    if (root) {
        deleteNode(*root);
        root  = 0;
        first = 0;
    }
}

} // namespace KFS
