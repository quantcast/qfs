/*!
 * $Id$
 *
 * \file kfstree.h
 * \brief Search tree for the KFS metadata server.
 * \author Blake Lewis (Kosmix Corp.)
 *
 * The tree is a B+ tree with leaves representing the structure
 * of the file system, e.g., directory entries, file attributes, etc.
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

#if !defined(KFS_KFSTREE_H)
#define KFS_KFSTREE_H

#include "Key.h"
#include "MetaNode.h"
#include "meta.h"
#include "common/StdAllocator.h"
#include "common/StTmp.h"
#include "kfsio/Globals.h"

#include <string>
#include <vector>
#include <algorithm>
#include <set>
#include <map>

namespace KFS {
using std::string;
using std::vector;
using std::lower_bound;
using std::set;
using std::ostream;
using std::map;
using std::less;

class Tree;

/*!
 * \brief an internal node in the KFS search tree.
 *
 * Nodes contain an array of keys together with links either
 * to nodes lower in the tree or to metadata at the leaves.
 * Each is linked to the following node at the same level in
 * the tree to allow linear traversal.
 */
class Node: public MetaNode {
    static const int NKEY = 32;
    // should size the node to near 4k; 120 gets us there...
    // static const int NKEY = 120;
    static const int NSPLIT = NKEY / 2;
    static const int NFEWEST = NKEY - NSPLIT;

    int count;          //!< how many children
    Key childKey[NKEY];     //!< children's key values
    MetaNode *childNode[NKEY];  //!< and pointers to them

    Node *next;         //!< following peer node

    void placeChild(Key k, MetaNode *n, int p)
    {
        childKey[p] = k;
        childNode[p] = n;
    }
    void appendChild(Key k, MetaNode *n)
    {
        placeChild(k, n, count);
        ++count;
    }
    void moveChildren(Node *dest, int start, int n);
    void insertChildren(Node *dest, int start, int n);
    void absorb(Node *dest);
    int excess() { return (count - NFEWEST) / 2; }
    void linkToPeer(Node *n) { next = n; }
    void openHole(int pos, int skip);
    void closeHole(int pos, int skip);
    Node *leftNeighbor(int pos)
    {
        return (pos == 0) ? NULL : child(pos - 1);
    }
    Node *rightNeighbor(int pos)
    {
        return (pos == count - 1) ? NULL : child(pos + 1);
    }
    void shiftLeft(Node *dest, int nshift);
    void shiftRight(Node *dest, int nshift);
protected:
    Node(int f): MetaNode(KFS_INTERNAL, f), count(0), next(NULL) { }
    virtual ~Node() {}
public:
    static Node* create(int f) { return new (allocate<Node>()) Node(f); }
    virtual void destroy()
    {
        this->~Node();
        deallocate(this);
    }
    bool hasleaves() const { return testflag(META_LEVEL1); }
    bool isroot() const { return testflag(META_ROOT); }
    bool isfull() const { return (count == NKEY); } //!< full
    bool isdepleted() const { return (count < NFEWEST); } //!< underfull
    int cpbit() const { return testflag(META_CPBIT) ? 1 : 0; }
    void markparity(int count)
    {
        if (count & 1)
            setflag(META_CPBIT);
        else
            clearflag(META_CPBIT);
    }
    /*!
    * \brief binary search to locate key within node
    * \param[in] test   the key that we are looking for
    * \return       the position of first key >= test;
    *           can be off the end of the array
    */
    template<typename MATCH>
    int findplace(const MATCH &test) const
    {
        const Key* const p = lower_bound(childKey, childKey + count, test);
        return p - childKey;
    }
    //! \brief rightmost (largest) key in node
    const Key key() const { return childKey[count - 1]; }
    Node *child(int n) const        //! \brief accessor
    {
        return static_cast <Node *> (childNode[n]);
    }
    Meta *leaf(int n) const         //! \brief accessor
    {
        return static_cast <Meta *> (childNode[n]);
    }
    const Key &getkey(int n) const { return childKey[n]; } //!< accessor
    Node *split(Tree *t, Node *father, int pos);    //!< split full node
    void addChild(Key *k, MetaNode *child, int pos); //!< insert child node
    void insertData(Key *key, Meta *item, int pos); //!< insert data item
    Node *peer() const { return next; } //!< return adjacent node
    int children() const { return count; } //!< how many children
    bool mergeNeighbor(int pos);        //!< merge underfull nodes
    bool balanceNeighbor(int pos);      //!< borrow from full node
    void resetKey(int pos);         //!< update key from child
    void remove(int pos);           //!< delete child node
    /*!
     * \brief return metadata with the specified key
     * \param[in] k key that we are looking for
     * \return  pointer to corresponding metadata
     * \warning only use in cases where the key is unique
     */
    template <typename T> T *extractMeta(int n)
    {
        return refine<T>(leaf(n));
    }
    template <typename T> T *extractMeta(const Key &k)
    {
        return refine<T>(leaf(findplace(k)));
    }
    ostream& show(ostream& os) const;
    void showChildren() const;
};

/*!
 * \brief for iterating through leaf nodes
 */
class LeafIter
{
    Node *dad;  //!< node containing child pointers
    int pos;    //!< index of current child
public:
    LeafIter(Node *d, int p): dad(d), pos(p) { }
    Meta *current() const { return dad->leaf(pos); }
    Node *parent() const { return dad; }
    int index() const { return pos; }
    void next()
    {
        if (++pos == dad->children()) {
            pos = 0;
            dad = dad->peer();
        }
    }
    void reset(Node *d, int p) {
        dad = d;
        pos = p;
    }
};

template<MetaType TId, typename T>
class MetaIterator
{
public:
    MetaIterator(
        Node* n   = 0,
        int   pos = 0,
        fid_t fid = 0)
        : mKey(TId, fid),
          mIt(n, pos)
        {}
    MetaIterator(
            Node*               n,
            int                 pos,
            const PartialMatch& key)
        : mKey(key),
          mIt(n, pos)
        {}
    T* next()
    {
        Node* const p = mIt.parent();
        T*          n;
        if (p && p->getkey(mIt.index()) == mKey) {
            n = refine<T>(mIt.current());
            mIt.next();
        } else {
            n = 0;
        }
        return n;
    }
    T* lowerBound(const Key& key)
    {
        int   p = 0;
        Node* n = mIt.parent();
        while (n && (p = n->findplace(key)) == n->children()) {
            n = n->peer();
        }
        mIt.reset(n, p);
        return next();
    }
private:
    PartialMatch mKey;
    LeafIter     mIt;
};

typedef MetaIterator<KFS_CHUNKINFO, MetaChunkInfo> ChunkIterator;
typedef MetaIterator<KFS_DENTRY,    MetaDentry>    DentryIterator;

struct PathToFidCacheEntry
{
    PathToFidCacheEntry()
        : fid(-1),
          fa(0),
          lastAccessTime(0)
        {}
    fid_t      fid;
    MetaFattr* fa;
    time_t     lastAccessTime;
};

template<typename T>
class PathListerT
{
public:
    PathListerT(T& functor)
        : mFunctor(functor),
          mPathLen(),
          mCurPath(),
          mCurDepth(0)
    {
        mCurPath.reserve(32 << 10);
        mPathLen.reserve(1024);
        mCurPath += "/";
        mPathLen.push_back(mCurPath.length());
    }
    bool operator()(const MetaDentry& de, const MetaFattr& fa, size_t depth)
    {
        if (depth < mCurDepth) {
            mCurDepth = depth;
            mPathLen.resize(mCurDepth + 1);
            mCurPath.resize(mPathLen.back());
        } else if (depth > mCurDepth) {
            assert(depth == mCurDepth + 1);
            mCurPath += "/";
            mPathLen.push_back(mCurPath.length());
            mCurDepth = depth;
        }
        if (fa.type == KFS_DIR) {
            mCurPath.resize(mPathLen.back());
            mCurPath += de.getName();
        }
        const string& dir = mCurPath;
        return mFunctor(dir, de, fa, depth);
    }
private:
    T&             mFunctor;
    vector<size_t> mPathLen;
    string         mCurPath;
    size_t         mCurDepth;
protected:
    PathListerT(const PathListerT&);
    PathListerT& operator=(const PathListerT&);
};

//! If a cache entry hasn't been accessed in 600 secs, remove it from cache
const int FID_CACHE_ENTRY_EXPIRE_INTERVAL = 600;
//! Once in 10 mins cleanup the cache
const int FID_CACHE_CLEANUP_INTERVAL = 600;

/*!
 * \brief the KFS search tree.
 *
 * A tree is just a pointer to the root node and a pointer
 * to the first (leftmost) leaf node.
 */
class Tree {
    Node *root;         //!< root node
    Node *first;            //!< leftmost level-1 node
    int hgt;            //!< height of tree
    struct pathlink {       //!< for recording descent path
        Node *n;        //!< parent node
        int pos;        //!< index of child
        pathlink(Node *nn, int p): n(nn), pos(p) {}
        pathlink(): n(0), pos(-1) { }
    };
    typedef std::map<
        string,
        PathToFidCacheEntry,
        less<string>,
        StdAllocator<std::pair<const string, PathToFidCacheEntry> >
    > PathToFidCacheMap;

    bool allowFidToPathConversion;  //!< fid->path translation is enabled?
    bool mIsPathToFidCacheEnabled; //!< should we enable path->fid cache?
    bool mUpdatePathSpaceUsage;
    //!< optimize for lookupPath by caching fid's of recently looked up
    //entries.
    PathToFidCacheMap mPathToFidCache;
    time_t mLastPathToFidCacheCleanupTime;
    StTmp<vector<MetaChunkInfo*> >::Tmp mChunkInfosTmp;
    StTmp<vector<MetaDentry*> >::Tmp    mDentriesTmp;


    template<typename MATCH>
    Node* lowerBound(const MATCH &k, int& kp) const
    {
        Node *n = root;
        int p = n->findplace(k);

        while (!n->hasleaves() && p != n->children()) {
            n = n->child(p);
            p = n->findplace(k);
        }
        kp = p;
        return (p != n->children() ? n : 0);
    }
    /*
     * Return the leaf node containing the first instance of
     * the specified key.
     */
    template<typename MATCH>
    Node* findLeaf(const MATCH &k, int& kp) const
    {
        Node *n = lowerBound(k, kp);
        return ((n && n->getkey(kp) == k) ? n : 0);
    }
    void unlink(fid_t dir, const string& fname, MetaFattr *fa, bool save_fa);
    int link(fid_t dir, const string& fname, FileType type, fid_t myID,
        int16_t numReplicas, int32_t striperType, int32_t numStripes,
        int32_t numRecoveryStripes, int32_t stripeSize,
        kfsUid_t user, kfsGid_t group, kfsMode_t mode,
        MetaFattr* parent, MetaFattr** newFattr = 0);
    bool emptydir(fid_t dir);
    bool is_descendant(fid_t src, fid_t dst, const MetaFattr* dstFa);
    void shift_path(vector <pathlink> &path);
    void recomputeDirSize(MetaFattr* dirattr);
    void updateCounts(MetaFattr* fa, chunkOff_t nbytes, int64_t nfiles, int64_t ndirs);
    template<typename T>
    void iterateDentriesSelf(
        T& functor, MetaFattr* parentfa, MetaFattr* curfa, size_t depth)
    {
        const PartialMatch dkey(KFS_DENTRY, curfa->id());
        int                kp;
        Node* const        l = findLeaf(dkey, kp);
        if (! l) {
            return;
        }
        LeafIter it(l, kp);
        Node*    p;
        while ((p = it.parent()) && p->getkey(it.index()) == dkey) {
            MetaDentry* const de = refine<MetaDentry>(it.current());
            MetaFattr*  const fa = getFattr(de);
            if (fa && fa != parentfa && fa != curfa) {
                if (! functor(*de, *fa, depth)) {
                    return;
                }
                if (fa->type == KFS_DIR) {
                    iterateDentriesSelf(
                        functor, curfa, fa, depth + 1);
                }
            }
            it.next();
        }
    }
    void FindChunk(int64_t chunkCount, fid_t fid, chunkOff_t pos,
        ChunkIterator& cit, MetaChunkInfo*& ci) const;
    void setFileSize(MetaFattr* fa, chunkOff_t size,
        int64_t nfiles, int64_t ndirs);
public:
    Tree()
        : root(0),
          first(0),
          hgt(0),
          allowFidToPathConversion(false),
          mIsPathToFidCacheEnabled(false),
          mUpdatePathSpaceUsage(false),
          mPathToFidCache(),
          mLastPathToFidCacheCleanupTime(0),
          mChunkInfosTmp(),
          mDentriesTmp()
    {
        root = Node::create(META_ROOT|META_LEVEL1);
        root->insertData(new Key(KFS_SENTINEL, 0), NULL, 0);
        first = root;
        hgt = 1;
    }
    //!< create a directory namespace
    int new_tree(kfsUid_t user = kKfsUserRoot,
            kfsGid_t group = kKfsGroupRoot, kfsMode_t mode = 0755)
    {
        fid_t dummy = 0;
        return mkdir(ROOTFID, "/", user, group, mode,
            kKfsUserRoot, kKfsGroupRoot, &dummy);
    }
    void enablePathToFidCache()
    {
        mIsPathToFidCacheEnabled = true;
    }
    void setUpdatePathSpaceUsage(bool flag)
    {
        const bool recomputeFlag = ! mUpdatePathSpaceUsage && flag;
        mUpdatePathSpaceUsage = flag;
        if (recomputeFlag) {
            recomputeDirSize();
        }
    }
    bool getUpdatePathSpaceUsageFlag() const
        { return mUpdatePathSpaceUsage; }
    int insert(Meta *m);            //!< add data item
    int del(Meta *m);           //!< remove data item
    Node *getroot() { return root; }    //!< return root node
    Node *firstLeaf() { return first; } //!< leftmost leaf
    void pushroot(Node *rootbro);       //!< insert new root
    void poproot();             //!< discard current root
    int height() { return hgt; }        //!< return tree height
    void printleaves();         //!< print debugging info
    MetaFattr* getFattr(fid_t fid);     //!< return attributes
    MetaFattr* getFattr(const MetaDentry* dentry)
    {
        MetaFattr* fa = dentry->getFattr();
        if (fa) {
            return fa;
        }
        return getFattr(dentry->id());
    }
    MetaDentry* getDentry(fid_t dir, const string& fname);
#ifdef KFS_TREE_OPS_HAS_REVERSE_LOOKUP
    MetaDentry *getDentry(fid_t fid);   //!< return dentry attributes
#endif
    //!< turn off conversion from file-id to pathname---useful when we
    //!< are going to compute the size of "/" and thereby each dir. in the tree
    void disableFidToPathname() { allowFidToPathConversion = false; }
    void enableFidToPathname() { allowFidToPathConversion = true; }
    string getPathname(const MetaFattr* fa);    //!< return full pathname for a given file id
    int listPaths(ostream &ofs);    //!< list out the paths in the tree
    //!< list out the paths in the tree for specific fid's
    int listPaths(ostream &ofs, const set<fid_t> specificIds);
    void cleanupPathToFidCache();
    void recomputeDirSize();        //!< re-compute the size of each dir. in tree

    int create(fid_t dir, const string& fname, fid_t *newFid,
            int16_t numReplicas, bool exclusive,
            int32_t striperType, int32_t numStripes,
            int32_t numRecoveryStripes, int32_t stripeSize,
            fid_t& todumpster,
            kfsUid_t user, kfsGid_t group, kfsMode_t mode,
            kfsUid_t euser, kfsGid_t egroup,
            MetaFattr** newFattr = 0);
    //!< final argument is optional: when non-null, this call will return
    //!< the size of the file (if known)
    int remove(fid_t dir, const string& fname, const string& pathname, fid_t& todumpster,
        kfsUid_t euser, kfsGid_t egroup);
    int mkdir(fid_t dir, const string& dname,
        kfsUid_t user, kfsGid_t group, kfsMode_t mode,
        kfsUid_t euser, kfsGid_t egroup,
        fid_t* newFid, MetaFattr** newFattr = 0);
    int rmdir(fid_t dir, const string& dname, const string& pathname,
        kfsUid_t euser, kfsGid_t egroup);
    int readdir(fid_t dir, vector<MetaDentry*>& result,
        int maxEntries = 0, bool* moreEntriesFlag = 0);
    int readdir(fid_t dir, const string& fnameStart, vector<MetaDentry*>& v,
        int maxEntries, bool& moreEntriesFlag);
    int getalloc(fid_t fid, vector <MetaChunkInfo *> &result);
    int getalloc(fid_t fid, MetaFattr*& fa, vector<MetaChunkInfo*>& v,
        int maxChunks);
    int getalloc(fid_t fid, chunkOff_t& offset,
        MetaFattr*& fa, MetaChunkInfo*& c, vector<MetaChunkInfo*>* v,
        chunkOff_t* chunkBlockStart);
    int getalloc(fid_t fid, chunkOff_t offset, vector<MetaChunkInfo*>& v,
        int maxChunks);
    int getalloc(fid_t fid, chunkOff_t offset, MetaChunkInfo **c);
    int getLastChunkInfo(fid_t fid, bool nonStripedFileFlag,
        MetaFattr*& fa, MetaChunkInfo*& c);
    int rename(fid_t dir, const string& oldname, const string& newname,
            const string& oldpath, bool once, fid_t& todumpster,
            kfsUid_t euser, kfsGid_t egroup);
    int lookup(fid_t dir, const string& fname,
        kfsUid_t euser, kfsGid_t egroup, MetaFattr*& fa,
        MetaFattr** outParent = 0, MetaDentry** outDentry = 0);
    int lookupPath(fid_t rootdir, const string& path,
        kfsUid_t euser, kfsGid_t egroup, MetaFattr*& fa);
    void setFileSize(MetaFattr* fa, chunkOff_t offset)
        { setFileSize(fa, offset, 0, 0); }
    void invalidateFileSize(MetaFattr* fa) const
        { fa->filesize = -(fa->filesize + 1); }
    chunkOff_t getFileSize(const MetaFattr& fa) const {
        return (fa.filesize >= 0 ?
                fa.filesize : chunkOff_t(-1) - fa.filesize);
    }
    chunkOff_t getFileSize(const MetaFattr* fa) const {
        return getFileSize(*fa);
    }
    int changeFileReplication(MetaFattr *fa, int16_t numReplicas);
    int changeDirReplication(MetaFattr *dirattr, int16_t numReplicas);
    int changePathReplication(fid_t file, int16_t numReplicas);

    int moveToDumpster(fid_t dir, const string& fname, fid_t todumpster);
    void cleanupDumpster();

    /*!
     * \brief Write-allocation
     * On receiveing a request from a client for space allocation,
     * the  request is handled in two steps:
     * 1. The metaserver picks a chunk-id and sends a RPC to
     *    to a chunkserver to create the chunk.
     * 2. After the chunkserver ack's the RPC, the metaserver
     *    updates the metatree to reflect the allocation of
     *    the chunkId to the file.
     * The following two functions, respectively, perform the above two steps.
     */

    /*
     * \brief Allocate a unique chunk identifier
     * \param[in] file  The id of the file for which need to allocate space
     * \param[in/out] offset    The offset in the file at which space should be allocated
     *      if the offset isn't specified, then the allocation
     *      request is a "chunk append" to the file.  The file offset at
     *      which the chunk is allocated is returned back.
     * \param[out] chunkId  The chunkId that is allocated
     * \param[out] numReplicas The # of replicas to be created for the chunk.  This
     *  parameter is inhreited from the file's attributes.
     * \param[out] stripedFileFlag  from the file's attributes
     * \param[out] chunkBlock  list of chunk in the striped file chunk block
     * \retval 0 on success; -errno on failure
     */
    int allocateChunkId(fid_t file, chunkOff_t &offset, chunkId_t *chunkId,
        seq_t *version, int16_t *numReplicas, bool *stripedFileFlag,
        vector <MetaChunkInfo *>* chunkBlock      = 0,
        chunkOff_t*               chunkBlockStart = 0,
        kfsUid_t                  euser           = kKfsUserRoot,
        kfsGid_t                  egroup          = kKfsGroupRoot,
        MetaFattr**               fa              = 0);

    /*
     * \brief Assign a chunk identifier to a file.  Update the metatree
     * to reflect the assignment.
     * \param[in] file  The id of the file for which need to allocate space
     * \param[in] offset    The offset in the file at which space should be allocated
     * \param[in] chunkId   The chunkId that is assigned to file/offset
     * \param[io] appendOffset If not null and chunk at the specified offset exists,
     * then allocate chunk after eof, and return the newly assigned chunk offset.
     * \param[io] curChunkId If not null return currently assigned chunk id.
     * \retval 0 on success; -errno on failure
     */
    int assignChunkId(fid_t file, chunkOff_t offset,
        chunkId_t chunkId, seq_t version,
        chunkOff_t *appendOffset = 0, chunkId_t  *curChunkId = 0,
        bool appendReplayFlag = false);

    /*
     * \brief Coalesce blocks of one file with another. Move all the chunks from src to dest.
     * \param[in] srcPath    The full pathname for the source for the blocks
     * \param[in] dstPath    The full pathname for the source for the blocks
     * \param[out] srcFid    The source fileid (used when making the change permanent).
     * \param[out] dstFid    The dest fileid (used when making blk change permanent).
     * \param[out] srcChunks  The id's of the chunks from source
     * \param[out] dstStartOffset  The initial dest length
     * \param[in] mtime The modification time pointer, do not update if 0
     * \param[out] numChunksMoved The number of chunks moved from src to dst.
     */
    int coalesceBlocks(const string& srcPath, const string& dstPath,
                fid_t &srcFid, fid_t &dstFid,
                chunkOff_t &dstStartOffset,
                const int64_t* mtime, size_t &numChunksMoved,
                kfsUid_t euser, kfsGid_t egroup);
    int coalesceBlocks(MetaFattr* srcFa, MetaFattr* dstFa,
                fid_t &srcFid, fid_t &dstFid,
                chunkOff_t &dstStartOffset,
                const int64_t* mtime, size_t &numChunksMoved,
                kfsUid_t euser, kfsGid_t egroup);

    /*
     * \brief Truncate a file to the specified file offset.  Due
     * to truncation, chunks past the desired offset will be
     * deleted.  When there are holes in the file or if the file
     * is extended past the last chunk, a truncation can cause
     * a chunk allocation to occur.  This chunk will be used to
     * track the file's size.
     * \param[in] file  The id of the file being truncated
     * \param[in] offset    The offset to which the file should be
     *          truncate to
     * \param[in] mtime Modification time
     * \retval 0 on success; -errno on failure; 1 if an allocation
     * is needed
     */
    int truncate(fid_t file, chunkOff_t offset, const int64_t* mtime,
        kfsUid_t euser, kfsGid_t egroup, chunkOff_t endOffset, bool setEofHintFlag);

    /*
     * \brief Is like truncate, but in the opposite direction: delete blks
     * from the head of the file to the specified offset.
     * \param[in] file  The id of the file being truncated
     * \param[in] offset    The offset before which chunks in the file should
     *          be deleted.
     * \param[in] mtime Modification time
     * \retval 0 on success; -errno on failure
     */
    int pruneFromHead(fid_t file, chunkOff_t offset, const int64_t* mtime,
        kfsUid_t euser = kKfsUserRoot, kfsGid_t egroup = kKfsGroupRoot);
    void invalidatePathCache(const string& pathname, const string& name,
        const MetaFattr* fa, bool removeDirPrefixFlag = false);
    // PathListerT can be used as argument to build path.
    template<typename T>
    void iterateDentries(T& functor)
    {
        MetaFattr* fa = 0;
        lookup(ROOTFID, "/", kKfsUserRoot, kKfsGroupRoot, fa);
        if (fa) {
            iterateDentriesSelf(functor, fa, fa, 0);
        }
    }
    ChunkIterator getAlloc(fid_t fid) const;
    ChunkIterator getAlloc(fid_t fid, MetaFattr*& fa) const;
    DentryIterator readDir(fid_t dir) const;
};

/*!
 * \brief return all metadata with the specified key
 * \param[in] node  leftmost leaf node in which the key appears
 * \param[in] k     the key itself
 * \param[out] result   vector of all items with that key
 *
 * Finds the first instance of the key, then looks at adjacent
 * items (possibly jumping to a new leaf node), gathering up all
 * with the same key.
 */
template <typename MATCH, typename T> void
extractAll(Node *n, int kp, const MATCH &k, vector <T *> &result)
{
    int p = kp;
    while (n != NULL && k == n->getkey(p)) {
        result.push_back(refine<T>(n->leaf(p)));
        if (++p == n->children()) {
            p = 0;
            n = n->peer();
        }
    }
}

extern Tree metatree;
extern void makeDumpsterDir();
extern void emptyDumpsterDir();
}
#endif // !defined(KFS_KFSTREE_H)
