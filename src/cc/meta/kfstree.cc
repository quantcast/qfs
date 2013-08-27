/*!
 * $Id$
 *
 * \file kfstree.cc
 * \brief Tree-manipulating routines for the KFS metadata server.
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
#include <sstream>
#include <iomanip>
#include <algorithm>
#include "kfstree.h"
#include "Checkpoint.h"

namespace KFS
{

using std::for_each;
using std::hex;
using std::cerr;

Tree metatree;

/*!
 * \brief Insert a child node at the indicated position.
 * \param[in] child the node to be inserted
 * \param[in] pos   the index where it should go
 */
void
Node::addChild(Key *k, MetaNode *child, int pos)
{
    openHole(pos, 1);
    childKey[pos] = *k;
    childNode[pos] = child;
}

/*!
 * \brief remove child and shift remaining entries to fill in the hole
 * \param[in] pos index of the child node
 *
 */
void
Node::remove(int pos)
{
    assert(pos >= 0 && pos < count);
    Meta *m = leaf(pos);
    m->destroy();
    closeHole(pos, 1);
}

void
Node::moveChildren(Node *dest, int start, int n)
{
    for (int i = 0; i != n; i++)
        dest->appendChild(childKey[start + i], childNode[start + i]);
    childKey[start] = Key(KFS_SENTINEL, 0);
    childNode[start] = NULL;
}

/*!
 * \brief split a full node
 * \param[in] t the tree (in case we add a new root)
 * \param[in] father    the parent of this node
 * \param[in] pos   position of this node in parent
 * \return  pointer to newly constructed sibling node
 *
 * Split this node (which is assumed to be full) into two
 * pieces, adding a new pointer to the father node, and if
 * necessary, a new root to the tree.  We split full nodes
 * as we traverse the tree downwards, so it should never
 * happen that the father node is full at this point.
 */
Node *
Node::split(Tree *t, Node *father, int pos)
{
    Node *brother = Node::create(flags());

    brother->linkToPeer(next);
    linkToPeer(brother);
    moveChildren(brother, count - NSPLIT, NSPLIT);
    count -= NSPLIT;
    if (father == NULL) {   // this must be the root
        assert(t->getroot() == this);
        t->pushroot(brother);
    } else {
        assert(!father->isfull());
        assert(father->child(pos) == this);
        father->resetKey(pos);
        Key k = brother->key();
        father->addChild(&k, brother, 1 + pos);
    }
    return brother;
}

/*
 * Create a space in the link array by moving everything
 * with index >= _pos_ by _skip_ spaces to the right.
 * N.B. Node must not be full
 */
void
Node::openHole(int pos, int skip)
{
    count += skip;
    assert(count <= NKEY);
    for (int i = count - 1; i >= pos + skip; --i) {
        childKey[i] = childKey[i - skip];
        childNode[i] = childNode[i - skip];
    }
}

/*
 * Fill in a hole created by moving or deleting child pointers.
 * pos is the beginning of the hole and skip is its size.
 */
void
Node::closeHole(int pos, int skip)
{
    assert(skip < count);
    count -= skip;
    for (int i = pos; i != count; i++) {
        childKey[i] = childKey[i + skip];
        childNode[i] = childNode[i + skip];
    }
    childKey[count] = Key(KFS_SENTINEL, 0);
    childNode[count] = NULL;
}

/*
 * Insert a new metadata item at the specified position
 * in this node, after making space by moving everything
 * with index >= pos to the right.
 */
void
Node::insertData(Key *k, Meta *item, int pos)
{
    assert(hasleaves());
    addChild(k, item, pos);
}

/*
 * This node is absorbed by its left neighbor.
 */
void
Node::absorb(Node *l)
{
    assert(count + l->children() <= NKEY);
    moveChildren(l, 0, count);
    count = 0;
    l->next = next;
}

/*!
 * \brief join an underfull node with its neighbor if possible
 * \param[in] pos   position of underfull child
 * \return      true if operation succeeded
 *
 * If the underfull child node at position pos has a
 * neighbor on either side that can absorb it, combine
 * the two nodes into one.
 *
 * Don't do the merge if the resulting node would be completely
 * full, since in that case, balanceNeighbor is probably a better
 * remedy.
 *
 * Return true if merge took place.
 */
bool
Node::mergeNeighbor(int pos)
{
    assert(!hasleaves());
    Node *left = leftNeighbor(pos);
    Node *right = rightNeighbor(pos);
    Node *middle = child(pos);
    int mkids = middle->children();
    int base;

    if (left != NULL && mkids + left->children() < NKEY) {
        middle->absorb(left);
        base = pos - 1;
    } else if (right != NULL && mkids + right->children() < NKEY) {
        right->absorb(middle);
        base = pos;
    } else
        return false;

    childKey[base] = childKey[base + 1];
    childNode[base + 1]->destroy();
    closeHole(base + 1, 1);

    return true;
}

/*
 * Move n children from _start_ in this node to the
 * beginning of _dest.
 */
void
Node::insertChildren(Node *dest, int start, int n)
{
    count -= n;
    for (int i = 0; i != n; i++)
        dest->placeChild(childKey[start + i], childNode[start + i], i);
}

/*
 * Move nshift children from the right end of this node
 * to the beginning of _dest_.
 */
void
Node::shiftRight(Node *dest, int nshift)
{
    dest->openHole(0, nshift);
    insertChildren(dest, count - nshift, nshift);
}

/*
 * Move nshift children from the beginning of this node
 * to the end of _dest_.
 */
void
Node::shiftLeft(Node *dest, int nshift)
{
    moveChildren(dest, 0, nshift);
    closeHole(0, nshift);
}

/*
 * Adjust the key for this position to match the rightmost
 * key of the child node; corrects the key value following
 * movement of children between nodes.
 */
void
Node::resetKey(int pos)
{
    Node *c = child(pos);
    assert(c != NULL);
    childKey[pos] = c->key();
}

/*!
 * \brief fill an underfull node by borrowing from neighbor
 * \param[in] pos   position of underfull child
 * \return      true if operation succeeded
 *
 * If the underfull child node at position pos has a neighbor
 * that is not underfull, borrow from it to bring this one up
 * to strength.  Return true if the operation succeeded.
 */
bool
Node::balanceNeighbor(int pos)
{
    assert(!hasleaves());
    Node *left = leftNeighbor(pos);
    Node *right = rightNeighbor(pos);
    Node *middle = child(pos);
    int lc = (left == NULL) ? -1 : left->children();
    int rc = (right == NULL) ? -1 : right->children();

    Node *donor = (lc >= rc) ? left : right;
    if (donor == NULL)
        return false;

    int nmove = donor->excess();
    if (nmove <= 0)
        return false;

    assert(nmove + middle->children() <= NKEY);

    if (donor == left) {
        left->shiftRight(middle, nmove);
        resetKey(pos - 1);
    } else {
        right->shiftLeft(middle, nmove);
        resetKey(pos);
    }

    return true;
}

/*!
 * \brief Insert the specified item in the tree.
 * \param item  the item to be inserted
 * \return  status code
 *
 * As we descend from the root, we split any full nodes encountered
 * along the path to ensure that there will be room at every level
 * to accommodate the insertion.  This eager splitting makes the tree
 * slightly larger than strictly necessary, but simplifies the algorithm,
 * since we don't need to backtrack from the leaf up to the first nonfull
 * node.
 *
 * The return value is zero if the insertion succeeds and
 * an error code if it fails (e.g., because duplicates are
 * not allowed).
 */
int
Tree::insert(Meta *item)
{
    Key mkey = item->key();
    Node *n = root, *dad = NULL;
    int cpos, dpos = -1;

    for (;;) {
        cpos = n->findplace(mkey);
        if (n->isfull()) {
            Node *brother = n->split(this, dad, dpos);
            if (cpos >= n->children()) {
                n = brother;
                cpos = n->findplace(mkey);
            }
        }
        if (n->hasleaves())
            break;
        dad = n;
        dpos = cpos;
        n = dad->child(dpos);
    }

    n->insertData(&mkey, item, cpos);
    return 0;
}

/*
 * If searching carries us into a new level-1 node below, shift the
 * next level of the descent path over by one, repeating as necessary
 * at higher levels.
 */
void
Tree::shift_path(vector <pathlink> &path)
{
    int i = path.size();
    bool done = false;

    while (!done) {
        assert(i != 0);
        pathlink pl = path[--i];
        pl.pos++;
        done = (pl.pos != pl.n->children());
        if (!done) {
            pl.pos = 0;
            pl.n = pl.n->peer();
        }
        path[i] = pl;
    }
}

/*!
 * \brief Delete specified item from tree.
 * \param[in] m     the item to be deleted
 * \return      0 on success, -1 otherwise
 */
int
Tree::del(Meta *m)
{
    Key mkey = m->key();
    vector <pathlink> path;
    Node *dad;
    bool removed = false;

    /*
     *  Descend to the appropriate leaf, remembering the
     *  path that we traverse from the root.
     */
    Node *n = root;
    int pos = n->findplace(mkey);
    while (!n->hasleaves()) {
        assert(pos != n->children());
        path.push_back(pathlink(n, pos));
        n = n->child(pos);
        pos = n->findplace(mkey);
    }

    /*
     * Try to remove the item.  Since there can be multiple
     * items with the same key, we have to iterate here to
     * find the right one (if it exists).  If we jump to a new
     * level-1 node, we have to update the path correspondingly.
     */
    Node *n0 = n;
    LeafIter li(n, pos);
    while (!removed && mkey == n->getkey(pos)) {
        if (m->match(n->leaf(pos))) {
            n->remove(pos);
            removed = true;
        } else {
            li.next();
            n = li.parent();
            pos = li.index();
            if (n != n0) {
                shift_path(path);
                n0 = n;
            }
        }
    }
    if (!removed)
        return -1;

    /*
     * If we removed the last child, the parent node's key
     * changes, and that can percolate up to higher-level nodes
     * as well.  Trace back up the path as needed to update the
     * keys.
     */
    if (pos == n->children()) {
        int i = path.size();
        while (i != 0) {
            dad = path[--i].n;
            pos = path[i].pos;
            dad->resetKey(pos);
            if (pos != dad->children() - 1)
                break;  /* stop if not rightmost child */
        }
    }

    /*
     * Now go back up the path to see if we can eliminate any
     * underfull nodes, either by merging them into a neighbor
     * or by borrowing children from a neighbor that has an excess
     * of them.  Stop at the first success, since the rearrangement
     * may invalidate the saved path.
     */
    bool balanced = false;
    dad = NULL;
    while (!path.empty() && !balanced) {
        pathlink pl = path.back();
        path.pop_back();
        dad = pl.n;
        pos = pl.pos;
        assert(dad->child(pos) == n);
        balanced = n->isdepleted() &&
            (dad->mergeNeighbor(pos) || dad->balanceNeighbor(pos));
        n = dad;
    }

    /*
     * If we rearranged children of the root, check whether the
     * root is left with just one child, in which case we can reduce
     * the height of the tree.
     */
    if (balanced && dad == root)
        poproot();

    return 0;
}

/*!
 * \brief Push a new root onto the top of the tree.
 * \param[in] brother   this node  holds the overflow from the old root
 *          and becomes its sibling.
 */
void
Tree::pushroot(Node *brother)
{
    Node *newroot = Node::create(META_ROOT);
    root->clearflag(META_ROOT);
    brother->clearflag(META_ROOT);
    Key k = root->key();
    newroot->addChild(&k, root, 0);
    k = brother->key();
    newroot->addChild(&k, brother, 1);
    root = newroot;
    ++hgt;
}

/*!
 * \brief get rid of the root node when it has only one child.
 */
void
Tree::poproot()
{
    if (!root->hasleaves() && root->children() == 1) {
        root = root->child(0);
        --hgt;
    }
}

/*
 * Output commands for debugging.
 */

/*!
 * \brief print all metadata belonging to this leaf node
 */
ostream&
Node::showSelf(ostream& os) const
{
    const ostream::fmtflags f = os.flags();
    os <<
        "internal/"  << this <<
        "/children/" << children() <<
        "/flags/"    << hex << flags()
    ;
    os.flags(f);
    return os;
}

void
showNode(MetaNode *n)
{
    if (n != NULL)
        n->show(cerr) << '\n';
}

void
Node::showChildren() const
{
    for_each(childNode, childNode + count, showNode);
}

/*!
 * \brief dump out all of the metadata items for debugging
 */
void
Tree::printleaves()
{
    for (Node *n = first; n != NULL; n = n->peer()) {
        showNode(n);
        n->showChildren();
    }
}

} // namespace KFS
