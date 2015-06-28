//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/11/03
// Author: Mike Ovsiannikov
//
// Copyright 2008-2011 Quantcast Corp.
//
// This file is part of Kosmos File System (KFS).
//
// Licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
//
//----------------------------------------------------------------------------

#ifndef QCDLLIST_H
#define QCDLLIST_H

// Simple doubly linked list: each node has to have mPrevPtr[K] and mNextPtr[K]
// pointers, where K is number of different lists that the node can be inserted
// simultaneously.
// mPrevPtr[K] and mNextPtr[K] pointer arrays don't have to be declared public,
// as long as QCDLListOp is declared as friend (no need to include this header):
// template<typename, unsigned int> friend class QCDLListOp;
// or if this header is included, then more specific friends can be declared:
// friend class QCDLListOp<MyNodeType,ListIndex>;

template <typename NodeT, unsigned int ListT = 0>
class QCDLListOp
{
public:
    static void Init(
        NodeT& inNode)
    {
        inNode.mPrevPtr[ListT] = &inNode;
        inNode.mNextPtr[ListT] = &inNode;
    }

    static void Insert(
        NodeT& inNode,
        NodeT& inAfter)
    {
        if (&inNode == &inAfter) {
            return;
        }
        // Remove.
        inNode.mPrevPtr[ListT]->mNextPtr[ListT] = inNode.mNextPtr[ListT];
        inNode.mNextPtr[ListT]->mPrevPtr[ListT] = inNode.mPrevPtr[ListT];
        // Insert.
        inNode.mPrevPtr[ListT] = &inAfter;
        inNode.mNextPtr[ListT] = inAfter.mNextPtr[ListT];
        inNode.mPrevPtr[ListT]->mNextPtr[ListT] = &inNode;
        inNode.mNextPtr[ListT]->mPrevPtr[ListT] = &inNode;
    }

    static void PushBackList(
        NodeT& inListLs,
        NodeT& inListRs)
    {
        NodeT& theLsPrev = *inListLs.mPrevPtr[ListT];
        NodeT& theRsPrev = *inListRs.mPrevPtr[ListT];

        theLsPrev.mNextPtr[ListT] = &inListRs;
        inListRs.mPrevPtr[ListT]  = &theLsPrev;
        theRsPrev.mNextPtr[ListT] = &inListLs;
        inListLs.mPrevPtr[ListT]  = &theRsPrev;
    }

    static void Remove(
        NodeT& inNode)
    {
        inNode.mPrevPtr[ListT]->mNextPtr[ListT] = inNode.mNextPtr[ListT];
        inNode.mNextPtr[ListT]->mPrevPtr[ListT] = inNode.mPrevPtr[ListT];
        Init(inNode);
    }

    static void Remove(
        NodeT& inStartNode,
        NodeT& inEndNode)
    {
        NodeT* thePtr = &inStartNode;
        while (thePtr != &inEndNode) {
            NodeT* const theRemPtr = thePtr;
            thePtr = GetNextPtr(thePtr);
            Remove(theRemPtr);
        }
    }

    static bool IsInList(
        const NodeT& inNode)
    {
        return (
            inNode.mPrevPtr[ListT] != &inNode ||
            inNode.mNextPtr[ListT] != &inNode
        );
    }

    static NodeT& GetPrev(
        const NodeT& inNode)
        { return *inNode.mPrevPtr[ListT]; }

    static NodeT& GetNext(
        const NodeT& inNode)
        { return *inNode.mNextPtr[ListT]; }

    static NodeT* GetPrevPtr(
        const NodeT* inNodePtr)
        { return (inNodePtr ? inNodePtr->mPrevPtr[ListT] : 0); }

    static NodeT* GetNextPtr(
        const NodeT* inNodePtr)
        { return (inNodePtr ? inNodePtr->mNextPtr[ListT] : 0); }
};

// Doubly linked list with head node pointer.
//
template <typename NodeT, unsigned int ListT = 0>
class QCDLList
{
private:
    typedef QCDLListOp<NodeT, ListT> ListOp;
public:
    static void Init(
        NodeT& inNode)
        { ListOp::Init(inNode); }

    static void Init(
        NodeT** inListPtr)
        { inListPtr[ListT] = 0; }

    static bool IsEmpty(
        NodeT*const* inListPtr)
        { return (! inListPtr[ListT]); }

    static void PushFront(
        NodeT** inListPtr,
        NodeT&  inNode)
    {
        // Insert removes the node from the list before inserting.
        NodeT*& theHeadPtr = inListPtr[ListT];
        if (theHeadPtr && &inNode != theHeadPtr) {
            ListOp::Insert(inNode, GetPrev(*theHeadPtr));
        }
        theHeadPtr = &inNode;
    }

    static NodeT& GetPrev(
        const NodeT& inNode)
        { return ListOp::GetPrev(inNode); }

    static NodeT& GetNext(
        const NodeT& inNode)
        { return ListOp::GetNext(inNode); }

    static void PushBack(
        NodeT** inListPtr,
        NodeT&  inNode)
    {
        NodeT*& theHeadPtr = inListPtr[ListT];
        if (theHeadPtr) {
            if (&inNode == theHeadPtr) {
                theHeadPtr = &GetNext(*theHeadPtr);
            } else {
                ListOp::Insert(inNode, GetPrev(*theHeadPtr));
            }
        } else {
            theHeadPtr = &inNode;
        }
    }

    static NodeT* PopFront(
        NodeT** inListPtr)
    {
        NodeT*&      theHeadPtr = inListPtr[ListT];
        NodeT* const theRetPtr  = theHeadPtr;
        if (theRetPtr) {
            if (theRetPtr == (theHeadPtr = &GetNext(*theRetPtr))) {
                theHeadPtr = 0;
            } else {
                ListOp::Remove(*theRetPtr);
            }
        }
        return theRetPtr;
    }

    static NodeT* PopBack(
        NodeT** inListPtr)
    {
        NodeT*&      theHeadPtr = inListPtr[ListT];
        NodeT* const theRetPtr  = ListOp::GetPrevPtr(theHeadPtr);
        if (theRetPtr) {
            if (theRetPtr == theHeadPtr) {
                theHeadPtr = 0;
            } else {
                ListOp::Remove(*theRetPtr);
            }
        }
        return theRetPtr;
    }

    static void Remove(
        NodeT** inListPtr,
        NodeT&  inNode)
    {
        NodeT*& theHeadPtr = inListPtr[ListT];
        if (&inNode == theHeadPtr) {
            if ((theHeadPtr = &GetNext(*theHeadPtr)) == &inNode) {
                theHeadPtr = 0;
                return;
            }
        }
        ListOp::Remove(inNode);
    }

    static void Clear(
        NodeT** inListPtr)
    {
        NodeT*& theHeadPtr = inListPtr[ListT];
        if (theHeadPtr) {
            NodeT* thePtr;
            while ((thePtr = &GetNext(*theHeadPtr)) != theHeadPtr) {
                ListOp::Remove(*thePtr);
            }
            theHeadPtr = 0;
        }
    }

    static void PushBackList(
        NodeT** inListLsPtr,
        NodeT** inListRsPtr)
    {
        NodeT*& theHeadLsPtr = inListLsPtr[ListT];
        NodeT*& theHeadRsPtr = inListRsPtr[ListT];
        if (theHeadLsPtr && theHeadRsPtr) {
            ListOp::PushBackList(*theHeadLsPtr, *theHeadRsPtr);
        } else if (theHeadRsPtr) {
            theHeadLsPtr = theHeadRsPtr;
        }
        theHeadRsPtr = 0;
    }

    static void PushFrontList(
        NodeT** inListLsPtr,
        NodeT** inListRsPtr)
    {
        PushBackList(inListRsPtr, inListLsPtr);
        inListLsPtr[ListT] = inListRsPtr[ListT];
        inListRsPtr[ListT] = 0;
    }

    // IsInList isn't completely foolproof, node can be in a different list.
    static bool IsInList(
        NodeT*const* inListPtr,
        const NodeT& inNode)
    {
        return (inListPtr[ListT] &&
            (&inNode == inListPtr[ListT] || ListOp::IsInList(inNode))
        );
    }

    static NodeT* Front(
        NodeT*const* inListPtr)
        { return inListPtr[ListT]; }

    static NodeT* Back(
        NodeT*const* inListPtr)
        { return ListOp::GetPrevPtr(inListPtr[ListT]); }

    class Iterator
    {
    public:
        Iterator(
            NodeT*const* inListPtr)
            : mHeadPtr(inListPtr),
              mNextNodePtr(inListPtr[ListT])
            {}

        ~Iterator()
            {}

        bool HasNext() const
            { return (mNextNodePtr != 0); }

        // Post increment, returns current node, or 0.
        NodeT* Next()
        {
            NodeT* const theRetPtr = mNextNodePtr;
            if ((mNextNodePtr = ListOp::GetNextPtr(theRetPtr)) == mHeadPtr[ListT]) {
                mNextNodePtr = 0;
            }
            return theRetPtr;
        }

        void Reset()
            { mNextNodePtr = mHeadPtr[ListT]; }
    private:
        NodeT*const* const mHeadPtr;
        NodeT*             mNextNodePtr;
    };
};

#endif /* QCDLLIST_H */
