//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/05/18
// Author: Mike Ovsainnikov
//
// Copyright 2011 Quantcast Corp.
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
// Simple singly linked list.
//
//----------------------------------------------------------------------------

#ifndef SINGLE_LINKED_LIST_H
#define SINGLE_LINKED_LIST_H

namespace KFS
{

template<typename T>
class SingleLinkedList
{
public:
    typedef T value_type;

    SingleLinkedList()
        : mData(),
          mNextPtr(0)
        {}
    SingleLinkedList(
        const SingleLinkedList& inNode)
        : mData(inNode.mData),
          mNextPtr(inNode.mNextPtr)
        {}
    SingleLinkedList(
        const T&          inData,
        SingleLinkedList* inNextPtr = 0)
        : mData(inData),
          mNextPtr(inNextPtr)
        {}
    SingleLinkedList& operator=(
        const SingleLinkedList& inNode)
    {
        mData    = inNode.mData;
        mNextPtr = inNode.mNextPtr;
        return *this;
    }
    T& GetData()
        { return mData; }
    const T& GetData() const
        { return mData; }
    SingleLinkedList*& GetNextPtr()
        { return mNextPtr; }
    SingleLinkedList* const & GetNextPtr() const
        { return mNextPtr; }
    SingleLinkedList& Reverse()
    {
        SingleLinkedList* theRetPtr = 0;
        SingleLinkedList* theCurPtr = this;
        do {
            SingleLinkedList* const theNextPtr = theCurPtr->mNextPtr;
            theCurPtr->mNextPtr = theRetPtr;
            theRetPtr = theCurPtr;
            theCurPtr = theNextPtr;
        } while (theCurPtr);
        return *theRetPtr;
    }

private:
    T                 mData;
    SingleLinkedList* mNextPtr;
};

}

#endif /* SINGLE_LINKED_LIST_H */
