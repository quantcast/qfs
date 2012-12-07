//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/01/29
//
// Copyright 2011-2012 Quantcast Corp.
// Author: Mike Ovsainnikov
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
// \brief Sorted linear hash table unit and performance tests.
//
//----------------------------------------------------------------------------

#include "common/LinearHash.h"
#include "common/PoolAllocator.h"

#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#include <iostream>
#include <memory>
#include <set>

#ifdef _USE_STD_DEQUE
#include <deque>
#endif

typedef int64_t MyKey;
struct MyKVPair
{
    typedef MyKey Key;
    typedef MyKey Val;

    MyKey key;

    MyKVPair(const Key& key, const Val& /* val */)
        : key(key)
        {}
    Key& GetKey()             { return key; }
    const Key& GetKey() const { return key; }
    Val& GetVal()             { return key; }
    const Val& GetVal() const { return key; }
};

#ifdef _USE_STD_DEQUE
struct MyDynArray : public std::deque<KFS::SingleLinkedList<MyKVPair>*>
{
    value_type& PopBack()
    {
        if (! empty()) {
            pop_back();
        }
        return back();
    }
    bool IsEmpty() const
        { return empty(); }
    size_t GetSize() const
        { return size(); }
    size_t PushBack(const value_type& v)
    {
        push_back(v);
        return size();
    }
    value_type& Front()
        { return front(); }
    value_type& Back()
        { return back(); }
};
#else
typedef KFS::DynamicArray<KFS::SingleLinkedList<MyKVPair>*, 22> MyDynArray;
#endif

template<typename T>
class Allocator
{
public:
    T* allocate(size_t n)
    {
        if (n != 1) {
            abort();
        }
        return reinterpret_cast<T*>(mAlloc.Allocate());
    }
    void deallocate(T* ptr, size_t n)
    {
        if (n != 1) {
            abort();
        }
        mAlloc.Deallocate(ptr);
    }
    static void construct(T* ptr, const T& other)
        {  new (ptr) T(other); }
    static void destroy(T* ptr)
        { ptr->~T(); }
    template <typename TOther>
    struct rebind {
        typedef Allocator<TOther> other;
    };
private:
    typedef KFS::PoolAllocator<
            sizeof(T),         // size_t TItemSize,
            size_t(8)   << 20, // size_t TMinStorageAlloc,
            size_t(512) << 20, // size_t TMaxStorageAlloc,
            true               // bool   TForceCleanupFlag
    > Alloc;
    Alloc mAlloc;
};

typedef KFS::LinearHash<
    MyKVPair,
    KFS::KeyCompare<MyKey>,
    MyDynArray,
    Allocator<MyKVPair>
> MySLH;

using namespace std;
typedef set<MyKey> MySet;

static void
TestFailed()
{
    abort();
}

static void
Verify(const MySet& set, MySLH& ht)
{
    if (set.size() != ht.GetSize()) {
        TestFailed();
    }
    for (MySet::const_iterator it = set.begin(); it != set.end(); ++it) {
        const MyKey* const p = ht.Find(*it);
        if (! p || *p != *it) {
            TestFailed();
        }
    }
    ht.First();
    for (const MyKVPair* p; (p = ht.Next()); ) {
        if (set.find(p->GetKey()) == set.end()) {
            TestFailed();
        }
    }
}

int
main(int argc, char** argv)
{
    if (argc <= 1 || (!strcmp(argv[1], "-h") || !strcmp(argv[1], "--help"))) {
        return 0;
    }

    MySLH ht;
    bool inserted = false;
    // Unit test.
    if (! ht.Insert(500, 500, inserted) || ! inserted) {
        TestFailed();
    }
    if (! ht.Insert(100, 100, inserted) || ! inserted) {
        TestFailed();
    }
    if (ht.Find(3)) {
        TestFailed();
    }
    ht.Clear();

    MySet myset;
    const int kRandTestSize = 100 * 1000;
    const unsigned int kSeed = 10;
    srandom(kSeed);
    for (int i = 0; i < kRandTestSize; i++) {
        const MyKey r = (MyKey)random();
        inserted = 0;
        if (*(ht.Insert(r, r, inserted)) != r) {
            TestFailed();
        }
        if (myset.insert(r).second != inserted) {
            TestFailed();
        }
        // Verify(myset, ht);
    }
    Verify(myset, ht);
    cout << "inserted: " << ht.GetSize() << " of " << kRandTestSize << "\n";

    srandom(kSeed);
    for (int i = 0; i < kRandTestSize; i++) {
        const MyKey r = (MyKey)random();
        if (i % 3 != 0) {
            continue;
        }
        const size_t rht  = ht.Erase(r);
        if (! rht) {
            const MyKey* const p = ht.Find(r);
            if (p) {
                TestFailed();
            }
        }
        const size_t rset = myset.erase(r);
        if (rht != rset) {
            TestFailed();
        }
        // Verify(myset, ht);
    }
    Verify(myset, ht);
    cout << "removed: size: " << ht.GetSize() <<
        " of " << kRandTestSize << "\n";

    srandom(kSeed);
    for (int i = 0; i < kRandTestSize; i++) {
        const MyKey r = (MyKey)random();
        const MyKey* const res = ht.Find(r);
        if (res && *res != r) {
            TestFailed();
        }
        if ((myset.find(r) != myset.end()) != (res != 0)) {
            TestFailed();
        }
    }

    // Performance test.
    ht.Clear();
    clock_t s = clock();
    int k = 0;
    inserted = false;
    const int nk = argc > 1 ? (int)atof(argv[1]) : (1<<27) - (1<<16);
    for (MyKey i = 1000 * 1000 + 345; k < nk; i += 33, k++) {
        if (! ht.Insert(i, i, inserted) || ! inserted) {
            abort();
        }
    }
    clock_t e = clock();
    cout << k << " " << double(e - s)/CLOCKS_PER_SEC << "\n";
    s = clock();
    k = 0;
    for (MyKey i = 1000 * 1000 + 345; k < nk; i += 33, k++) {
        if (! ht.Find(i)) {
            abort();
        }
    }
    e = clock();
    cout << k << " " << double(e - s)/CLOCKS_PER_SEC << "\n";
    s = clock();
    k = 0;
    ht.First();
    int64_t t = 0;
    for (const MyKVPair* p; (p = ht.Next()); k++) {
        // cout << p->Key() << "\n";
        t += p->GetKey();
    }
    e = clock();
    cout << k << " " << double(e - s)/CLOCKS_PER_SEC << " " << t << "\n";
    cout << "press any key and then enter to continue\n";
    string str;
    cin >> str;
    s = clock();
    k = 0;
    for (MyKey i = 1000 * 1000 + 345; k < nk; i += 33, k++) {
        if (ht.Erase(i) != 1) {
            abort();
        }
    }
    e = clock();
    cout << k << " " << double(e - s)/CLOCKS_PER_SEC << "\n";
    if (! ht.IsEmpty()) {
        abort();
    }
    return 0;
}
