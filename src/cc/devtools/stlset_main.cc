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
// \brief Stl set (red and black tree) performance test.
//
//----------------------------------------------------------------------------

#include "common/StdAllocator.h"

#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#include <set>
#include <iostream>
#include <memory>

typedef int64_t MyKey;

typedef std::set<
    MyKey,
    std::less<MyKey>,
    KFS::StdFastAllocator<MyKey>
> MySet;

using namespace std;

int main(int argc, char** argv)
{
    if (argc <= 1 || (!strcmp(argv[1], "-h") || !strcmp(argv[1], "--help"))) {
        return 0;
    }

    clock_t s = clock();
    MySet ht;
    int k = 0;
    const int nk = argc > 1 ? (int)atof(argv[1]) : (1<<27)-(1<<16);
    for (MyKey i = 1000 * 1000 + 345; k < nk; i += 33, k++) {
        ht.insert(i);
    }
    clock_t e = clock();
    cout << k << " " << double(e - s)/CLOCKS_PER_SEC << "\n";
    s = e;
    k = 0;
    for (MyKey i = 1000 * 1000 + 345; k < nk; i += 33, k++) {
        if (ht.find(i) == ht.end()) {
            abort();
        }
    }
    e = clock();
    cout << k << " " << double(clock() - s)/CLOCKS_PER_SEC << "\n";
    s = e;
    k = 0;
    int64_t t = 0;
    for (MySet::const_iterator it = ht.begin(); it != ht.end(); ++it, k++) {
        // cout << *it << "\n";
        t += *it;
    }
    e = clock();
    cout << k << " " << double(e - s)/CLOCKS_PER_SEC << " " << t << "\n";
    cout << "enter number to exit\n";
    cin >> k;
    return 0;
}
