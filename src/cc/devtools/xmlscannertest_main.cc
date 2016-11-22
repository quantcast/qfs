//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/10/24
// Author: Mike Ovsainnikov
//
// Copyright 2015,2016 Quantcast Corporation. All rights reserved.
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
// Xml scanner test.
//
//----------------------------------------------------------------------------

#include "common/XmlScanner.h"

#include <iostream>
#include <string>

namespace KFS
{

using std::cerr;
using std::cin;
using std::cout;
using std::string;


class XmlScannerTest
{
public:
    XmlScannerTest()
        : mKey(),
          mValue(),
          mFunc(*this, mKey, mValue, 10 << 10, 10 << 10)
    {
        mKey.reserve(1 << 10);
        mValue.reserve(1 << 10);
    }
    bool operator()(
        const string& inKey,
        const string& inVal)
    {
        cout << "tag: " << inKey << " value: " << inVal << "\n";
        return true;
    }
    int Scan()
    {
        Iterator theIt;
        if (! mFunc.Scan(theIt)) {
            cerr << "parse error at: " << mFunc.GetKey() << "\n";
            return 1;
        }
        return 0;
    }
private:
    class Iterator
    {
    public:
        int Next() const
        {
            return cin.get();
        }
    };
    string                                           mKey;
    string                                           mValue;
    XmlScanner::KeyValueFunc<string, XmlScannerTest> mFunc;
};

}

    int
main(
    int    /* inArgCount */,
    char** /* inArgs */)
{
    KFS::XmlScannerTest theScanner;
    return theScanner.Scan();
}
