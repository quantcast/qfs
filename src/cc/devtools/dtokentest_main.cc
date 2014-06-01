//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/5/20
// Author: Mike Ovsiannikov
//
// Copyright 2014 Quantcast Corp.
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

#include "kfsio/DelegationToken.h"
#include "kfsio/Base64.h"

#include <iostream>
#include <sstream>

using std::cout;
using std::string;
using std::ostringstream;
using namespace KFS;

    int
main(
    int    inArgCount,
    char** inArgPtr)
{
    int theErrorCount = 0;
    for (int p = 0; p < 2; p++) {
        string theKey("this is a test");
        string theSessionKey("session key test");
        if (p) {
            theKey.resize(48);
            theSessionKey.resize(48);
        }
        string theTestKeys[2];
        for (int i = 0; i < 2; i++) {
            ostringstream theStream;
            const bool theOkFlag = DelegationToken::WriteToken(
                theStream,
                123,
                456,
                789,
                1000,
                0,
                1000,
                theKey.data(),
                (int)theKey.size(),
                0,
                true,
                2000,
                i == 0 ? theSessionKey.data() : 0,
                i == 0 ? theSessionKey.size() : 0
            );
            const string theTokenAndKey = theStream.str();
            if (theOkFlag) {
                const size_t thePos = theTokenAndKey.rfind(' ');
                if (thePos != string::npos) {
                    theTestKeys[i] = theTokenAndKey.substr(thePos + 1);
                } else {
                    cout << "error: invalid token and key format\n";
                    theErrorCount++;
                }
            } else {
                cout << "error: create token and key failure\n";
                theErrorCount++;
            }
            cout <<
                i << ": " << theOkFlag << "\n" <<
                theTokenAndKey << "\n" <<
                theTestKeys[i].size() << " " << theTestKeys[i] << "\n";
        }
        if (theTestKeys[0].empty() || theTestKeys[1].empty()) {
            continue;
        }
        string theErrMsg;
        CryptoKeys::Key theDecryptedKey;
        int theStatus = DelegationToken::DecryptSessionKeyFromString(
            theSessionKey.data(),
            theSessionKey.size(),
            theTestKeys[0].data(),
            theTestKeys[0].size(),
            theDecryptedKey,
            &theErrMsg
        );
        cout << "status: " << theStatus << " " << theErrMsg << "\n";
        if (theStatus) {
            theErrorCount++;
            continue;
        }
        char theBuf[Base64::GetEncodedMaxBufSize(CryptoKeys::Key::GetSize())];
        const int theLength = Base64::Encode(
            theDecryptedKey.GetPtr(), theDecryptedKey.GetSize(),
            theBuf
        );
        if ((int)sizeof(theBuf) < theLength) {
            cout << "invalid endoded length: " <<
                sizeof(theBuf) << " < " << theLength << "\n";
            abort();
        }
        (cout << theLength << " "
            ).write(theBuf, theLength < 0 ? 0 : theLength) << "\n";
        if (theLength != (int)theTestKeys[1].size() ||
                theTestKeys[1].compare(0, theLength, theBuf) != 0) {
            cout << "error: key mismatch\n";
            theErrorCount++;
        }
    }
    if (theErrorCount == 0) {
        cout << "test passed\n";
    } else {
        cout << "test failed\n";
    }
    return theErrorCount;
}
