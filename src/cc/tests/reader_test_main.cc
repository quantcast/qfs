//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/23
//
// Copyright 2008-2012 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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
// \brief Test program that reads data from a previously written file.
// Verifies that the data we get back from KFS is what we expect.
//----------------------------------------------------------------------------

#include <iostream>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fstream>
#include <fcntl.h>
#include "libclient/KfsClient.h"

#define MIN_FILE_SIZE 2048
#define MAX_FILE_SIZE (4096 * 8)
#define MAX_FILE_NAME_LEN 256

using std::cout;
using std::endl;
using std::ifstream;

using namespace KFS;
KfsClient * gKfsClient;

bool doFileOps(char *testDataFile, char *dirname, int seqNum, int numIter);
bool compareData(char *dst, char *src, int numBytes);
void generateData(char *testDataFile, char *buf, int numBytes);

int
main(int argc, char **argv)
{
    char dirname[256];

    if (argc < 3) {
        cout << "Usage: " << argv[0] << " <test-data-file> <kfs-client-properties file>\n"
             << "       The properties file has as contents the following:\n"
             << "         metaServer.name = <hostname>\n"
             << "         metaServer.port = <port\n";
        exit(0);
    }

    gKfsClient = Connect(argv[2]);
    if (!gKfsClient) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }

    srand(100);

    strcpy(dirname, "/dir1");

    if (doFileOps(argv[1], dirname, 0, 1) < 0) {
        cout << "File ops failed" << endl;
    }

    cout << "Test passed" << endl;
}

bool
doFileOps(char *testDataFile, char *parentDir, int seqNum, int numIter)
{
    char *dataBuf, *kfsBuf;
    int numRead, numBytes = 0;
    int i, fd, res = 0;
    int seekOffStart, toRead;
    char fileName[MAX_FILE_NAME_LEN];

    numBytes = 8192;

    dataBuf = new char[numBytes];
    generateData(testDataFile, dataBuf, numBytes);

    memset(fileName, 0, MAX_FILE_NAME_LEN);
    snprintf(fileName, MAX_FILE_NAME_LEN, "%s/foo.%d",
             parentDir, seqNum);

    fd = gKfsClient->Open(fileName, O_RDONLY);
    if (fd < 0) {
        cout << "Open failed: " << endl;
        exit(0);
    }

    kfsBuf = new char[numBytes];
    memcpy(kfsBuf, dataBuf, numBytes);

    numRead = gKfsClient->Read(fd, kfsBuf, numBytes);

    cout << "Full seq. read (numRead =" << numRead << "): " << " expect: " << numBytes << endl;
    if (numRead != numBytes)
        goto out;

    if (!compareData(kfsBuf, dataBuf, numRead)) {
        cout << "Data mismatch...offset: " << 0 << endl;
        goto out;
    }

    for (i = 0; i < numIter; i++) {
        seekOffStart = rand() % numBytes;
        toRead = rand() % numBytes;

        if (seekOffStart + toRead > numBytes) {
            toRead = numBytes - seekOffStart;
        }

        cout << "seeking to: " << seekOffStart << " and reading: " << toRead << endl;

        if (gKfsClient->Seek(fd, seekOffStart, SEEK_SET) < 0) {
            cout << "Seek to offset: " << seekOffStart << " failed" << endl;
            goto out;
        }
        numRead = gKfsClient->Read(fd, kfsBuf, toRead);
        if (numRead != toRead) {
            cout << "Expect to read: " << toRead << " but got: " << numRead << endl;
            goto out;
        }
        if (!compareData(kfsBuf, dataBuf + seekOffStart, numRead)) {
            cout << "Data mismatch...offset: " << seekOffStart << endl;
            goto out;
        }
    }

    res = 0;

  out:

    delete [] kfsBuf;
    delete [] dataBuf;

    gKfsClient->Close(fd);

    return res;
}

void
generateData(char *testDataFile, char *buf, int numBytes)
{
    int i;
    ifstream ifs(testDataFile);

    if (!ifs) {
        cout << "Unable to open test data file" << endl;
        exit(0);
    }

    for (i = 0; i < numBytes; ++i) {
        ifs >> buf[i];
        if (ifs.eof()) {
            cout << "Test-data file is too small (" << i << " vs. data= " << numBytes << endl;
            exit(0);
        }
    }

}

bool
compareData(char *dst, char *src, int numBytes)
{
    int i;

    for (i = 0; i < numBytes; i++) {
        if (dst[i] == src[i])
            continue;
        cout << "Mismatch at index: " << i << endl;
        return false;
    }
    return true;
}

