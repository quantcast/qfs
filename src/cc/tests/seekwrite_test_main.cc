//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/23
//
// Copyright 2008-2011 Quantcast Corp.
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
// \brief Test to verify zero-filling of data on a read:
//  - Write 512 bytes; seek to a chunk boundary; write M bytes
//  - Try to read N bytes, where we expect the client libary to
// zero-fill 4096-512 bytes.
//----------------------------------------------------------------------------

#include <iostream>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <fstream>
#include <algorithm>
#include "libclient/KfsClient.h"

#define MIN_FILE_SIZE 2048
#define MAX_FILE_SIZE (65536 * 3)
#define MAX_FILE_NAME_LEN 256

const int KFS_DATA_BUF_SIZE = 65536;

using std::cout;
using std::endl;
using std::min;
using std::ifstream;

using namespace KFS;
KfsClient * gKfsClient;
bool doMkdir(char *dirname);
bool doFileOps(char *testDataFile, char *dirname, int seqNum, int numIter);
bool compareData(const char *dst, const char *src, int numBytes);
void generateData(char *testDataFile, char *buf, int numBytes);

void
doWrite(int fd, off_t offset, int numBytes, const char *data);

void
doRead(int fd, off_t offset, int numBytes, const char *src);

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

    doMkdir(dirname);
    if (doFileOps(argv[1], dirname, 0, 1) < 0) {
        cout << "File ops failed" << endl;
    }

    cout << "Test passed" << endl;
}

bool
doMkdir(char *dirname)
{
    int fd;

    cout << "Making dir: " << dirname << endl;

    fd = gKfsClient->Mkdir(dirname);
    if (fd < 0) {
        cout << "Mkdir failed: " << fd << endl;
        return false;
    }
    cout << "Mkdir returned: " << fd << endl;
    return fd > 0;
}


bool
doFileOps(char *testDataFile,
               char *parentDir, int seqNum, int numIter)
{
    char *dataBuf, *kfsBuf, *kfsBuf1;
    int numRead, numBytes = 0;
    int fd;
    off_t seekPt;
    char fileName[MAX_FILE_NAME_LEN];

    while (numBytes < MIN_FILE_SIZE) {
        numBytes = rand() % MAX_FILE_SIZE;
    }

    numBytes = 65536;

    cout << "Writing " << numBytes << endl;

    dataBuf = new char[numBytes];
    generateData(testDataFile, dataBuf, numBytes);

    memset(fileName, 0, MAX_FILE_NAME_LEN);
    snprintf(fileName, MAX_FILE_NAME_LEN, "%s/foo.%d",
             parentDir, seqNum);

    fd = gKfsClient->Create(fileName);
    if (fd < 0) {
        cout << "Create failed: " << endl;
        exit(0);
    }

    kfsBuf = new char[KFS_DATA_BUF_SIZE];
    memcpy(kfsBuf, dataBuf, numBytes);

    kfsBuf1 = new char[KFS_DATA_BUF_SIZE];

#if 1
    seekPt = ((off_t) 5000) * 1024 * 1024;
    gKfsClient->Seek(fd, seekPt, SEEK_CUR);

    if (gKfsClient->Write(fd, kfsBuf, 512) < 0) {
        cout << "Write failed: " << endl;
        exit(0);
    }
    gKfsClient->Close(fd);

    fd = gKfsClient->Open(fileName, O_RDONLY);
    numRead = gKfsClient->Read(fd, kfsBuf1, 512);

    cout << "Read returned: " << numRead << endl;
    if (numRead < 0) {
        exit(0);
    }
    gKfsClient->Seek(fd, seekPt, SEEK_SET);
    numRead = gKfsClient->Read(fd, kfsBuf1, 512);

    cout << "Read returned: " << numRead << endl;
    if (numRead > 0) {
        exit(0);
    }

    // Seek to a chunk boundary
    if (gKfsClient->Write(fd, kfsBuf, numBytes - 4096) < 0) {
        cout << "Write failed: " << endl;
        exit(0);
    }
#else
    srand(100);

/*
    doWrite(fd, 49141, 3664, kfsBuf);
    doWrite(fd, 38278, 3809, kfsBuf);
    doWrite(fd, 20965, 2377, kfsBuf);
    doWrite(fd, 46345, 1458, kfsBuf);
    doRead(fd, 20965, 2377, dataBuf);
    doRead(fd, 20965, 2377, dataBuf);
    doRead(fd, 20965, 2377, dataBuf);
    doWrite(fd, 18823, 3415, kfsBuf);
    doWrite(fd, 211, 3848, kfsBuf);
    doWrite(fd, 80213, 483, kfsBuf);
    doWrite(fd, 34846, 2485, kfsBuf);
    doRead(fd, 46345, 1458, dataBuf);
*/
    // doWrite(fd, 0, 65536, kfsBuf);
    // doWrite(fd, 65536, 65536, kfsBuf);
    // doWrite(fd, 65536*2, 65536, kfsBuf);

    doWrite(fd, 20965, 2377, kfsBuf);

    doWrite(fd, 65536*2, 2377, kfsBuf);

    doWrite(fd, 0, 8192, kfsBuf);

    doRead(fd, 0, 4096, kfsBuf);

    doWrite(fd, 40345, 8192, &kfsBuf[40345]);
    doWrite(fd, 66000, 15385, &kfsBuf[66000-65536]);
    cout << "Writes are done..." << endl;

    if (fd > 0) {
        cout << "Done..." << endl;
        return 0;
    }

    doRead(fd, 65536, 6000, dataBuf);
    doRead(fd, 20965, 2377, dataBuf);
    doRead(fd, 64000, 2377, dataBuf);

    if (fd > 0) {
        cout << "Test passed.." << endl;
        return 0;
    }


    // Seek to a random location

    for (int i = 0; i < 1024; i++) {
        seekPt = rand() % MAX_FILE_SIZE;
        gKfsClient->Seek(fd, seekPt, SEEK_SET);

        cout << "Seeking to: " << seekPt << " and writing/reading: " << numBytes << endl;

        // write some data there
        if (gKfsClient->Write(fd, kfsBuf, numBytes) < 0) {
            cout << "Write failed: " << endl;
            exit(0);

        }
        // fill it with some junk so that it should get overwritten
        memset(kfsBuf1, '5', numBytes);

        gKfsClient->Seek(fd, seekPt, SEEK_SET);
        if (gKfsClient->Read(fd, kfsBuf1, numBytes) < 0) {
            cout << "Read failed: " << endl;
            exit(0);
        }

        if (!compareData(kfsBuf1, dataBuf, numBytes)) {
            cout << "Mismatch..." << endl;
            exit(0);
        }

    }

#endif
    cout << "write is done...." << endl;
    gKfsClient->Close(fd);

    fd = gKfsClient->Open(fileName, O_RDONLY);
    if (fd < 0) {
        cout << "Open failed: " << endl;
        exit(0);
    }
    cout << "Sync done...seek'ing" << endl;

    gKfsClient->Seek(fd, 0, SEEK_SET);

    cout << "Seek done...seq read'ing" << endl;

    numRead = gKfsClient->Read(fd, kfsBuf, 16384);

    cout << "Full seq. read (numRead =" << numRead << "): " << " expect: " << numBytes << endl;
    if (numRead != numBytes)
        return -1;

    delete [] kfsBuf;
    delete [] dataBuf;
    delete [] kfsBuf1;

    return 0;
}

void
doWrite(int fd, off_t offset, int numBytes, const char *data)
{
    gKfsClient->Seek(fd, offset, SEEK_SET);

    // write some data there
    if (gKfsClient->Write(fd, data, numBytes) < 0) {
        cout << "Write failed: " << endl;
        exit(0);
    }
}

void
doRead(int fd, off_t offset, int numBytes, const char *src)
{
    char *kfsBuf1 = new char[numBytes];
    int bytesCompared, toCompare;
    off_t start;

    // fill it with some junk so that it should get overwritten
    memset(kfsBuf1, '5', numBytes);

    gKfsClient->Seek(fd, offset, SEEK_SET);
    if (gKfsClient->Read(fd, kfsBuf1, numBytes) < 0) {
        cout << "Read failed: " << endl;
        exit(0);
    }

    start = offset;
    bytesCompared = 0;
    while (bytesCompared < numBytes) {
        toCompare =  min((int)(KFS_DATA_BUF_SIZE - (start % KFS_DATA_BUF_SIZE)),
                         (int)(numBytes - bytesCompared));
        if (!compareData(kfsBuf1 + bytesCompared, src + (start % KFS_DATA_BUF_SIZE),
                         toCompare)) {
            cout << "Mismatch on read starting at: " << offset << endl;
            exit(0);
        }
        start += toCompare;
        bytesCompared += toCompare;
    }
    delete [] kfsBuf1;
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

    for (i = 0; i < numBytes; ++i) {
        buf[i] = 'a' + (char) (i % 26);
    }
}

bool
compareData(const char *dst, const char *src, int numBytes)
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

