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
// \brief Program that reads sequentially from a file in KFS.
//
//----------------------------------------------------------------------------

#include <iostream>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <fstream>
#include <time.h>
#include <boost/scoped_array.hpp>
#include "libclient/KfsClient.h"

using std::cout;
using std::endl;
using std::ifstream;
using std::string;

using namespace KFS;
KfsClient * gKfsClient;

static off_t doRead(const string &kfspathname,
    int numMBytes, int readSizeBytes, int cliBufSize, int readAhead, double sleepSec);

int
main(int argc, char **argv)
{
    char optchar;
    string kfspathname = "";
    char *kfsPropsFile = NULL;
    int numMBytes = 1, readSizeBytes = 65536;
    bool help = false;
    const char* logLevel = "INFO";
    int cliBufSize = -1;
    int readAhead = -1;
    double sleepSec = -1;

    while ((optchar = getopt(argc, argv, "f:p:m:b:s:a:S:v")) != -1) {
        switch (optchar) {
            case 'f':
                kfspathname = optarg;
                break;
            case 'b':
                readSizeBytes = atoi(optarg);
                break;
            case 'p':
                kfsPropsFile = optarg;
                break;
            case 'm':
                numMBytes = atoi(optarg);
                break;
            case 'v':
                logLevel = "DEBUG";
                break;
            case 's':
                cliBufSize = atoi(optarg);
                break;
            case 'S':
                sleepSec = atof(optarg);
                break;
            case 'a':
                readAhead = atoi(optarg);
                break;
            default:
                cout << "Unrecognized flag: " << optchar << endl;
                help = true;
                break;
        }
    }

    if (help || (kfsPropsFile == NULL) || (kfspathname == "")) {
        cout << "Usage: " << argv[0] << " -p <Kfs Client properties file>"
                " -m <# of MB to read> -b <read size in bytes> -f <Kfs file>"
                " -S <sleep sec. between reads> -s <kfs buffer size> [-v]\n"
                "       The properties file has as contents the following:\n"
                "         metaServer.name = <hostname>\n"
                "         metaServer.port = <port\n";
        exit(0);
    }

    cout << "Doing reads to: " << kfspathname << " # MB = " << numMBytes;
    cout << " # of bytes per read: " << readSizeBytes << endl;

    gKfsClient = Connect(kfsPropsFile);
    if (!gKfsClient) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(-1);
    }
    gKfsClient->SetLogLevel(logLevel);

    string kfsdirname, kfsfilename;
    string::size_type slash = kfspathname.rfind('/');

    if (slash == string::npos) {
        cout << "Bad kfs path: " << kfsdirname << endl;
        exit(-1);
    }

    kfsdirname.assign(kfspathname, 0, slash);
    kfsfilename.assign(kfspathname, slash + 1, kfspathname.size());

    struct timeval startTime, endTime;
    double timeTaken;
    off_t bytesRead;

    gettimeofday(&startTime, NULL);

    bytesRead = doRead(kfspathname, numMBytes, readSizeBytes, cliBufSize, readAhead, sleepSec);

    gettimeofday(&endTime, NULL);

    timeTaken = (endTime.tv_sec - startTime.tv_sec) +
        (endTime.tv_usec - startTime.tv_usec) * 1e-6;

    cout << "Read rate: " << (((double) bytesRead * 8.0) / timeTaken) / (1024.0 * 1024.0) << " (Mbps)" << endl;
    cout << "Read rate: " << ((double) (bytesRead) / timeTaken) / (1024.0 * 1024.0) << " (MBps)" << endl;

    return 0;
}

off_t
doRead(const string &filename, int numMBytes,
    int readSizeBytes, int cliBufSize, int readAhead, double sleepSec)
{
    const int mByte = 1024 * 1024;
    boost::scoped_array<char> dataBuf;
    int res, bytesRead = 0, nMBytes = 0, fd;
    off_t nread = 0;

    dataBuf.reset(new char [readSizeBytes]);

    fd = gKfsClient->Open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        cout << "Open failed: " << endl;
        exit(-1);
    }
    if (cliBufSize >= 0) {
        const size_t size = gKfsClient->SetIoBufferSize(fd, cliBufSize);
        cout << "Setting kfs buffer size to: "
            << cliBufSize << " got: " << size << endl;
    }
    if (readAhead >= 0) {
        const size_t size = gKfsClient->SetReadAheadSize(fd, readAhead);
        cout << "Setting kfs read ahead size to: "
            << readAhead << " got: " << size << endl;
    }
    struct timespec sleepTm;
    const bool doSleep = sleepSec > 0;
    if (doSleep) {
        sleepTm.tv_sec = time_t(sleepSec);
        sleepTm.tv_nsec = long((sleepSec - (double)sleepTm.tv_sec) * 1e9);
    }

    while (nread < numMBytes * mByte) {
        res = gKfsClient->Read(fd, dataBuf.get(), readSizeBytes);
        if (res != readSizeBytes)
            return (bytesRead + nMBytes * 1024 * 1024);
        nread += readSizeBytes;
        if (doSleep) {
            nanosleep(&sleepTm, 0);
        }
    }
    cout << "read of " << nread / (1024 * 1024) << " (MB) is done" << endl;
    gKfsClient->Close(fd);

    return nread;
}
