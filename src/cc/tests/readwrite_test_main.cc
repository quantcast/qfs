//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/07/02
//
// Copyright 2008-2010 Quantcast Corp
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
// \brief Program that reads from one KFS and writes to another
//
//----------------------------------------------------------------------------

#include <iostream>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <fstream>
#include "libclient/KfsClient.h"

using std::cout;
using std::endl;
using std::ifstream;
using std::string;

using namespace KFS;

KfsClient *srcKfsClient, *dstKfsClient;
static off_t doWrite(const string &kfspathname, int numMBytes, size_t writeSizeBytes);

void
getHostPort(const string &hp, string &host, int &port)
{
    string::size_type delim = hp.find(':');

    port = -1;
    if (delim == string::npos)
        return;
    host.assign(hp, 0, delim);
    string rest;

    rest.assign(hp, delim + 1, string::npos);
    port = atoi(rest.c_str());
}

int
main(int argc, char **argv)
{
    char optchar;
    string kfspathname = "";
    string srckfs, dstkfs;
    int numMBytes = 1;
    size_t writeSizeBytes = 65536;
    bool help = false;

    while ((optchar = getopt(argc, argv, "s:d:f:m:b:")) != -1) {
        switch (optchar) {
            case 'f':
                kfspathname = optarg;
                break;
            case 'b':
                writeSizeBytes = atoll(optarg);
                break;
            case 's':
                srckfs = optarg;
                break;
            case 'd':
                dstkfs = optarg;
                break;
            case 'm':
                numMBytes = atoi(optarg);
                break;
            default:
                cout << "Unrecognized flag: " << optchar << endl;
                help = true;
                break;
        }
    }

    if (help || (srckfs == "") || (dstkfs == "") || (kfspathname == "")) {
        cout << "Usage: " << argv[0] << " -s <src kfs> -d <dst kfs> "
             << " -m <# of MB to write> -b <write size in bytes> -f <Kfs file>\n"
             << "       The <src kfs> and <dst kfs> given in metahost:port format.\n";
        exit(0);
    }

    cout << "Doing writes to: " << kfspathname << " # MB = " << numMBytes;
    cout << " # of bytes per write: " << writeSizeBytes << endl;

    string host;
    int port;

    getHostPort(srckfs, host, port);
    srcKfsClient = Connect(host, port);
    if (!srcKfsClient) {
        cout << "kfs client" << srckfs << " failed to initialize...exiting" << endl;
        exit(-1);
    }

    getHostPort(dstkfs, host, port);
    dstKfsClient = Connect(host, port);
    if (!dstKfsClient) {
        cout << "kfs client " << dstkfs << " failed to initialize...exiting" << endl;
        exit(-1);
    }

    string kfsdirname, kfsfilename;
    string::size_type slash = kfspathname.rfind('/');

    if (slash == string::npos) {
        cout << "Bad kfs path: " << kfsdirname << endl;
        exit(-1);
    }

    kfsdirname.assign(kfspathname, 0, slash);
    kfsfilename.assign(kfspathname, slash + 1, kfspathname.size());
    dstKfsClient->Mkdirs(kfsdirname.c_str());

    struct timeval startTime, endTime;
    double timeTaken;
    off_t bytesWritten;

    gettimeofday(&startTime, NULL);

    bytesWritten = doWrite(kfspathname, numMBytes, writeSizeBytes);

    gettimeofday(&endTime, NULL);

    timeTaken = (endTime.tv_sec - startTime.tv_sec) +
        (endTime.tv_usec - startTime.tv_usec) * 1e-6;

    cout << "Write rate: " << (((double) bytesWritten * 8.0) / timeTaken) / (1024.0 * 1024.0) << " (Mbps)" << endl;
    cout << "Write rate: " << ((double) bytesWritten / timeTaken) / (1024.0 * 1024.0) << " (MBps)" << endl;
    return 0;
}

off_t
doWrite(const string &filename, int numMBytes, size_t writeSizeBytes)
{
    const size_t mByte = 1024 * 1024;
    char dataBuf[mByte];
    int res, ifd, ofd;
    size_t bytesWritten = 0, bytesRead;
    int nMBytes = 0;
    off_t nwrote = 0;

    if (writeSizeBytes > mByte) {
        cout << "Setting write size to: " << mByte << endl;
        writeSizeBytes = mByte;
    }

    for (bytesWritten = 0; bytesWritten < mByte; bytesWritten++) {
        dataBuf[bytesWritten] = 'a' + bytesWritten % 26;
    }
    ifd = srcKfsClient->Open(filename.c_str(), O_RDONLY);
    if (ifd < 0) {
        cout << "open failed: " << endl;
        exit(-1);
    }

    ofd = dstKfsClient->Create(filename.c_str());
    if (ofd < 0) {
        cout << "Create failed: " << endl;
        exit(-1);
    }

    for (nMBytes = 0; nMBytes < numMBytes; nMBytes++) {
        for (bytesWritten = 0; bytesWritten < mByte; bytesWritten += writeSizeBytes) {
            res = srcKfsClient->Read(ifd, dataBuf, writeSizeBytes);
            if (res == 0)
                break;
            bytesRead = res;
            res = dstKfsClient->Write(ofd, dataBuf, bytesRead);
            if (res == 0)
                break;

            nwrote += bytesRead;
        }
    }
    cout << "write of " << nwrote / (1024 * 1024) << " (MB) is done" << endl;
    srcKfsClient->Close(ifd);
    dstKfsClient->Close(ofd);

    return nwrote;
}

