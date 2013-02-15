//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2007/09/05
// Author: Sriram Rao (Kosmix Corp.)
//
// Copyright 2012 Quantcast Corp.
// Copyright 2007 Kosmix Corp.
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
// \brief A sample C++ program that demonstrates QFS APIs.  To run
// this program, you need:
//   - qfs client libraries to link with
//   - a QFS deployment
//
// The cmake build system takes care of compiling this. Follow the DeveloperDoc
// in the top-level 'doc' directory for compile instructions.
//
// To see compile commandline (assuming QFS code is at ~/code/qfs, and it has
// already been compiled using instructions),
//   cd ~/code/qfs/build/examples/cc && make VERBOSE=1
//
//----------------------------------------------------------------------------

#include <iostream>
#include <fstream>
#include <cerrno>

extern "C" {
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
}

#include "libclient/KfsClient.h"
#include "libclient/KfsAttr.h"

using std::cout;
using std::cerr;
using std::endl;
using std::ifstream;
using std::string;
using std::vector;

KFS::KfsClient *gKfsClient;

// generate sample data for testing
void generateData(char *buf, int numBytes);

int
main(int argc, char **argv)
{
    string serverHost = "";
    int port = -1;
    bool help = false;
    char optchar;

    while ((optchar = getopt(argc, argv, "hp:s:")) != -1) {
        switch (optchar) {
            case 'p':
                port = atoi(optarg);
                break;
            case 's':
                serverHost = optarg;
                break;
            case 'h':
                help = true;
                break;
            default:
                cout << "Unrecognized flag " << optchar << endl;
                help = true;
                break;
        }
    }

    if (help || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port> "
             << endl;
        exit(0);
    }

    //
    // Get a handle to the KFS client object.  This is our entry into
    // the KFS namespace.
    //

    gKfsClient = KFS::Connect(serverHost, port);
    if (!gKfsClient) {
        cerr << "kfs client failed to initialize...exiting" << "\n";
        exit(-1);
    }

    // Make a directory /ctest
    string baseDir = "ctest";
    int res = gKfsClient->Mkdirs(baseDir.c_str());
    if (res < 0 && res != -EEXIST) {
        cout << "Mkdir failed: " << KFS::ErrorCodeToStr(res) << endl;
        exit(-1);
    }

    // What we just created better be a directory
    if (!gKfsClient->IsDirectory(baseDir.c_str())) {
        cout << "KFS doesn't think: " << baseDir << " is a dir!" << endl;
        exit(-1);
    }

    // Create a simple file with default replication (at most 3)
    string tempFilename = baseDir + "/foo.1";
    int fd;

    // fd is our file-handle to the file we are creating; this
    // file handle should be used in subsequent I/O calls on
    // the file.
    if ((fd = gKfsClient->Create(tempFilename.c_str())) < 0) {
        cout << "Create failed: " << KFS::ErrorCodeToStr(fd) << endl;
        exit(-1);
    }

    // Get the directory listings
    vector<string> entries;

    if ((res = gKfsClient->Readdir(baseDir.c_str(), entries)) < 0) {
        cout << "Readdir failed! " << KFS::ErrorCodeToStr(res) << endl;
        exit(-1);
    }

    cout << "Read dir returned: " << endl;
    for (size_t i = 0; i < entries.size(); i++) {
        cout << entries[i] << endl;
    }

    // write something to the file
    int numBytes = 2048;
    char *dataBuf = new char[numBytes];

    generateData(dataBuf, numBytes);

    // make a copy and write out using the copy; we keep the original
    // so we can validate what we get back is what we wrote.
    char *copyBuf = new char[numBytes];
    memcpy(copyBuf, dataBuf, numBytes);

    res = gKfsClient->Write(fd, copyBuf, numBytes);
    if (res != numBytes) {
        cout << "Was able to write only: " << res << " instead of " << numBytes << endl;
    }

    // flush out the changes
    gKfsClient->Sync(fd);

    // Close the file-handle
    gKfsClient->Close(fd);

    // Determine the file-size
    KFS::KfsFileAttr fileAttr;
    gKfsClient->Stat(tempFilename.c_str(), fileAttr);
    long size = fileAttr.fileSize;

    if (size != numBytes) {
        cout << "KFS thinks the file's size is: " << size << " instead of " << numBytes << endl;
    }

    // rename the file
    string newFilename = baseDir + "/foo.2";
    gKfsClient->Rename(tempFilename.c_str(), newFilename.c_str());

    if (gKfsClient->Exists(tempFilename.c_str())) {
        cout << tempFilename << " still exists after rename!" << endl;
        exit(-1);
    }

    // Re-create the file and try a rename that should fail...
    int fd1 = gKfsClient->Create(tempFilename.c_str());

    if (!gKfsClient->Exists(tempFilename.c_str())) {
        cout << " After rec-create..., " << tempFilename << " doesn't exist!" << endl;
        exit(-1);
    }

    gKfsClient->Close(fd1);

    // try to rename and don't allow overwrite
    if (gKfsClient->Rename(newFilename.c_str(), tempFilename.c_str(), false) == 0) {
        cout << "Rename  with overwrite disabled succeeded...error!" << endl;
        exit(-1);
    }

    // Remove the file
    gKfsClient->Remove(tempFilename.c_str());

    // Re-open the file
    if ((fd = gKfsClient->Open(newFilename.c_str(), O_RDWR)) < 0) {
        cout << "Open on : " << newFilename << " failed: " << KFS::ErrorCodeToStr(fd) << endl;
        exit(-1);
    }

    // read some bytes
    res = gKfsClient->Read(fd, copyBuf, 128);
    if (res != 128) {
        if (res < 0) {
            cout << "Read on : " << newFilename << " failed: " << KFS::ErrorCodeToStr(res) << endl;
            exit(-1);
        }
    }

    // Verify what we read matches what we wrote
    for (int i = 0; i < 128; i++) {
        if (dataBuf[i] != copyBuf[i]) {
            cout << "Data mismatch at : " << i << endl;
        }
    }
    delete[] dataBuf;

    // seek to offset 40
    gKfsClient->Seek(fd, 40);

    // Seek and verify that we are we think we are
    size = gKfsClient->Tell(fd);
    if (size != 40) {
        cout << "After seek, we are at: " << size << " should be at 40 " << endl;
    }

    gKfsClient->Close(fd);

    // remove the file
    gKfsClient->Remove(newFilename.c_str());

    // remove the dir
    if ((res = gKfsClient->Rmdir(baseDir.c_str()) < 0)) {
        cout << "Unable to remove: " << baseDir << " : " << KFS::ErrorCodeToStr(res) << endl;
    } else {
        cout << "Tests passed!" << endl;
    }
}

void generateData(char *buf, int numBytes)
{
    int i;

    srand(100);
    for (i = 0; i < numBytes; i++) {
        buf[i] = (char) ('a' + (rand() % 26));
    }
}

