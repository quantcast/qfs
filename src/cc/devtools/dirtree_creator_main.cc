//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/05/09
//
// Copyright 2008-2012 Quantcast Corp.
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
// \brief This program can be used to evaluate the memory use on the metaserver
// by creating a directory hierarchy. For input, provide a file that
// lists the directory hierarchy to be created with the path to a
// complete file, one per line.
//
//----------------------------------------------------------------------------

#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <fstream>
#include "libclient/KfsClient.h"

using std::ios_base;
using std::cout;
using std::ifstream;
using std::string;

using namespace KFS;

int
main(int argc, char **argv)
{
    char        optchar;
    const char* kfsPropsFile = 0;
    const char* dataFile = 0;
    bool        help = false;
    ifstream    ifs;

    while ((optchar = getopt(argc, argv, "f:p:")) != -1) {
        switch (optchar) {
            case 'f':
                dataFile = optarg;
                break;
            case 'p':
                kfsPropsFile = optarg;
                break;
            default:
                cout << "Unrecognized flag: " << optchar << "\n";
                help = true;
                break;
        }
    }

    if (help || ! kfsPropsFile || ! dataFile) {
        cout << "Usage: " << argv[0] << " -p <Kfs Client properties file> "
             << " -f <data file>" << "\n";
        return 0;
    }

    ifs.open(dataFile, ios_base::in);
    if (!ifs) {
        cout << "Unable to open: " << dataFile << "\n";
        return 1;
    }

    KfsClient* const kfsClient = Connect(kfsPropsFile);
    if (! kfsClient ) {
        cout << "kfs client failed to initialize...exiting" << "\n";
        return 1;
    }

    int    fd    = 0;
    int    count = 0;
    string kfspathname;
    while (getline(ifs, kfspathname)) {
        string kfsdirname, kfsfilename;
        string::size_type slash = kfspathname.rfind('/');

        if (slash == string::npos) {
            cout << "Bad kfs path: " << kfsdirname << "\n";
            fd = 1;
            break;
        }
        kfsdirname.assign(kfspathname, 0, slash);
        kfsfilename.assign(kfspathname, slash + 1, kfspathname.size());
        if (kfsfilename.rfind(".crc") != string::npos) {
            continue;
        }
        ++count;
        if ((count % 10000) == 0) {
            cout << "Done with " << count << " non-crc files" << "\n";
        }
        if ((fd = kfsClient->Mkdirs(kfsdirname.c_str())) < 0) {
            cout << "Mkdir failed: " << kfsdirname <<
                ": " << ErrorCodeToStr(fd) << "\n";
            break;
        }
        fd = kfsClient->Create(kfspathname.c_str());
        if (fd < 0) {
            cout << "Create failed for path: " << kfspathname << " error: " <<
                ErrorCodeToStr(fd) << "\n";
            break;
        }
        fd = kfsClient->Close(fd);
    }
    delete kfsClient;

    return (fd != 0 ? 1 : 0);
}
