//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/06/11
//
// Author: Sriram Rao
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
// \brief This works only on Linux:  Whenever chunks are moved to
// lost+found, it is likely because we had trouble reading data off
// disk.  We'd like to leave the "bad" sectors pinned while freeing up
// the other readable sectors.  This takes out the bad sectors from
// allocation while improving disk health.  On Linux, we use XFS
// ioctl's to punch holes for all the readable data from a file, while
// leaving the regions that return EIO pinned on disk.
//
//----------------------------------------------------------------------------

#include <dirent.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <boost/scoped_array.hpp>
#include <iostream>

#ifdef KFS_OS_NAME_LINUX
#include <xfs/xfs.h>
#endif

using std::cout;
using std::endl;
using std::string;
using boost::scoped_array;

static void trimFile(string &fn, int64_t filesz, bool verbose);

int main(int argc, char **argv)
{
    char optchar;
    bool help = false;
    string pathname = "";
    int res, count;
    int ndaysOld = 5;
    struct dirent **entries;
    struct stat statBuf;
    bool verbose = false;

    while ((optchar = getopt(argc, argv, "hvd:n:")) != -1) {
        switch (optchar) {
            case 'd': 
                pathname = optarg;
                break;
            case 'v':
                verbose = true;
                break;
            case 'n':
                ndaysOld = atoi(optarg);
                break;
            case 'h':
            default:
                help = true;
                break;
        }
    }

    if ((help) || (pathname == "")) {
        cout << "Usage: " << argv[0] << " -d <pathname> {-n <days>} {-v} " << endl;
        cout << "where, files older than n days are pruned" << endl;
        exit(-1);
    }
    
    time_t now = time(0);

    res = stat(pathname.c_str(), &statBuf);
    if (res < 0) {
        cout << "Unable to open: " << pathname << endl;
        exit(-1);
    }
    if (S_ISREG(statBuf.st_mode)) {
        if ((now - statBuf.st_mtime) < (ndaysOld * 24 * 60 * 60)) {
            cout << "Ignoring file: " << pathname << " since it is not old enough for pruning" << endl;
            exit(0);
        }
        trimFile(pathname, statBuf.st_size, verbose);
        exit(0);
    }

    count = scandir(pathname.c_str(), &entries, 0, alphasort);
    if (count < 0) {
        cout << "Unable to open: " << pathname << endl;
        exit(-1);
    }
    

    for (int i = 0; i < count; i++) {
        string fn = pathname;
        fn = fn + "/" + entries[i]->d_name;
        res = stat(fn.c_str(), &statBuf);
        free(entries[i]);
        if ((res < 0) || (!S_ISREG(statBuf.st_mode)) ||
            (fn.rfind(".corrupt") != string::npos)) {
            // either it is not a file or it has been trimmed earlier
            // and renamed to .corrupt
            continue;
        }
        if ((now - statBuf.st_mtime) < (ndaysOld * 24 * 60 * 60)) {
            cout << "Ignoring file: " << fn << " since it is not old enough for pruning" << endl;
            continue;
        }
        trimFile(fn, statBuf.st_size, verbose);
    }
    free(entries);
    exit(0);
}

#ifdef KFS_OS_NAME_LINUX

static const int oneMB = 1 << 20;
static const int fourK = 4096;

static int trimPart(int fd, int64_t startP, int64_t endP, int blkSz, int &numFreed, int &numKept);

static void trimFile(string &fn, int64_t filesz, bool verbose)
{
    int fd, res;
    int numFreed = 0, numKept = 0;

    fd = open(fn.c_str(), O_RDWR | O_DIRECT);
    // fd = open(fn.c_str(), O_RDONLY | O_DIRECT);
    if (fd < 0) {
        cout << "Unable to open: " << fn << endl;
        return;
    }
    bool shouldPreserveFile = false;
    int64_t filePos = 0;
    int dummy;
    while (1) {
        // remember the filePos, since on a successful read it will be updated
        filePos = lseek(fd, 0, SEEK_CUR);
        // read in big chunks; if any fail, we will switch down to
        // reading in 4k and pinning only the unreadable 4k blocks
        res = trimPart(fd, filePos, filePos + oneMB, oneMB, numFreed, dummy);
        if ((res == 0) || (filePos > filesz)) {
            // hit EOF
            break;
        }
        if (res < 0) {
            filePos = lseek(fd, filePos, SEEK_SET);
            shouldPreserveFile = true;
            trimPart(fd, filePos, filePos + oneMB, fourK, numFreed, numKept);
        }
    }
    close(fd);
    if (shouldPreserveFile) {
        string corruptFn = fn + ".corrupt";
        rename(fn.c_str(), corruptFn.c_str());
        cout << "Renamed: " << fn << " -> " << corruptFn << endl;
        cout << "Filesize: " << filePos << endl;
        cout << "Num 4k-blocks kept: " << numKept << endl;
        cout << "Num 4k-blocks freed: " << numFreed << endl;
    } else {
        cout << "Non-corrupt file: " << fn << " ; so nuking" << endl;
        unlink(fn.c_str());
    }
}

static int
trimPart(int fd, int64_t startP, int64_t endP, int blkSz, int &numFreed, int &numKept)
{
    static char *data = NULL;
    xfs_flock64_t theFree = {0};
    int64_t currP = startP;
    int retval = 1, res;

    if (data == NULL) {
        data = (char *) memalign(fourK, oneMB * sizeof(char));
    }
    while (currP < endP) {
        if (currP >= endP)
            break;
        res = read(fd, data, blkSz);
        if (res == 0)
            return (retval == -1 ? -1 : 0);
        if (res < 0) {
            // we couldn't read a block; so, reset the position and skip over the block 
            // we failed to read
            currP = lseek(fd, currP + blkSz, SEEK_SET);
            cout << "Couldn't read block at: " << currP - blkSz << " of len = " << blkSz << endl;
            numKept += (blkSz / 4096);
            // file needs to be preserved
            retval = -1;
            continue;
        }
        // got data, so free the block and punch a hole in the file
        theFree.l_whence = 0;
        theFree.l_start = currP;
        theFree.l_len = res;
        xfsctl(0, fd, XFS_IOC_UNRESVSP64, &theFree);
        numFreed += (res / 4096);
        currP += res;
    }
    return retval;
}

#else
static void trimFile(string &fn, int64_t filesz, bool verbose)
{

}
#endif
