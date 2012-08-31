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
// \brief Convert v1 chunks to the current version.
//
//----------------------------------------------------------------------------

#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>
#include <boost/scoped_array.hpp>
#include <iostream>
#include <sstream>

#include "kfsio/checksum.h"
#include "kfsio/Globals.h"
#include "common/MsgLogger.h"
#include "kfsio/FileHandle.h"
#include "Chunk.h"
#include "ChunkManager.h"

using std::cout;
using std::endl;
using std::string;
using std::ostringstream;
using boost::scoped_array;

string KFS::RestartChunkServer();
string KFS::RestartChunkServer()
{
    return string("not supported");
}

using namespace KFS;

// This structure is on-disk
struct DiskChunkInfoV1_t {
    DiskChunkInfoV1_t() : metaMagic (CHUNK_META_MAGIC), metaVersion(CHUNK_META_VERSION) { }
    DiskChunkInfoV1_t(kfsFileId_t f, kfsChunkId_t c, int64_t s, kfsSeq_t v) :
        metaMagic (CHUNK_META_MAGIC), metaVersion(CHUNK_META_VERSION),
        fileId(f), chunkId(c), chunkSize(s), chunkVersion(v) { }
    void SetChecksums(const uint32_t *checksums) {
        memcpy(chunkBlockChecksum, checksums, MAX_CHUNK_CHECKSUM_BLOCKS * sizeof(uint32_t));
    }
    int metaMagic;
    int metaVersion;

    kfsFileId_t fileId;
    kfsChunkId_t chunkId;
    int64_t  chunkSize; 
    uint32_t chunkBlockChecksum[MAX_CHUNK_CHECKSUM_BLOCKS];
    // some statistics about the chunk: 
    // -- version # has an estimate of the # of writes
    // -- track the # of reads
    // ...
    uint32_t chunkVersion;
    uint32_t numReads;
    char filename[MAX_FILENAME_LEN];
};

static void upgradeChunkFile(string chunkDir, string fn, bool verbose);
static string makeChunkFilename(const string &chunkDir, const DiskChunkInfo_t &chunkInfo);

int main(int argc, char **argv)
{
    char optchar;
    bool help = false;
    const char *chunkDir = NULL;
    int res, count;
    struct dirent **entries;
    struct stat statBuf;
    bool verbose = false;

    KFS::MsgLogger::Init(0, KFS::MsgLogger::kLogLevelINFO);

    while ((optchar = getopt(argc, argv, "hvd:")) != -1) {
        switch (optchar) {
            case 'd': 
                chunkDir = optarg;
                break;
            case 'v':
                verbose = true;
                break;
            case 'h':
            default:
                help = true;
                break;
        }
    }

    if ((help) || (chunkDir == NULL)) {
        cout << "Usage: " << argv[0] << " -d <chunkdir> {-v}" << endl;
        exit(-1);
    }
    
    res = scandir(chunkDir, &entries, 0, alphasort);
    if (res < 0) {
        cout << "Unable to open: " << chunkDir << endl;
        exit(-1);
    }

    count = res;
    for (int i = 0; i < count; i++) {
        string fn = chunkDir;
        fn = fn + "/" + entries[i]->d_name;
        res = stat(fn.c_str(), &statBuf);
        if ((res < 0) || (!S_ISREG(statBuf.st_mode)))
            continue;
        upgradeChunkFile(chunkDir, entries[i]->d_name, verbose);
        free(entries[i]);
    }
    free(entries);
    exit(0);
}

static void upgradeChunkFile(string chunkDir, string chunkfn, bool verbose)
{
    DiskChunkInfoV1_t chunkInfoV1;
    int fd, res;
    FileHandlePtr f;
    scoped_array<char> data;
    string fn, newfn;

    fn = chunkDir + "/" + chunkfn;

    fd = open(fn.c_str(), O_RDWR);
    if (fd < 0) {
        cout << "Unable to open: " << fn << endl;
        return;
    }
    f.reset(new FileHandle_t(fd));
    res = pread(fd, &chunkInfoV1, sizeof(DiskChunkInfoV1_t), 0);
    if (res < 0) {
        cout << "Unable to read chunkinfo for: " << fn << endl;
        return;
    }

    DiskChunkInfo_t chunkInfo(chunkInfoV1.fileId, chunkInfoV1.chunkId, 
                              chunkInfoV1.chunkSize, chunkInfoV1.chunkVersion);
    chunkInfo.SetChecksums(chunkInfoV1.chunkBlockChecksum);
    res = pwrite(fd, &chunkInfo, sizeof(DiskChunkInfo_t), 0);
    if (res < 0) {
        cout << "Unable to write chunkinfo for: " << fn << endl;
        return;
    }
    
    if (verbose) {
        cout << "fid: "<< chunkInfo.fileId << endl;
        cout << "chunkId: "<< chunkInfo.chunkId << endl;
        cout << "size: "<< chunkInfo.chunkSize << endl;
        cout << "version: "<< chunkInfo.chunkVersion << endl;
    }
    // upgrade the meta-data
    
    newfn = makeChunkFilename(chunkDir, chunkInfo);
    res = rename(fn.c_str(), newfn.c_str());
    if (res < 0)
        perror("rename");
}

string makeChunkFilename(const string &chunkDir, const DiskChunkInfo_t &chunkInfo)
{
    ostringstream os;

    os << chunkDir << '/' << chunkInfo.fileId << '.' << chunkInfo.chunkId << '.' << chunkInfo.chunkVersion;
    return os.str();
}
