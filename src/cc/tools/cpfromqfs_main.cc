//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/23
// Author: Sriram Rao
//         Mike Ovsiannikov
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
// \brief Tool that copies out a file/directory from KFS to the local file
// system.  This tool is analogous to restore, such as, restore a previously
// backed up directory from KFS.
//
//----------------------------------------------------------------------------

#include "libclient/KfsClient.h"
#include "common/MsgLogger.h"

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

#include <iostream>
#include <cerrno>
#include <limits>

namespace KFS
{

using std::cout;
using std::cerr;
using std::vector;
using std::min;
using std::max;
using std::numeric_limits;

class CpFromKfs
{
public:
    CpFromKfs()
        : mKfsClient(0),
          mSkipHolesFlag(false),
          mFailShortReadsFlag(true),
          mStart(-1),
          mStop(-1),
          mMaxRead(numeric_limits<int64_t>::max()),
          mReadAhead(-1),
          mBufSize(0),
          mAllocBufSize(0),
          mReadExitCount(-1),
          mKfsBuf(0)
        {}
    ~CpFromKfs()
    {
        delete mKfsClient;
        delete [] mKfsBuf;
    }

    int Run(int argc, char **argv);

private:
    KfsClient* mKfsClient;
    bool       mSkipHolesFlag;
    bool       mFailShortReadsFlag;
    chunkOff_t mStart;
    chunkOff_t mStop;
    chunkOff_t mMaxRead;
    int        mReadAhead;
    int        mBufSize;
    int        mAllocBufSize;
    int        mReadExitCount;
    char*      mKfsBuf;

    // Given a kfsdirname, restore it to dirname.  Dirname will be created
    // if it doesn't exist.
    int RestoreDir(string dirname, string kfsdirname);

    // Given a kfsdirname/filename, restore it to dirname/filename.  The
    // operation here is simple: read the file from KFS and dump it to filename.
    //
    int RestoreFile(string kfspath, string localpath);

    // does the guts of the work
    int RestoreFile2(string kfsfilename, string localfilename);

    void AddDirSlash(string& dir)
    {
        if (dir.empty() || dir[dir.length() - 1] != '/') {
            dir += "/";
        }
    }
};

int
CpFromKfs::Run(int argc, char **argv)
{
    string              kfsPath;
    string              localPath;
    string              serverHost;
    int                 port       = -1;
    bool                helpFlag   = false;
    MsgLogger::LogLevel logLevel   = MsgLogger::kLogLevelINFO;
    int                 maxRetry   = -1;
    int                 retryDelay = -1;
    int                 opTimeout  = -1;
    const char*         config     = 0;
    int                 optchar;

    while ((optchar = getopt(argc, argv, "d:hp:s:k:a:b:w:r:R:D:T:X:F:Svf:M:")) != -1) {
        switch (optchar) {
            case 'd':
                localPath = optarg;
                break;
            case 's':
                serverHost = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'k':
                kfsPath = optarg;
                break;
            case 'h':
                helpFlag = true;
                break;
            case 'S':
                mSkipHolesFlag = true;
                break;
            case 'v':
                logLevel = MsgLogger::kLogLevelDEBUG;
                break;
            case 'a':
                mStart = (chunkOff_t)atof(optarg);
                break;
            case 'b':
                mStop = (chunkOff_t)atof(optarg);
                break;
            case 'M':
                mMaxRead = (chunkOff_t)atof(optarg);
                break;
            case 'w':
                mBufSize = (int)atof(optarg);
                break;
            case 'r':
                mReadAhead = (int)atof(optarg);
                break;
            case 'R':
                maxRetry = (int)atof(optarg);
                break;
            case 'D':
                retryDelay = (int)atof(optarg);
                break;
            case 'T':
                opTimeout = (int)atof(optarg);
                break;
            case 'X':
                mReadExitCount = (int)atof(optarg);
                break;
            case 'F':
                mFailShortReadsFlag = atoi(optarg) != 0;
                break;
            case 'f':
                config = optarg;
                break;
            default:
                helpFlag = true;
                break;
        }
    }

    if (helpFlag ||
            kfsPath.empty() ||
            localPath.empty() ||
            serverHost.empty() ||
            port < 0 ||
            (mStart >= 0 && mStop >= 0 && mStart >= mStop)) {
        cerr << "Usage: " << argv[0] << "\n"
            " -s -- meta server name\n"
            " -p -- meta server port\n"
            " -k -- qfs source path\n"
            " -d -- local path; \"-\" means stdout\n"
            " [-v]       -- versbose logging\n"
            " [-S]       -- skip holes\n"
            " [-w]       -- write buffer size; default optimal for a file\n"
            " [-a]       -- copy start file offset, default 0\n"
            " [-b]       -- copy finish files offset, default -1 -- eof\n"
            " [-r]       -- read ahead size, default 0 -- no read ahead\n"
            " [-R]       -- op retry count, default -1 -- qfs client default\n"
            " [-D]       -- op retry delay, default -1 -- qfs client default\n"
            " [-T]       -- op timeout, in seconds, default -1 -- qfs "
                            "client default\n"
            " [-F {0|1}] -- fail short reads (partial sparse file support),"
                            "default 1\n"
            " [-X n]     -- debugging: call exit(1) after n read calls\n"
            " [-f file]  -- configuration file name\n"
            " [-M ]      -- maximum number of bytes to read per file\n"
        ;
        return (1);
    }

    MsgLogger::Init(0, logLevel);
    mKfsClient = KfsClient::Connect(serverHost, port, config);
    if (! mKfsClient) {
        cerr << "qfs client failed to initialize" << "\n";
        return 1;
    }
    mKfsClient->SetDefaultFullSparseFileSupport(! mFailShortReadsFlag);
    if (maxRetry > 0) {
        mKfsClient->SetMaxRetryPerOp(maxRetry);
    }
    if (retryDelay > 0) {
        mKfsClient->SetRetryDelay(retryDelay);
    }
    if (opTimeout > 0) {
        mKfsClient->SetDefaultIOTimeout(opTimeout);
    }
    if (mReadAhead >= 0) {
        mKfsClient->SetDefaultReadAheadSize(mReadAhead);
    }

    int         ret;
    KfsFileAttr attr;
    if ((ret = mKfsClient->Stat(kfsPath.c_str(), attr)) < 0) {
        cerr << "QFS: " << kfsPath << ": " << ErrorCodeToStr(ret) << "\n";
        return ret;
    }

    if (attr.isDirectory) {
        if (localPath == "-") {
            ret = -EISDIR;
            cerr << kfsPath << ": " << ErrorCodeToStr(ret) << "\n";
        } else {
            ret = RestoreDir(kfsPath, localPath);
        }
    } else {
        ret = RestoreFile(kfsPath, localPath);
    }
    return ret;
}

int
CpFromKfs::RestoreFile(string kfsPath, string localPath)
{
    struct stat statInfo;

    // Check if destination is directory. If it is then copy file into it.
    if (localPath != "-" &&
            stat(localPath.c_str(), &statInfo) == 0 &&
            S_ISDIR(statInfo.st_mode)) {
        // Get kfs file name.
        const string::size_type slash = kfsPath.rfind('/');
        string filename;
        if (slash != string::npos) {
            filename.assign(kfsPath, slash+1, string::npos);
        } else {
            filename = kfsPath;
        }
        return RestoreFile2(kfsPath, localPath + "/" + filename);
    }
    return RestoreFile2(kfsPath, localPath);
}

int
CpFromKfs::RestoreDir(string kfsdirname, string dirname)
{
    int res;
    vector<KfsFileAttr> fileInfo;
    vector<KfsFileAttr>::size_type i;
    struct stat statInfo;
#ifdef KFS_OS_NAME_SUNOS
    const int dirPerms = S_IRWXU|S_IRWXG|S_IRWXO;
#elif defined(ALLPERMS)
    const int dirPerms = ALLPERMS;
#else
    const int dirPerms = 0777;
#endif

    if ((res = mKfsClient->ReaddirPlus(kfsdirname.c_str(), fileInfo)) < 0) {
        cerr << kfsdirname << ": " << ErrorCodeToStr(res) << "\n";
        return res;
    }

    if ((stat(dirname.c_str(), &statInfo) != 0 ||
            ! S_ISDIR(statInfo.st_mode)) &&
            mkdir(dirname.c_str(), dirPerms)) {
        const int err = errno;
        cerr << dirname << ": " << strerror(err) << "\n";
        return err;
    }
    AddDirSlash(dirname);
    AddDirSlash(kfsdirname);
    for (i = 0; i < fileInfo.size() && res == 0; ++i) {
        if (fileInfo[i].isDirectory) {
            if (fileInfo[i].filename == "." || fileInfo[i].filename == "..") {
                continue;
            }
            res = RestoreDir(kfsdirname + fileInfo[i].filename,
                             dirname + fileInfo[i].filename);
        } else {
            res = RestoreFile2(kfsdirname + fileInfo[i].filename,
                               dirname + fileInfo[i].filename);
        }
    }
    return res;
}

int
CpFromKfs::RestoreFile2(string kfsfilename, string localfilename)
{
    const int kfsfd = mKfsClient->Open(kfsfilename.c_str(), O_RDONLY);
    if (kfsfd < 0) {
        cerr << kfsfilename << ": " << ErrorCodeToStr(kfsfd) << "\n";
        return kfsfd;
    }

    const int localFd = localfilename == "-" ?
        dup(1) : open(localfilename.c_str(),
            O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR|S_IWUSR);
    if (localFd < 0) {
        const int err = errno;
        cerr << localfilename << ": " << strerror(err) << "\n";
        mKfsClient->Close(kfsfd);
        return err;
    }

    if (mSkipHolesFlag) {
        mKfsClient->SkipHolesInFile(kfsfd);
    }
    int theSize;
    if (mBufSize <= 0) {
        theSize = mKfsClient->GetReadAheadSize(kfsfd);
    } else {
        theSize = mBufSize;
    }
    if (theSize <= 0) {
        theSize = 1 << 20;
    }
    if (theSize != mAllocBufSize || ! mKfsBuf) {
        delete [] mKfsBuf;
        mKfsBuf = new char[theSize];
        mAllocBufSize = theSize;
    }

    chunkOff_t pos = max(chunkOff_t(0), mStart);
    if (pos > 0) {
        const chunkOff_t nPos = mKfsClient->Seek(kfsfd, pos, SEEK_SET);
        if (nPos < 0 || nPos != pos) {
            const int err = nPos < 0 ? (int)nPos : -EINVAL;
            cerr << localfilename << ": " <<
                ErrorCodeToStr(err) << " seek: " << pos << " " << nPos <<
            "\n";
            mKfsClient->Close(kfsfd);
            close(localFd);
            return (nPos < 0 ? (int)nPos : -EINVAL);
        }
    }
    if (mStop >= 0) {
        mKfsClient->SetEOFMark(kfsfd, mStop);
    }

    chunkOff_t rem = mMaxRead;
    int        err = 0;
    while (0 < rem) {
        const int nRead = mKfsClient->Read(kfsfd, mKfsBuf,
            (size_t)min(rem, (chunkOff_t)mAllocBufSize));
        if (nRead <= 0) {
            if (nRead < 0) {
                err = nRead;
                cerr << kfsfilename << ": " << ErrorCodeToStr(err) << "\n";
            }
            break;
        }
        if (mReadExitCount > 0 && --mReadExitCount == 0) {
            KFS_LOG_STREAM_INFO <<
                "exiting, remaining read count: " << mReadExitCount <<
            KFS_LOG_EOM;
            exit(1);
        }
        pos += nRead;
        rem -= nRead;
        for (const char* p = mKfsBuf, * const e = p + nRead; p < e; ) {
            const ssize_t n = write(localFd, p, e - p);
            if (n < 0) {
                if (errno != EINTR && errno != EAGAIN) {
                    err = errno;
                    break;
                }
            } else {
                p += n;
            }
        }
        if (err != 0) {
            cerr << localfilename << ": " << strerror(err) << "\n";
            break;
        }
        if (mStop > 0 && pos >= mStop) {
            KFS_LOG_STREAM_INFO <<
                "stopping: pos: " << pos << " stop: " << mStop <<
            KFS_LOG_EOM;
            break;
        }
    }
    if (rem <= 0) {
        KFS_LOG_STREAM_INFO <<
            "stopping: max read: " << mMaxRead <<
        KFS_LOG_EOM;
    }

    mKfsClient->Close(kfsfd);
    close(localFd);
    return err;

}

} // namespace KFS

int
main(int argc, char **argv)
{
    KFS::CpFromKfs cpFromKfs;
    return cpFromKfs.Run(argc, argv);
}
