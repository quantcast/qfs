//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/08/20
// Author: Mike Ovsiannikov
//
// Copyright 2012 Quantcast Corp.
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
// \brief Kfs tool.
//
//----------------------------------------------------------------------------

#include "FileSystem.h"
#include "common/MsgLogger.h"
#include "qcdio/QCUtils.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

namespace KFS
{
namespace tools
{

using std::string;
using std::cout;
using std::cerr;
using std::vector;
using std::pair;
using std::max;

class KfsTool
{
public:
    KfsTool()
        : mIoBufferSize(6 << 20),
          mIoBufferPtr(new char[mIoBufferSize])
        {}
    ~KfsTool()
    {
        delete [] mIoBufferPtr;
    }
    int Run(
        int    inArgCount,
        char** inArgsPtr)
    {
        string              theMetaHost;
        string              theMetaPort;
        bool                theHelpFlag = false;
        MsgLogger::LogLevel theLogLevel = MsgLogger::kLogLevelINFO;

        int theOpt;
        while ((theOpt = getopt(inArgCount, inArgsPtr, "hs:p:v")) != -1) {
            switch (theOpt) {
                case 's':
                    theMetaHost = optarg;
                    break;
                case 'p':
                    theMetaPort = optarg;
                    break;
                case 'h':
                    theHelpFlag = true;
                    break;
                case 'v':
                    theLogLevel = MsgLogger::kLogLevelDEBUG;
                    break;
                default:
                    cout << "Unrecognized option : " << char(theOpt) << "\n";
                    theHelpFlag = true;
                    break;
            }
        }

        if (theHelpFlag || (theMetaHost.empty() && ! theMetaPort.empty())) {
            cout <<
                "Usage: " << (inArgCount > 0 ? inArgsPtr[0] : "") << "\n"
                " [-s <meta server host>]\n"
                " [-p <meta server port>]\n"
            ;
            return 1;
        }
        MsgLogger::Init(0, theLogLevel);

        if (! theMetaHost.empty()) {
            string theUri = "qfs://" + theMetaHost;
            if (theMetaPort.empty()) {
                theUri += ":";
                theUri += theMetaPort;
            }
            const int theErr = FileSystem::SetDefault(theUri);
            if (theErr != 0) {
                cerr << theUri << ": " <<
                    FileSystem::GetStrError(-theErr) << "\n";
                return 1;
            }
        }
        int theErr = 0;
        if (optind < inArgCount) {
            const char* const theCmdPtr = inArgsPtr[optind];
            if (strcmp(theCmdPtr, "-cat") == 0) {
                theErr = Cat(inArgsPtr + optind + 1, inArgCount - optind - 1);
            } else {
                cerr << "unsupported option: " << theCmdPtr << "\n";
                theErr = EINVAL;
            }
        }
        return (theErr == 0 ? 0 : 1);
    }
private:
    static const char* GlobError(
        int inError)
    {
        switch (inError) {
            case GLOB_NOSPACE:
                return "out of memory";
            case GLOB_ABORTED:
                return "read error";
            case GLOB_NOMATCH:
                return "no matches found";
            case 0:
                return "no error";
            default:
                return "unspecified error";
        }
    }
    typedef vector<pair<FileSystem*, vector<string> > > GlobResult;
    static int Glob(
        char**       inArgsPtr,
        int          inArgCount,
        GlobResult&  outResult)
    {
        outResult.reserve(outResult.size() + max(0, inArgCount));
        for (int i = 0; i < inArgCount; i++) {
            const string theArg   = inArgsPtr[i];
            FileSystem*  theFsPtr = 0;
            string       thePath;
            int          theErr   = FileSystem::Get(theArg, theFsPtr, &thePath);
            if (theErr) {
                cerr << theArg <<
                    ": " << FileSystem::GetStrError(theErr) << "\n";
                return theErr;
            }
            glob_t    theGlobRes = {0};
            const int kGlobFlags = GLOB_NOSORT | GLOB_NOCHECK;
            const int theRet     = theFsPtr->Glob(
                thePath,
                kGlobFlags,
                0, // the err func.
                &theGlobRes
            );
            if (theRet == 0) {
                outResult.resize(outResult.size() + 1);
                outResult.back().first = theFsPtr;
                string thePrefix;
                if (thePath.empty() || thePath[0] != '/') {
                    string theCwd;
                    if ((theErr = theFsPtr->GetCwd(theCwd))) {
                        cerr << theArg <<
                            ": " << theFsPtr->StrError(theErr) << "\n";
                        return theErr;
                    }
                    thePrefix += theCwd;
                    if (! thePrefix.empty() && *thePrefix.rbegin() != '/' &&
                            (theGlobRes.gl_pathc > 1 ||
                            (theGlobRes.gl_pathc == 1 && 
                            theGlobRes.gl_pathv[0][0] != 0))) {
                        thePrefix += "/";
                    }
                }
                vector<string>& theResult = outResult.back().second;
                theResult.reserve(theGlobRes.gl_pathc);
                for (size_t i = 0; i < theGlobRes.gl_pathc; i++) {
                    theResult.push_back(thePrefix + theGlobRes.gl_pathv[i]);
                }
            } else {
                cerr << inArgsPtr[i] << ": " << GlobError(theRet) <<
                    " " << theRet << "\n";
            }
            globfree(&theGlobRes);
            if (theRet != 0) {
                return theRet;
            }
        }
        return 0;
    }
    int Cat(
        char** inArgsPtr,
        int    inArgCount)
    {
        GlobResult theResult;
        const int theErr = Glob(inArgsPtr, inArgCount, theResult);
        if (theErr) {
            return theErr;
        }
        for (GlobResult::const_iterator theFsIt = theResult.begin();
                theFsIt != theResult.end();
                ++theFsIt) {
            FileSystem& theFs = *(theFsIt->first);
            for (vector<string>::const_iterator theIt = theFsIt->second.begin();
                    theIt != theFsIt->second.end();
                    ++theIt) {
                const int theFd = theFs.Open(*theIt, O_RDONLY, 0);
                if (theFd < 0) {
                    cerr << theFs.GetUri() << *theIt <<
                        ": " << theFs.StrError(theFd) << "\n";
                    return theFd;
                }
                int theErr = 0;
                for (; ;) {
                    const ssize_t theNRead =
                        theFs.Read(theFd, mIoBufferPtr, mIoBufferSize);
                    if (theNRead == 0) {
                        break;
                    }
                    if (theNRead < 0) {
                        theErr = (int)theNRead;
                        cerr << theFs.GetUri() << *theIt <<
                            ": " << theFs.StrError(theErr) << "\n";
                        break;
                    }
                    if (! cout.write(mIoBufferPtr, theNRead)) {
                        theErr = errno;
                        cerr << theFs.GetUri() << *theIt <<
                            ": stdout: " << QCUtils::SysError(theFd) << "\n";
                        break;
                    }
                }
                theFs.Close(theFd);
                if (theErr != 0) {
                    return theErr;
                }
            }
        }
        return 0;
    }
private:
    size_t mIoBufferSize;
    char*  mIoBufferPtr;
private:
    KfsTool(const KfsTool& inTool);
    KfsTool& operator=(const KfsTool& inTool);
};

}
}

int
main(int argc, char** argv)
{
    KFS::tools::KfsTool theTool;
    return theTool.Run(argc, argv);
}
