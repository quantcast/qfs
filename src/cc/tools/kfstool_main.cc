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

#include "libclient/KfsClient.h"
#include "libclient/kfsglob.h"
#include "common/MsgLogger.h"
#include "qcdio/QCUtils.h"

#include <unistd.h>
#include <stdlib.h>

#include <string>
#include <memory>

namespace KFS
{
namespace tools
{

using std::string;
using std::cout;
using std::auto_ptr;

class KfsTool
{
public:
    KfsTool()
        {}
    int Run(
        int    inArgCount,
        char** inArgsPtr)
    {
        string              theMetaHost;
        int                 theMetaPort = -1;
        bool                theHelpFlag = false;
        MsgLogger::LogLevel theLogLevel = MsgLogger::kLogLevelINFO;

        int theOpt;
        while ((theOpt = getopt(inArgCount, inArgsPtr, "hs:p:v")) != -1) {
            switch (theOpt) {
                case 's':
                    theMetaHost = optarg;
                    break;
                case 'p':
                    theMetaPort = atoi(optarg);
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

        if (theHelpFlag || theMetaHost.empty() || theMetaPort <= 0) {
            cout <<
                "Usage: " << (inArgCount > 0 ? inArgsPtr[0] : "") << "\n"
                " -s <meta server name>\n"
                " -p <port>\n"
            ;
            return 1;
        }

        MsgLogger::Init(0, theLogLevel);
        KfsClient* const theClientPtr = Connect(theMetaHost, theMetaPort);
        if (! theClientPtr) {
            cout << "kfs client intialization failure\n";
            return 1;
        }
        auto_ptr<KfsClient> theCleanup(theClientPtr);

        for (int i = optind; i < inArgCount; i++) {
            glob_t theGlobRes = {0};
            const int kGlobFlags = 0;
            const int theRet     = KfsGlob(
                *theClientPtr,
                inArgsPtr[i],
                kGlobFlags,
                0, // the err func.
                &theGlobRes
            );
            if (theRet == 0) {
                cout << inArgsPtr[i] << ": found: " <<
                    theGlobRes.gl_pathc << " matches\n";
                for (size_t i = 0; i < theGlobRes.gl_pathc; i++) {
                    cout << theGlobRes.gl_pathv[i] << "\n";
                }
            } else {
                cout << inArgsPtr[i] << ": " << GlobError(theRet) <<
                    " " << theRet << "\n";
            }
            globfree(&theGlobRes);
        }
        return 0;
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

