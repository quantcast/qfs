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

#include <string>
#include <iostream>

namespace KFS
{
namespace tools
{

using std::string;
using std::cout;
using std::cerr;
using std::ostringstream;

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
                cerr << theUri << ": " << QCUtils::SysError(theErr) << "\n";
                return 1;
            }
        }
        int theErr = 0;
        for (int i = optind; i < inArgCount; i++) {
            const string theArg   = inArgsPtr[i];
            FileSystem*  theFsPtr = 0;
            string       thePath;
            theErr = FileSystem::Get(theArg, theFsPtr, &thePath);
            if (theErr) {
                cerr << theArg << ": " << QCUtils::SysError(theErr) << "\n";
                break;
            }
            glob_t    theGlobRes = {0};
            const int kGlobFlags = 0;
            const int theRet     = theFsPtr->Glob(
                thePath,
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

