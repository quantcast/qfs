//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2007/09/26
// Author: Sriram Rao
//
// Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
// Copyright 2007-2008 Kosmix Corp.
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
// \brief A simple shell that lets users navigate KFS directory hierarchy.
//
//----------------------------------------------------------------------------

#include "kfsshell.h"
#include "libclient/KfsClient.h"
#include "common/MsgLogger.h"

#include <iostream>
#include <cerrno>
#include <map>
#include <memory>
#include <iomanip>
#include <unistd.h>

namespace KFS
{
namespace tools
{
using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::vector;
using std::string;
using std::auto_ptr;
using std::getline;
using std::flush;


typedef map <string, cmdHandler> CmdHandlers;

static CmdHandlers handlers;

static void setupHandlers();

bool hasFArg(uint8_t flag);
bool hasSArg(uint8_t flag);
bool isCorrectFSArgument(uint8_t flag, string const& serverHost, int port,
                         char const* config);

/// @retval: status code from executing the last command
static int processCmds(KfsClient *client, bool quietMode,
                       int nargs, const char **cmdLine);

static int
kfsshell_main(int argc, char **argv)
{
    string              serverHost;
    int                 port        = 20000;
    bool                help        = false;
    bool                quietMode   = false;
    MsgLogger::LogLevel logLevel    = MsgLogger::kLogLevelINFO;
    const char*         logLevelStr = 0;
    const char*         config      = 0;

    uint8_t flag = 0x00; // default value is 0
    int optchar;
    while ((optchar = getopt(argc, argv, "hqs:p:vf:l:")) != -1) {
        switch (optchar) {
            case 's':
                serverHost = optarg;
                flag = flag | 0x01;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'h':
                help = true;
                break;
            case 'v':
                logLevel    = MsgLogger::kLogLevelDEBUG;
                logLevelStr = 0;
                break;
            case 'q':
                quietMode = true;
                break;
            case 'f':
                config = optarg;
                flag = flag | 0x02;
                break;
            case 'l':
                logLevelStr = optarg;
                break;
            default:
                cout << "Unrecognized flag : " << char(optchar);
                help = true;
                break;
        }
    }

    if (help || !isCorrectFSArgument(flag, serverHost, port, config)) {
        cout << "Usage: " << argv[0]
             << " either -f or -s can be used, if both exist, -f will be used\n"
                " {-s <meta server name>}\n"
                " [-p <port> (default 20000)]\n"
                " [-q [cmd]]\n"
                " {-f <config file name>}\n"
                " [-l <log level> (DEBUG|INFO|NOTICE|WARN|ERROR|FATAL)]\n"
                "Starts an interactive client shell to QFS.\n"
                "  -q: switches to execution in quiet mode.\n"
                " cmd: command to execute, only in quiet mode.\n";
        return 1;
    }

    MsgLogger::Init(0, logLevel, 0, 0, logLevelStr);
    KfsClient* kfsClient = NULL;
    if (hasFArg(flag)) {
        kfsClient = Connect(config);
    } else if(hasSArg(flag)) {
        kfsClient = KfsClient::Connect(serverHost, port, config);
    }
    if (! kfsClient) {
        cout << "qfs client failed to initialize\n";
        return 1;
    }
    auto_ptr<KfsClient> cleanup(kfsClient);
    setupHandlers();

    return processCmds(kfsClient, quietMode,
        argc - optind, (const char **) &argv[optind]) == 0 ? 0 : 1;
}

// return true if -f arg exists
bool hasFArg(uint8_t flag) {
    return (flag & 0x02) != 0;
}

// return true if -s arg exists
bool hasSArg(uint8_t flag) {
    return (flag & 0x01) != 0;
}

bool isCorrectFSArgument(uint8_t flag, string const& serverHost, int port, char const* config) {
    if (hasFArg(flag)) {
        // -f argument exist, return true
        return true;
    }

    if (hasSArg(flag)) {
        // -s argument exist
        if (serverHost.empty() || port <= 0) {
            return false;
        }
        return true;
  }
  // return false if neither -f nor -s exists
  return false;
}

void printCmds()
{
    for (CmdHandlers::const_iterator
            it = handlers.begin(); it != handlers.end(); ++it) {
        cout << it->first << "\n";
    }
}

int handleHelp(KfsClient *client, const vector<string> &args)
{
    printCmds();
    return 0;
}

int handleExit(KfsClient *client, const vector<string> &args)
{
    exit(0);
    return 0;
}

void setupHandlers()
{
    handlers["cd"] = handleCd;
    handlers["changeReplication"] = handleChangeReplication;
    handlers["cp"] = handleCopy;
    handlers["ls"] = handleLs;
    handlers["mkdir"] = handleMkdirs;
    handlers["mv"] = handleMv;
    handlers["rmdir"] = handleRmdir;
    handlers["rm"] = handleRm;
    handlers["stat"] = handleFstat;
    handlers["pwd"] = handlePwd;
    handlers["help"] = handleHelp;
    handlers["append"] = handleAppend;
    handlers["finfo"] = fileInfo;
    handlers["cinfo"] = chunkInfo;
    handlers["chmod"] = handleChmod;
    handlers["chown"] = handleChown;
    handlers["chgrp"] = handleChgrp;
    handlers["quit"] = handleExit;
    handlers["exit"] = handleExit;
}

int processCmds(KfsClient *client, bool quietMode, int nargs, const char **cmdLine)
{
    string s, cmd;
    int retval = 0;

    for (; ;) {
        if (quietMode) {
            // Turn off prompt printing when quiet mode is enabled;
            // this allows scripting with KfsShell
            if (nargs <= 0) {
                break;
            }
            s.clear();
            for (int i = 0; i < nargs; i++) {
                if (! s.empty()) {
                    s += " ";
                }
                s += cmdLine[i];
            }
            nargs = 0;
        } else {
            cout << "QfsShell> " << flush;
            if (! getline(cin, s)) {
                break;
            }
        }

        // buf contains info of the form: <cmd>{<args>}
        // where, <cmd> is one of kfs cmds
        string::size_type curr;

        // get rid of leading spaces
        curr = s.find_first_not_of(" \t");
        s.erase(0, curr);
        curr = s.find(' ');
        if (curr != string::npos) {
            cmd.assign(s, 0, curr);
        } else {
            cmd = s;
        }
        // extract out the args
        vector<string> args;
        while (curr != string::npos) {
            string component;

            // curr points to a ' '
            curr++;
            const string::size_type next = s.find(' ', curr);
            if (next != string::npos)
                component.assign(s, curr, next - curr);
            else
                component.assign(s, curr, string::npos);

            if (component != "")
                args.push_back(component);
            curr = next;
        }

        CmdHandlers::const_iterator h = handlers.find(cmd);
        if (h == handlers.end()) {
            cout << "Unknown cmd: " << cmd << endl;
            cout << "Supported cmds are: " << endl;
            printCmds();
            cout << "Type <cmd name> --help for command specific help" << endl;
            continue;
        }

        retval = ((*h).second)(client, args);
    }
    return retval;
}
}} // KFS::tools

int
main(int argc, char **argv)
{
    return KFS::tools::kfsshell_main(argc, argv);
}
