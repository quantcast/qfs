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
#include <iomanip>
#include <unistd.h>

#include <readline/readline.h>
#include <readline/history.h>

namespace KFS
{
namespace tools
{
using std::cin;
using std::cout;
using std::cerr;
using std::endl;
using std::map;
using std::vector;
using std::string;
using std::getline;
using std::flush;


typedef map <string, cmdHandler> CmdHandlers;

static CmdHandlers handlers;
char history_file[512];

static void setupHandlers();

/// @retval: status code from executing the last command
static int processCmds(KfsClient* client, bool quietMode, int nargs,
    const char* const* cmdLine);

static int
kfsshell_main(int argc, char** argv)
{
    string              serverHost;
    int                 port        = 20000;
    bool                help        = false;
    bool                quietMode   = false;
    MsgLogger::LogLevel logLevel    = MsgLogger::kLogLevelINFO;
    const char*         logLevelStr = 0;
    const char*         config      = 0;

    int optchar;
    while ((optchar = getopt(argc, argv, "hqs:p:vf:l:")) != -1) {
        switch (optchar) {
            case 's':
                serverHost = optarg;
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
                break;
            case 'l':
                logLevelStr = optarg;
                break;
            default:
                cerr << "Unrecognized flag : " << char(optchar) << "\n";
                help = true;
                break;
        }
    }

    if (help || serverHost.empty() || port <= 0) {
        (help ? static_cast<ostream&>(cout) : static_cast<ostream&>(cerr)) <<
            "Usage: " << argv[0] <<
            " -s <meta server name>\n"
            " [-p <port> (default 20000)]\n"
            " [-q [cmd]]\n"
            " [-f <config file name>]\n"
            " [-l <log level> (DEBUG|INFO|NOTICE|WARN|ERROR|FATAL)]\n"
            "Starts an interactive client shell to QFS.\n"
            "  -q: switches to execution in quiet mode.\n"
            " cmd: command to execute, only in quiet mode.\n";
        return 1;
    }

    rl_initialize();  
    using_history();
    sprintf(history_file, "%s/%s", getenv("HOME"), ".qfs_history");
    read_history(history_file);

    MsgLogger::Init(0, logLevel, 0, 0, logLevelStr);
    setupHandlers();
    KfsClient* const kfsClient = KfsClient::Connect(serverHost, port, config);
    if (! kfsClient) {
        cerr << "qfs client failed to initialize\n";
        return 1;
    }
    const int res = processCmds(kfsClient, quietMode, argc - optind,
        argv + optind);
    delete kfsClient;
    return (0 == res ? 0 : 1);
}

void
printCmds()
{
    for (CmdHandlers::const_iterator
            it = handlers.begin(); it != handlers.end(); ++it) {
        cout << it->first << "\n";
    }
}

int
handleHelp(KfsClient *client, const vector<string> &args)
{
    printCmds();
    return 0;
}

static bool gStopFlag = false;

int
handleExit(KfsClient *client, const vector<string> &args)
{
    gStopFlag = true;
    return 0;
}

void
setupHandlers()
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

static char *gets_interactive() {
    rl_reset_screen_size();
    return readline("QfsShell> ");
}

int
processCmds(KfsClient* client, bool quietMode, int nargs,
    const char* const* cmdLine)
{
    string s, cmd;
    int retval = 0;

    while (! gStopFlag) {
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
            char* read_buffer = gets_interactive(); 
            s = read_buffer;
            if (s.length()) {
                add_history(s.data());
                if (write_history(history_file)) {
                    printf("failed to write history: %s\n", strerror(errno));
                }
            } else {
                    break;
            }
            rl_free(read_buffer);
            //cout << "QfsShell> " << flush;
            //if (! getline(cin, s)) {
            //    break;
            //}
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
            cerr << "Unknown cmd: " << cmd << endl;
            if (! quietMode) {
                cout << "Supported cmds are:\n";
                printCmds();
                cout << "Type <cmd name> --help for command specific help\n";
            }
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
