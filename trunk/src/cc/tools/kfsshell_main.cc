//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2007/09/26
// Author: Sriram Rao (Kosmix Corp.) 
//
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
// \brief A simple shell that lets users navigate KFS directory hierarchy.
// 
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>

#include "libkfsClient/KfsClient.h"
#include "common/log.h"
#include "tools/KfsShell.h"

#include <iostream>
#include <tr1/unordered_map>
using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::vector;
using std::string;

using namespace KFS;
using namespace KFS::tools;

typedef map <string, cmdHandler> CmdHandlers;
typedef map <string, cmdHandler>::iterator CmdHandlersIter;

CmdHandlers handlers;

static void setupHandlers();
static void processCmds(bool quietMode);

int
main(int argc, char **argv)
{
    string kfsdirname = "";
    string serverHost = "";
    int port = -1;
    bool help = false;
    bool quietMode = false;
    char optchar;

    while ((optchar = getopt(argc, argv, "hqs:p:")) != -1) {
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
            case 'q':
                quietMode = true;
                break;
            default:
                cout << "Unrecognized flag : " << optchar;
                help = true;
                break;
        }
    }

    if (help || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port> {-q}" << endl;
        exit(0);
    }

    KfsClient *kfsClient = KfsClient::Instance();
    kfsClient->Init(serverHost, port);
    if (!kfsClient->IsInitialized()) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }
    
    setupHandlers();

    processCmds(quietMode);

    return 0;
}

void printCmds()
{
    cout << "cd" << endl;
    cout << "changeReplication" << endl;
    cout << "cp" << endl;
    cout << "ls" << endl;
    cout << "mkdir" << endl;
    cout << "mv" << endl;
    cout << "rm" << endl;
    cout << "rmdir" << endl;
    cout << "pwd" << endl;
}

void handleHelp(const vector<string> &args)
{
    printCmds();
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
    // handlers["ping"] = handlePing;
    handlers["rm"] = handleRm;
    handlers["pwd"] = handlePwd;
    handlers["help"] = handleHelp;
}

void processCmds(bool quietMode)
{
    char buf[256];
    string cmd;

    while (1) {
        if (!quietMode) {
            // Turn off prompt printing when quiet mode is enabled;
            // this allows scripting with KfsShell
            cout << "KfsShell> ";
        }
        cin.getline(buf, 256);

        if (cin.eof())
            break;

        string s = buf, cmd;
        // buf contains info of the form: <cmd>{<args>}
        // where, <cmd> is one of kfs cmds
        string::size_type curr, next;
        
        // get rid of leading spaces
        curr = s.find_first_not_of(" \t");
        s.erase(0, curr);
        curr = s.find(' ');
        if (curr != string::npos)
            cmd.assign(s, 0, curr);
        else
            cmd = s;

        next = curr;
        // extract out the args
        vector<string> args;
        while (curr != string::npos) {
            string component;

            // curr points to a ' '
            curr++;
            next = s.find(' ', curr);        
            if (next != string::npos)
                component.assign(s, curr, next - curr);
            else
                component.assign(s, curr, string::npos);

            if (component != "")
                args.push_back(component);
            curr = next;
        }

        CmdHandlersIter h = handlers.find(cmd);
        if (h == handlers.end()) {
            cout << "Unknown cmd: " << cmd << endl;
            cout << "Supported cmds are: " << endl;
            printCmds();
            cout << "Type <cmd name> --help for command specific help" << endl;
            continue;
        }
        
        ((*h).second)(args);
        
    }
}

