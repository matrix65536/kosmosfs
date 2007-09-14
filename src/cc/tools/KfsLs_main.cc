//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/tools/KfsLs_main.cc#5 $
//
// Created 2006/10/28
// Author: Sriram Rao (Kosmix Corp.) 
//
// Copyright 2006 Kosmix Corp.
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
// \brief Tool for listing directory contents (ala ls -l).
// 
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>

extern "C" {
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
}

#include "libkfsClient/KfsClient.h"
#include "common/log.h"

#define MAX_FILE_NAME_LEN 256

using std::cout;
using std::endl;
using std::ofstream;

KfsClient *gKfsClient;

static void DirList(string &kfsdirname);

int
main(int argc, char **argv)
{
    string kfsdirname = "";
    string serverHost = "";
    int port = -1;
    bool help = false;
    char optchar;

    while ((optchar = getopt(argc, argv, "hs:p:d:")) != -1) {
        switch (optchar) {
            case 's':
                serverHost = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'd':
                kfsdirname = optarg;
                break;
            case 'h':
                help = true;
                break;
            default:
                COSMIX_LOG_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help || (kfsdirname == "") || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port>"
             << " -d <dir> " << endl;
        exit(0);
    }

    gKfsClient = KFS::getKfsClient();
    gKfsClient->Init(serverHost, port);
    if (!gKfsClient->IsInitialized()) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }
    
    DirList(kfsdirname);

}

void
DirList(string &kfsdirname)
{
    string kfssubdir, subdir;
    int res;
    vector<KfsFileAttr> fileInfo;
    vector<KfsFileAttr>::size_type i;

    if ((res = gKfsClient->ReaddirPlus((char *) kfsdirname.c_str(), fileInfo)) < 0) {
        cout << "Readdir plus failed: " << res << endl;
        return;
    }
    
    for (i = 0; i < fileInfo.size(); ++i) {
        if (fileInfo[i].isDirectory) {
            if ((fileInfo[i].filename == ".") ||
                (fileInfo[i].filename == ".."))
                continue;
            cout << fileInfo[i].filename << "/" << '\t' << "(dir)";
        } else {
            cout << fileInfo[i].filename << '\t' << fileInfo[i].fileSize;
        }
        cout << endl;
    }
}
