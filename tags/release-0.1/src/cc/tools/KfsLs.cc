//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
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
#include "tools/KfsShell.h"

using std::cout;
using std::endl;
using std::ofstream;
using std::vector;

using namespace KFS;

static void dirList(string kfsdirname);

// may want to do "ls -r"
void
KFS::tools::handleLs(const vector<string> &args)
{
    if ((args.size() >= 1) && (args[0] == "--help")) {
        cout << "Usage: ls {<dir>} " << endl;
        return;
    }

    if (args.size())
        dirList(args[0]);
    else
        dirList(".");
}

void
dirList(string kfsdirname)
{
    string kfssubdir, subdir;
    int res;
    vector<KfsFileAttr> fileInfo;
    vector<KfsFileAttr>::size_type i;

    KfsClient *kfsClient = KFS::getKfsClient();

    if ((res = kfsClient->ReaddirPlus((char *) kfsdirname.c_str(), fileInfo)) < 0) {
        cout << "Readdir plus failed: " << ErrorCodeToStr(res) << endl;
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
