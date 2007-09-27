//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/09/21
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
// \brief Tool that implements rm -rf <path>
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>
#include <vector>
#include <string>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
}

#include "libkfsClient/KfsClient.h"
#include "tools/KfsShell.h"

using std::cout;
using std::endl;
using std::ifstream;
using std::vector;
using std::string;

using namespace KFS;
using namespace KFS::tools;

void doRecrRemove(const char *dirname);
bool doRecrRmdir(const char *dirname);
bool doRemoveFile(const char *pathname);

void
KFS::tools::handleRm(const vector<string> &args)
{
    if ((args.size() < 1) || (args[0] == "--help") || (args[0] == "")) {
        cout << "Usage: rm <path> " << endl;
        return;
    }

    doRecrRemove(args[0].c_str());
}

void
doRecrRemove(const char *pathname)
{
    int res;
    struct stat statInfo;
    KfsClient *kfsClient = KfsClient::Instance();

    res = kfsClient->Stat(pathname, statInfo);
    if (res < 0) {
        cout << "Unable to remove: " <<  pathname << " : " 
             << ErrorCodeToStr(res) << endl;
        return;
    }
    if (S_ISDIR(statInfo.st_mode)) {
        doRecrRmdir(pathname);
    } else {
        doRemoveFile(pathname);
    }
}

bool
doRemoveFile(const char *pathname)
{
    int res;
    KfsClient *kfsClient = KfsClient::Instance();

    res = kfsClient->Remove(pathname);
    if (res < 0) {
        cout << "unable to remove: " << pathname <<  ' ' << "error = " << res << endl;
    }

    return res == 0;
}

// do the equivalent of rm -rf
bool
doRecrRmdir(const char *dirname)
{
    int res;
    vector<KfsFileAttr> dirEntries;
    vector<KfsFileAttr>::size_type i;
    KfsClient *kfsClient = KfsClient::Instance();

    res = kfsClient->ReaddirPlus(dirname, dirEntries);
    if (res < 0) {
        cout << "Readdir failed on: " << dirname << ' ' << "error: " << res << endl;
        return false;
    }

    for (i = 0; i < dirEntries.size(); ++i) {
        string path = dirname;

        if ((dirEntries[i].filename == ".") ||
            (dirEntries[i].filename == ".."))
            continue;

        // don't add a trailing slash if it exists already
        if (path[path.size() - 1] != '/')
            path += "/";
        path += dirEntries[i].filename;
        if (dirEntries[i].isDirectory) {
            doRecrRmdir(path.c_str());
        } else {
            doRemoveFile(path.c_str());
        }
    }
    doRmdir(dirname);
    return true;
}


