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
#include "common/log.h"

#define MAX_FILE_NAME_LEN 256

using std::cout;
using std::endl;
using std::ifstream;
using std::vector;

using namespace KFS;
KfsClient *gKfsClient;

void doRecrRemove(const char *dirname);
bool doRmdir(const char *dirname);
bool doRecrRmdir(const char *dirname);
bool doRemoveFile(const char *pathname);


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
            case 'd':
                kfsdirname = optarg;
                break;
            case 's':
                serverHost = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'h':
                help = true;
                break;
            default:
                KFS_LOG_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help || (kfsdirname == "") || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port> "
             << " -d <path> " << endl;
        exit(0);
    }

    gKfsClient = KfsClient::Instance();
    gKfsClient->Init(serverHost, port);
    if (!gKfsClient->IsInitialized()) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }

    doRecrRemove(kfsdirname.c_str());
}

void
doRecrRemove(const char *pathname)
{
    int res;
    struct stat statInfo;

    res = gKfsClient->Stat(pathname, statInfo);
    if (res < 0) {
        cout << "unable to stat : " << pathname << endl;
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

    res = gKfsClient->Remove(pathname);
    if (res < 0) {
        cout << "unable to remove: " << pathname <<  ' ' << "error = " << res << endl;
    }

    return res == 0;
}

bool
doRmdir(const char *dirname)
{
    int res;

    cout << "Rm dir: " << dirname << endl;

    res = gKfsClient->Rmdir(dirname);
    if (res < 0) {
        cout << "unable to rmdir: " << dirname <<  ' ' << "error = " << res << endl;
        return false;
    }
    return true;
}

// do the equivalent of rm -rf
bool
doRecrRmdir(const char *dirname)
{
    int res;
    vector<KfsFileAttr> dirEntries;
    vector<KfsFileAttr>::size_type i;

    cout << "rmdir -r: " << dirname << endl;

    res = gKfsClient->ReaddirPlus(dirname, dirEntries);
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


