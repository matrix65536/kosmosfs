//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/tools/KfsRm_main.cc#5 $
//
// Created 2006/09/21
// Author: Sriram Rao (Kosmix Corp.) 
//
// Copyright (C) 2006 Kosmix Corp.
//
// This file is part of Kosmos File System (KFS).
//
// KFS is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation under version 3 of the License.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see
// <http://www.gnu.org/licenses/>.
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
                COSMIX_LOG_ERROR("Unrecognized flag %c", optchar);
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

    cout << "rmdir -p: " << dirname << endl;

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


