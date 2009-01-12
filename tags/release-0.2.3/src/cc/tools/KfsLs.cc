//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/10/28
// Author: Sriram Rao
//
// Copyright 2008 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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
#include <time.h>
}

#include "libkfsClient/KfsClient.h"
#include "tools/KfsShell.h"

using std::cout;
using std::endl;
using std::ofstream;
using std::vector;

using namespace KFS;

static int dirList(string kfsdirname, bool longMode, bool humanReadable);
static int doDirList(string kfsdirname);
static int doDirListPlusAttr(string kfsdirname, bool humanReadable);
static void printFileInfo(const string &filename, time_t mtime, off_t filesize, bool humanReadable);
static void getTimeString(time_t time, char *buf, int bufLen = 256);


// may want to do "ls -r"
int
KFS::tools::handleLs(const vector<string> &args)
{
    bool longMode = false, humanReadable = false;
    vector<string>::size_type pathIndex = 0;

    if ((args.size() >= 1) && (args[0] == "--help")) {
        cout << "Usage: ls {-lh} {<dir>} " << endl;
        return 0;
    }

    if (args.size() >= 1) {
        if (args[0][0] == '-') {
            pathIndex = 1;
            for (uint32_t i = 1; i < args[0].size(); i++) {
                switch (args[0][i]) {
                    case 'l':
                        longMode = true;
                        break;
                    case 'h':
                        humanReadable = true;
                        break;
                }
            }
        }
    }

    if (args.size() > pathIndex)
        return dirList(args[pathIndex], longMode, humanReadable);
    else
        return dirList(".", longMode, humanReadable);
}

int
dirList(string kfsdirname, bool longMode, bool humanReadable)
{
    if (longMode)
        return doDirListPlusAttr(kfsdirname, humanReadable);
    else
        return doDirList(kfsdirname);
}

int
doDirList(string kfsdirname)
{
    string kfssubdir, subdir;
    int res;
    vector<string> entries;
    vector<string>::size_type i;

    KfsClientPtr kfsClient = getKfsClientFactory()->GetClient();

    if (kfsClient->IsFile((char *) kfsdirname.c_str())) {
        cout << kfsdirname << endl;
        return 0;
    }
        
    if ((res = kfsClient->Readdir((char *) kfsdirname.c_str(), entries)) < 0) {
        cout << "Readdir failed: " << ErrorCodeToStr(res) << endl;
        return res;
    }

    // we could provide info of whether the thing is a dir...but, later
    for (i = 0; i < entries.size(); ++i) {
        if ((entries[i] == ".") || (entries[i] == ".."))
            continue;
        cout << entries[i] << endl;
    }
    return 0;
}

int
doDirListPlusAttr(string kfsdirname, bool humanReadable)
{
    string kfssubdir, subdir;
    int res;
    vector<KfsFileAttr> fileInfo;
    vector<KfsFileAttr>::size_type i;

    KfsClientPtr kfsClient = getKfsClientFactory()->GetClient();

    if (kfsClient->IsFile((char *) kfsdirname.c_str())) {
        struct stat statInfo;

        kfsClient->Stat(kfsdirname.c_str(), statInfo);
        printFileInfo(kfsdirname, statInfo.st_mtime, statInfo.st_size, humanReadable);
        return 0;
    }
    if ((res = kfsClient->ReaddirPlus((char *) kfsdirname.c_str(), fileInfo)) < 0) {
        cout << "Readdir plus failed: " << ErrorCodeToStr(res) << endl;
        return res;
    }
    
    for (i = 0; i < fileInfo.size(); ++i) {
        if (fileInfo[i].isDirectory) {
            if ((fileInfo[i].filename == ".") ||
                (fileInfo[i].filename == ".."))
                continue;
            char timeBuf[256];

            getTimeString(fileInfo[i].mtime.tv_sec, timeBuf);

            cout << fileInfo[i].filename << "/" << '\t' << timeBuf << '\t' << "(dir)" << endl;
        } else {
            printFileInfo(fileInfo[i].filename, fileInfo[i].mtime.tv_sec, 
                          fileInfo[i].fileSize, humanReadable);
        }
    }
    return 0;
}

void
printFileInfo(const string &filename, time_t mtime, off_t filesize, bool humanReadable)
{
    char timeBuf[256];

    getTimeString(mtime, timeBuf);

    if (!humanReadable) {
        cout << filename << '\t' << timeBuf << '\t' << filesize << endl;
        return;
    }
    if (filesize < (1 << 20)) {
        cout << filename << '\t' << timeBuf << '\t' << (float) (filesize) / (1 << 10) << " K";
    }
    else if (filesize < (1 << 30)) {
        cout << filename << '\t' << timeBuf << '\t' << (float) (filesize) / (1 << 20) << " M";
    }
    else {
        cout << filename << '\t' << timeBuf << '\t' << (float) (filesize) / (1 << 30) << " G";
    }
    cout << endl;
}

void
getTimeString(time_t time, char *buf, int bufLen)
{
    struct tm locTime;

    localtime_r(&time, &locTime);
    strftime(buf, bufLen, "%b %e %H:%M", &locTime);
}
