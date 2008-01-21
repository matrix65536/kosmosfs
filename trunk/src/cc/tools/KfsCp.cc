//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/06/23
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
// \brief Tool that copies a file/directory from a KFS path to another
// KFS path.  This does the analogous of "cp -r".
//
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
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
using namespace KFS;
using namespace KFS::tools;

//
// Given a file defined by a KFS srcPath, copy it to KFS as defined by
// dstPath
//
int CopyFile(const string &srcPath, const string &dstPath);

// Given a srcDirname, copy it to dirname.  Dirname will be created
// if it doesn't exist.  
void CopyDir(const string &srcDirname, string dstDirname);

// Guts of the work
int CopyFile2(string srcfilename, string dstfilename);

void
KFS::tools::handleCopy(const vector<string> &args)
{
    if ((args.size() < 2) || (args[0] == "--help") || (args[0] == "") || (args[1] == "")) {
        cout << "Usage: cp <source path> <dst path>" << endl;
        return;
    }

    KfsClient *kfsClient = KfsClient::Instance();

    if (!kfsClient->Exists(args[0].c_str())) {
	cout << "Source path: " << args[0] << " is non-existent!" << endl;
        return;
    }

    if (kfsClient->IsFile(args[0].c_str())) {
	CopyFile(args[0], args[1]);
        return;
    }

    CopyDir(args[0], args[1]);
}

int
CopyFile(const string &srcPath, const string &dstPath)
{
    string filename;
    string::size_type slash = srcPath.rfind('/');
    KfsClient *kfsClient = KfsClient::Instance();

    // get everything after the last slash
    if (slash != string::npos) {
	filename.assign(srcPath, slash+1, string::npos);
    } else {
	filename = srcPath;
    }

    // for the dest side: if the dst is a dir, we are copying to
    // dstPath with srcFilename; otherwise, dst is a file (that
    // potenitally exists) and we are ovewriting/creating it
    if (kfsClient->IsDirectory(dstPath.c_str())) {
        string dst = dstPath;

        if (dst[dstPath.size() - 1] != '/')
            dst += "/";
        
        return CopyFile2(srcPath, dst + filename);
    }
    
    // dstPath is the filename that is being specified for the cp
    // target.  try to copy to there...
    return CopyFile2(srcPath, dstPath);
}

void
CopyDir(const string &srcDirname, string dstDirname)
{
    vector<string> dirEntries;
    vector<string>::size_type i;
    int res;
    KfsClient *kfsClient = KfsClient::Instance();

    if ((res = kfsClient->Readdir((char *) srcDirname.c_str(), dirEntries)) < 0) {
        cout << "Readdir plus failed: " << res << endl;
        return;
    }

    if (!doMkdirs(dstDirname.c_str())) {
	cout << "Unable to make kfs dir: " << dstDirname << endl;
	return;
    }
    
    for (i = 0; i < dirEntries.size(); ++i) {
        if ((dirEntries[i] == ".") || (dirEntries[i] == ".."))
            continue;

        if (kfsClient->IsDirectory(dirEntries[i].c_str())) {
	    CopyDir(srcDirname + "/" + dirEntries[i],
                    dstDirname + "/" + dirEntries[i]);
        } else {
            CopyFile2(srcDirname + "/" + dirEntries[i],
		      dstDirname + "/" + dirEntries[i]);
        }
    }
}

//
// Guts of the work to copy the file.
//
int
CopyFile2(string srcfilename, string dstfilename)
{
    const int bufsize = 65536;
    char kfsBuf[bufsize];
    int srcfd, dstfd, nRead, toRead;
    long long n = 0;
    int res;
    KfsClient *kfsClient = KfsClient::Instance();

    srcfd = kfsClient->Open(srcfilename.c_str(), O_RDONLY);
    if (srcfd < 0) {
        cout << "Unable to open: " << srcfilename.c_str() << endl;
        return srcfd;
    }

    dstfd = kfsClient->Create((char *) dstfilename.c_str());
    if (dstfd < 0) {
        cout << "Create " << dstfilename << " failed: " << ErrorCodeToStr(dstfd) << endl;
        return dstfd;
    }

    while (1) {
	toRead = bufsize;
	nRead = kfsClient->Read(srcfd, kfsBuf, toRead);
	if (nRead <= 0)
	    break;

        // write it out
        res = kfsClient->Write(dstfd, kfsBuf, nRead);
        if (res < 0) {
            cout << "Write failed with error code: " << ErrorCodeToStr(res) << endl;
            return res;
        }
        n += nRead;
    }
    kfsClient->Close(srcfd);
    kfsClient->Close(dstfd);

    return 0;
}
