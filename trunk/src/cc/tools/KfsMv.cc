//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2007/09/24
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
// \brief Tool that renames a file/directory from a KFS path to another
// KFS path.  This does the analogous of "mv".
//
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>

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

using namespace KFS;
using namespace KFS::tools;

//
// Given a file defined by a KFS srcPath, move it to KFS as defined by
// dstPath
//
int MoveFile(const string &srcPath, const string &dstPath);

// Given a srcDirname, copy it to dirname.  Dirname will be created
// if it doesn't exist.  
void MoveDir(const string &srcDirname, string dstDirname);

// Guts of the work
int MoveFile2(string srcfilename, string dstfilename);

void
KFS::tools::handleMv(const vector<string> &args)
{
    struct stat statInfo;

    if ((args.size() < 2) || (args[0] == "--help") || (args[0] == "") || (args[1] == "")) {
        cout << "Usage: mv <source path> <dst path>" << endl;
        return;
    }

    KfsClient *kfsClient = KfsClient::Instance();

    if (kfsClient->Stat(args[0].c_str(), statInfo) < 0) {
	cout << "Source path: " << args[0] << " is non-existent!" << endl;
        return;
    }

    if (!S_ISDIR(statInfo.st_mode)) {
	MoveFile(args[0], args[1]);
        return;
    }

    MoveDir(args[0], args[1]);
}

int
MoveFile(const string &srcPath, const string &dstPath)
{
    string filename;
    string::size_type slash = srcPath.rfind('/');
    struct stat statInfo;
    string kfsParentDir;
    KfsClient *kfsClient = KfsClient::Instance();

    // get everything after the last slash
    if (slash != string::npos) {
	filename.assign(srcPath, slash+1, string::npos);
    } else {
	filename = srcPath;
    }
    
    //
    // get the path in KFS.  If we what we have is an existing file or
    // directory in KFS, kfsParentDir will point to it; if kfsPath is
    // non-existent, then we find the parent dir and check for its
    // existence.  That is, we are trying to handle cp file/a to
    // kfs://path/b and we are checking for existence of "/path"
    //
    kfsParentDir = dstPath;
    if (kfsClient->Stat(dstPath.c_str(), statInfo)) {
	slash = dstPath.rfind('/');
	if (slash == string::npos)
	    kfsParentDir = "";
	else {
	    kfsParentDir.assign(dstPath, 0, slash);
	    kfsClient->Stat(kfsParentDir.c_str(), statInfo);

	    // this is the name of the file in the dest path
	    filename.assign(dstPath, slash+1, string::npos);
	}
    }

    // kfs side is a directory
    if (S_ISDIR(statInfo.st_mode)) {
	return MoveFile2(srcPath, kfsParentDir + "/" + filename);
    }
    
    if (S_ISREG(statInfo.st_mode)) {
	return MoveFile2(srcPath, dstPath);
    }
    
    // need to make the kfs dir
    cout << "KFS Path: " << dstPath << " is non-existent!" << endl;
    return -1;
}

void
MoveDir(const string &srcDirname, string dstDirname)
{
    vector<KfsFileAttr> fileInfo;
    vector<KfsFileAttr>::size_type i;
    int res;
    KfsClient *kfsClient = KfsClient::Instance();

    if ((res = kfsClient->ReaddirPlus((char *) srcDirname.c_str(), fileInfo)) < 0) {
        cout << "Readdir plus failed: " << res << endl;
        return;
    }

    if (!doMkdirs(dstDirname.c_str())) {
	cout << "Unable to make kfs dir: " << dstDirname << endl;
	return;
    }
    
    for (i = 0; i < fileInfo.size(); ++i) {
        if (fileInfo[i].isDirectory) {
            if ((fileInfo[i].filename == ".") ||
                (fileInfo[i].filename == ".."))
                continue;
	    MoveDir(srcDirname + "/" + fileInfo[i].filename, 
                    dstDirname + "/" + fileInfo[i].filename);
        } else {
            MoveFile2(srcDirname + "/" + fileInfo[i].filename,
		      dstDirname + "/" + fileInfo[i].filename);
        }
    }
    kfsClient->Rmdir(srcDirname.c_str());
}

//
// Guts of the work to move the file.
//
int
MoveFile2(string srcfilename, string dstfilename)
{
    int res;
    KfsClient *kfsClient = KfsClient::Instance();

    if ((res = kfsClient->Rename(srcfilename.c_str(), dstfilename.c_str())) < 0) {
        cout << "Rename failed: " << ErrorCodeToStr(res) << endl;
        return res;
    }
    return 0;
}
