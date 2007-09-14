//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/tools/cpFs2Kfs_main.cc#3 $
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

KfsClient *gKfsClient;

// Make the directory hierarchy in KFS defined by path.
bool doMkdirs(const char *path);

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

int
main(int argc, char **argv)
{
    string dstPath = "";
    string serverHost = "";
    int port = -1;
    char *srcPath = NULL;
    bool help = false;
    char optchar;
    struct stat statInfo;

    while ((optchar = getopt(argc, argv, "d:hk:p:s:")) != -1) {
        switch (optchar) {
            case 'd':
                srcPath = optarg;
                break;
            case 'k':
                dstPath = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 's':
                serverHost = optarg;
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

    if (help || (srcPath == NULL) || (dstPath == "") || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port> "
             << " -d <source path> -k <dst path> " << endl;
        exit(0);
    }

    gKfsClient = KfsClient::Instance();
    gKfsClient->Init(serverHost, port);
    if (!gKfsClient->IsInitialized()) {
	cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }

    if (gKfsClient->Stat(srcPath, statInfo) < 0) {
	cout << "Source path: " << srcPath << " is non-existent!" << endl;
	exit(-1);
    }

    if (!S_ISDIR(statInfo.st_mode)) {
	CopyFile(srcPath, dstPath);
	exit(0);
    }

    CopyDir(srcPath, dstPath);
}

int
CopyFile(const string &srcPath, const string &dstPath)
{
    string filename;
    string::size_type slash = srcPath.rfind('/');
    struct stat statInfo;
    string kfsParentDir;

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
    if (gKfsClient->Stat(dstPath.c_str(), statInfo)) {
	slash = dstPath.rfind('/');
	if (slash == string::npos)
	    kfsParentDir = "";
	else {
	    kfsParentDir.assign(dstPath, 0, slash);
	    gKfsClient->Stat(kfsParentDir.c_str(), statInfo);

	    // this is the name of the file in the dest path
	    filename.assign(dstPath, slash+1, string::npos);
	}
    }

    // kfs side is a directory
    if (S_ISDIR(statInfo.st_mode)) {
	return CopyFile2(srcPath, kfsParentDir + "/" + filename);
    }
    
    if (S_ISREG(statInfo.st_mode)) {
	return CopyFile2(srcPath, dstPath);
    }
    
    // need to make the kfs dir
    cout << "KFS Path: " << dstPath << " is non-existent!" << endl;
    return -1;
}

void
CopyDir(const string &srcDirname, string dstDirname)
{
    vector<KfsFileAttr> fileInfo;
    vector<KfsFileAttr>::size_type i;
    int res;

    if ((res = gKfsClient->ReaddirPlus((char *) srcDirname.c_str(), fileInfo)) < 0) {
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
	    CopyDir(srcDirname + "/" + fileInfo[i].filename, 
                    dstDirname + "/" + fileInfo[i].filename);
        } else {
            CopyFile2(srcDirname + "/" + fileInfo[i].filename,
		      dstDirname + "/" + fileInfo[i].filename);
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

    cout << "In copyfile2: " << srcfilename << "->" << dstfilename << endl;

    srcfd = gKfsClient->Open(srcfilename.c_str(), O_RDONLY);
    if (srcfd < 0) {
        cout << "Unable to open: " << srcfilename.c_str() << endl;
	exit(0);
    }

    dstfd = gKfsClient->Create((char *) dstfilename.c_str(), O_WRONLY);
    if (dstfd < 0) {
        cout << "Create " << dstfilename << " failed: " << dstfd << endl;
	exit(0);
    }

    while (1) {
	toRead = bufsize;
	nRead = gKfsClient->Read(srcfd, kfsBuf, toRead);
	if (nRead <= 0)
	    break;

        // write it out
        res = gKfsClient->Write(dstfd, kfsBuf, nRead);
        if (res < 0) {
            cout << "Write failed with error code: " << res << endl;
            exit(0);
        }
        n += nRead;
    }
    gKfsClient->Close(srcfd);
    gKfsClient->Close(dstfd);

    return 0;
}

bool
doMkdirs(const char *path)
{
    int res;

    res = gKfsClient->Mkdirs((char *) path);
    if ((res < 0) && (res != -EEXIST)) {
        cout << "Mkdir failed: " << ErrorCodeToStr(res) << endl;
        return false;
    }
    return true;
}
