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
// \brief Tool that copies out a file/directory from KFS to the local file
// system.  This tool is analogous to restore, such as, restore a previously
// backed up directory from KFS.
//
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>

extern "C" {
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/stat.h>
}

#include "libkfsClient/KfsClient.h"
#include "common/log.h"

#define MAX_FILE_NAME_LEN 256

using std::cout;
using std::endl;
using std::ofstream;

using namespace KFS;
KfsClient *gKfsClient;

// Given a kfsdirname, restore it to dirname.  Dirname will be created
// if it doesn't exist. 
void RestoreDir(string &dirname, string &kfsdirname);

// Given a kfsdirname/filename, restore it to dirname/filename.  The
// operation here is simple: read the file from KFS and dump it to filename.
//
int RestoreFile(string &kfspath, string &localpath);

// does the guts of the work
int RestoreFile2(string kfsfilename, string localfilename);

int
main(int argc, char **argv)
{
    DIR *dirp;
    string kfsPath = "";
    string localPath = "";
    string serverHost = "";
    int port = -1;
    bool help = false;
    char optchar;
    struct stat statInfo;

    KFS::MsgLogger::Init(NULL);

    while ((optchar = getopt(argc, argv, "d:hp:s:k:")) != -1) {
        switch (optchar) {
            case 'd':
                localPath = optarg;
                break;
            case 's':
                serverHost = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'k':
                kfsPath = optarg;
                break;
            case 'h':
                help = true;
                break;
            default:
                KFS_LOG_VA_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help || (kfsPath == "") || (localPath == "") || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port> "
             << " -k <kfs source path> -d <local path> " << endl;
        exit(0);
    }

    gKfsClient = KfsClient::Instance();
    gKfsClient->Init(serverHost, port);
    if (!gKfsClient->IsInitialized()) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }

    if (gKfsClient->Stat(kfsPath.c_str(), statInfo) < 0) {
	cout << "KFS path: " << kfsPath << " is non-existent!" << endl;
	exit(-1);
    }

    if (!S_ISDIR(statInfo.st_mode)) {
	RestoreFile(kfsPath, localPath);
	exit(0);
    }

    RestoreDir(kfsPath, localPath);
    closedir(dirp);
}

int
RestoreFile(string &kfsPath, string &localPath)
{
    string filename;
    string::size_type slash = kfsPath.rfind('/');
    struct stat statInfo;
    string localParentDir;

    // get everything after the last slash
    if (slash != string::npos) {
	filename.assign(kfsPath, slash+1, string::npos);
    } else {
	filename = kfsPath;
    }
    
    // get the path in local FS.  If we what we is an existing file or
    // directory in local FS, localParentDir will point to it; if localPath is
    // non-existent, then we find the parent dir and check for its
    // existence.  That is, we are trying to handle cp kfs://file/a to
    // /path/b and we are checking for existence of "/path"
    localParentDir = localPath;
    if (stat(localPath.c_str(), &statInfo)) {
	slash = localPath.rfind('/');
	if (slash == string::npos)
	    localParentDir = "";
	else {
	    localParentDir.assign(localPath, 0, slash);
	    stat(localParentDir.c_str(), &statInfo);

	    // this is the target filename
	    filename.assign(localPath, slash + 1, string::npos);
	}
    }
    
    if (S_ISDIR(statInfo.st_mode)) {
	return RestoreFile2(kfsPath, localParentDir + "/" + filename);
    }
    
    if (S_ISREG(statInfo.st_mode)) {
	return RestoreFile2(kfsPath, localPath);
    }
    
    // need to make the local dir
    cout << "Local Path: " << localPath << " is non-existent!" << endl;
    return -1;

}

void
RestoreDir(string &kfsdirname, string &dirname)
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
	    subdir = dirname + "/" + fileInfo[i].filename;
            mkdir(subdir.c_str(), ALLPERMS);
            kfssubdir = kfsdirname + "/" + fileInfo[i].filename.c_str();
            RestoreDir(subdir, kfssubdir);
        } else {
            RestoreFile2(kfsdirname + "/" + fileInfo[i].filename,
			 dirname + "/" + fileInfo[i].filename);
        }
    }
}

// 
// Guts of the work
//
int
RestoreFile2(string kfsfilename, string localfilename)
{
    const int bufsize = 65536;
    char kfsBuf[bufsize];
    int kfsfd, n = 0, nRead, toRead;
    ofstream ofs;

    cout << "Restoring: " << localfilename << endl;

    kfsfd = gKfsClient->Open((char *) kfsfilename.c_str(), O_RDONLY);
    if (kfsfd < 0) {
        cout << "Open failed: " << endl;
        exit(0);
    }

    ofs.open(localfilename.c_str(), std::ios_base::out);
    if (!ofs) {
        cout << "Unable to open: " << localfilename << endl;
        exit(0);
    }
    
    while (!ofs.eof()) {
        toRead = bufsize;

        nRead = gKfsClient->Read(kfsfd, kfsBuf, toRead);
        if (nRead <= 0) {
	    // EOF
            break;
        }
        n += nRead;
        ofs.write(kfsBuf, nRead);
    }
    gKfsClient->Close(kfsfd);
    return 0;

}
