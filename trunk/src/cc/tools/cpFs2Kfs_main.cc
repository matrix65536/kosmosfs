//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/tools/cpFs2Kfs_main.cc#6 $
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
// \brief Tool that copies a file/directory from a local file system to
// KFS.  This tool is analogous to dump---backup a directory from a
// file system into KFS.
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
bool doMkdirs(const char *path);

//
// Given a file defined by sourcePath, copy it to KFS as defined by
// kfsPath
//
int BackupFile(const string &sourcePath, const string &kfsPath);

// Given a dirname, backit up it to dirname.  Dirname will be created
// if it doesn't exist.  
void BackupDir(const string &dirname, string &kfsdirname);

// Guts of the work
int BackupFile2(string srcfilename, string kfsfilename);

int
main(int argc, char **argv)
{
    DIR *dirp;
    string kfsPath = "";
    string serverHost = "";
    int port = -1;
    char *sourcePath = NULL;
    bool help = false;
    char optchar;
    struct stat statInfo;

    while ((optchar = getopt(argc, argv, "d:hk:p:s:")) != -1) {
        switch (optchar) {
            case 'd':
                sourcePath = optarg;
                break;
            case 'k':
                kfsPath = optarg;
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

    if (help || (sourcePath == NULL) || (kfsPath == "") || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port> "
             << " -d <source path> -k <Kfs path> " << endl;
        exit(0);
    }

    gKfsClient = KfsClient::Instance();
    gKfsClient->Init(serverHost, port);
    if (!gKfsClient->IsInitialized()) {
	cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }

    if (stat(sourcePath, &statInfo) < 0) {
	cout << "Source path: " << sourcePath << " is non-existent!" << endl;
	exit(-1);
    }

    if (!S_ISDIR(statInfo.st_mode)) {
	BackupFile(sourcePath, kfsPath);
	exit(0);
    }

    if ((dirp = opendir(sourcePath)) == NULL) {
	perror("opendir: ");
        exit(0);
    }

    BackupDir(sourcePath, kfsPath);

    closedir(dirp);
}

int
BackupFile(const string &sourcePath, const string &kfsPath)
{
    string filename;
    string::size_type slash = sourcePath.rfind('/');
    struct stat statInfo;
    string kfsParentDir;

    // get everything after the last slash
    if (slash != string::npos) {
	filename.assign(sourcePath, slash+1, string::npos);
    } else {
	filename = sourcePath;
    }
    
    // get the path in KFS.  If we what we have is an existing file or
    // directory in KFS, kfsParentDir will point to it; if kfsPath is
    // non-existent, then we find the parent dir and check for its
    // existence.  That is, we are trying to handle cp file/a to
    // kfs://path/b and we are checking for existence of "/path"
    kfsParentDir = kfsPath;
    if (gKfsClient->Stat(kfsPath.c_str(), statInfo)) {
	slash = kfsPath.rfind('/');
	if (slash == string::npos)
	    kfsParentDir = "";
	else {
	    kfsParentDir.assign(kfsPath, 0, slash);
	    gKfsClient->Stat(kfsParentDir.c_str(), statInfo);

	    // this is the name of the file in the dest path
	    filename.assign(kfsPath, slash+1, string::npos);
	}
    }
    
    if (S_ISDIR(statInfo.st_mode)) {
	return BackupFile2(sourcePath, kfsParentDir + "/" + filename);
    }
    
    if (S_ISREG(statInfo.st_mode)) {
	return BackupFile2(sourcePath, kfsPath);
    }
    
    // need to make the kfs dir
    cout << "KFS Path: " << kfsPath << " is non-existent!" << endl;
    return -1;
}

void
BackupDir(const string &dirname, string &kfsdirname)
{
    string subdir, kfssubdir;
    DIR *dirp;
    struct dirent *fileInfo;

    if ((dirp = opendir(dirname.c_str())) == NULL) {
        perror("opendir: ");
        exit(0);
    }

    if (!doMkdirs(kfsdirname.c_str())) {
        cout << "Unable to make kfs dir: " << kfsdirname << endl;
        closedir(dirp);
	return;
    }

    while ((fileInfo = readdir(dirp)) != NULL) {
        if (strcmp(fileInfo->d_name, ".") == 0)
            continue;
        if (strcmp(fileInfo->d_name, "..") == 0)
            continue;
        
        string name = dirname + "/" + fileInfo->d_name;
        struct stat buf;
        stat(name.c_str(), &buf);

        if (S_ISDIR(buf.st_mode)) {
	    subdir = dirname + "/" + fileInfo->d_name;
            kfssubdir = kfsdirname + "/" + fileInfo->d_name;
            BackupDir(subdir, kfssubdir);
        } else if (S_ISREG(buf.st_mode)) {
	    BackupFile2(dirname + "/" + fileInfo->d_name, kfsdirname + "/" + fileInfo->d_name);
        }
    }
    closedir(dirp);
}

//
// Guts of the work to copy the file.
//
int
BackupFile2(string srcfilename, string kfsfilename)
{
    const int bufsize = 65536;
    char kfsBuf[bufsize];
    int kfsfd, nRead;
    long long n = 0;
    ifstream ifs;
    int res;

    ifs.open(srcfilename.c_str(), std::ios_base::in);
    if (!ifs) {
        cout << "Unable to open: " << srcfilename.c_str() << endl;
        exit(0);
    }

    kfsfd = gKfsClient->Create((char *) kfsfilename.c_str());
    if (kfsfd < 0) {
        cout << "Create " << kfsfilename << " failed: " << kfsfd << endl;
        exit(0);
    }

    while (!ifs.eof()) {
        ifs.read(kfsBuf, bufsize);

        nRead = ifs.gcount();
        
        if (nRead <= 0)
            break;
        
        res = gKfsClient->Write(kfsfd, kfsBuf, nRead);
        if (res < 0) {
            cout << "Write failed with error code: " << res << endl;
            exit(0);
        }
        n += nRead;
        // cout << "Wrote: " << n << endl;
    }
    gKfsClient->Close(kfsfd);


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

