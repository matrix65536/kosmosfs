//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/06/23
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
// \brief Tool that copies a file/directory from a local file system to
// KFS.  This tool is analogous to dump---backup a directory from a
// file system into KFS.
//
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>
#include <boost/scoped_array.hpp>
using boost::scoped_array;
using std::ios_base;

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <openssl/md5.h>
}

#include "libkfsClient/KfsClient.h"
#include "common/log.h"
#include "KfsToolsCommon.h"

#define MAX_FILE_NAME_LEN 256

using std::cout;
using std::endl;
using std::ifstream;
using std::ofstream;
using namespace KFS;
using namespace KFS::tools;

int
main(int argc, char **argv)
{
    DIR *dirp;
    string kfsPath = "";
    string serverHost = "";
    int port = -1;
    string sourcePath = "";
    bool help = false;
    bool verboseLogging = false;
    char optchar;
    struct stat statInfo;

    KFS::tools::getEnvServer(serverHost, port);
    
    KFS::MsgLogger::Init(NULL);

    while ((optchar = getopt(argc, argv, "d:hk:p:s:v")) != -1) {
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
                KFS::tools::parseServer(optarg, serverHost, port);
                break;
            case 'h':
                help = true;
                break;
            case 'v':
                verboseLogging = true;
                break;
            default:
                KFS_LOG_VA_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help || (sourcePath == "") || (kfsPath == "") || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port> "
             << " -d <source path> -k <Kfs path> {-v}" << endl;
        cout << "<source path> of - means stdin and is supported only if <kfs path> is a file" << endl;
        exit(-1);
    }

    KfsClientPtr kfsClient = getKfsClientFactory()->SetDefaultClient(serverHost, port);

    if (!kfsClient) {
	cout << "kfs client failed to initialize...exiting" << endl;
        exit(-1);
    }

    if (verboseLogging) {
        KFS::MsgLogger::SetLevel(log4cpp::Priority::DEBUG);
    } else {
        KFS::MsgLogger::SetLevel(log4cpp::Priority::WARN);
    } 

    statInfo.st_mode = S_IFREG;
    if ((sourcePath != "-") && (stat(sourcePath.c_str(), &statInfo) < 0)) {
        KFS_LOG_VA_INFO("Source path: %s is non-existent", sourcePath.c_str());
	exit(-1);
    }

    if (!S_ISDIR(statInfo.st_mode)) {
	BackupFile(kfsClient, sourcePath, kfsPath);
	exit(0);
    }

    if ((dirp = opendir(sourcePath.c_str())) == NULL) {
	perror("opendir: ");
        exit(-1);
    }

    // when doing cp -r a/b kfs://c, we need to create c/b in KFS.
    MakeKfsLeafDir(kfsClient, sourcePath, kfsPath);

    BackupDir(kfsClient, sourcePath, kfsPath);

    closedir(dirp);
}
