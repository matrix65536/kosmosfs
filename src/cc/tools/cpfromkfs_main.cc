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
#include "KfsToolsCommon.h"

#define MAX_FILE_NAME_LEN 256

using std::cout;
using std::cerr;
using std::endl;
using std::ofstream;

using namespace KFS;
using namespace KFS::tools;

int
main(int argc, char **argv)
{
    string kfsPath = "";
    string localPath = "";
    string serverHost = "";
    int port = -1;
    bool help = false;
    bool verboseLogging = false;
    char optchar;
    struct stat statInfo;

    KFS::tools::getEnvServer(serverHost, port);
    
    KFS::MsgLogger::Init(NULL);

    while ((optchar = getopt(argc, argv, "d:hp:s:k:v")) != -1) {
        switch (optchar) {
            case 'd':
                localPath = optarg;
                break;
            case 's':
                KFS::tools::parseServer(optarg, serverHost, port);
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
            case 'v':
                verboseLogging = true;
                break;
            default:
                KFS_LOG_VA_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help || (kfsPath == "") || (localPath == "") || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port> "
             << " -k <kfs source path> -d <local path> {-v} " << endl;
        cout << "<local path> of - means stdout and is supported only if <kfs path> is a file" << endl;
        exit(0);
    }

    KfsClientPtr kfsClient = getKfsClientFactory()->SetDefaultClient(serverHost, port);

    if (!kfsClient) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }

    if (verboseLogging) {
        KFS::MsgLogger::SetLevel(log4cpp::Priority::DEBUG);
    } else {
        KFS::MsgLogger::SetLevel(log4cpp::Priority::WARN);
    } 

    if (kfsClient->Stat(kfsPath, statInfo) < 0) {
	cout << "KFS path: " << kfsPath << " is non-existent!" << endl;
	exit(-1);
    }

    int retval;

    if (!S_ISDIR(statInfo.st_mode)) {
	retval = RestoreFile(kfsClient, kfsPath, localPath);
    } else {
        retval = RestoreDir(kfsClient, kfsPath, localPath);
    }
    exit(retval);
}
