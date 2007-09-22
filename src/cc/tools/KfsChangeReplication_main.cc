//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2007/09/20
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
// \brief Tool that changes the degree of replication for a file.
// This tool asks the metaserver to change the degree of replication;
// the change, presuming resources exist, will be done by the
// metaserver asynchronously.
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

using std::cout;
using std::endl;

using namespace KFS;

KfsClient *gKfsClient;

int
main(int argc, char **argv)
{
    string kfspathname = "";
    string serverHost = "";
    int port = -1, res;
    int16_t numReplicas = 1;
    bool help = false;
    char optchar;

    while ((optchar = getopt(argc, argv, "hs:p:f:n:")) != -1) {
        switch (optchar) {
            case 's':
                serverHost = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'n':
                numReplicas = (int16_t) (atoi(optarg));
                break;
            case 'f':
                kfspathname = optarg;
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

    if (help || (kfspathname == "") || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port> "
             << " -f <filename> -n <replication factor> " << endl;
        exit(0);
    }

    gKfsClient = KfsClient::Instance();
    gKfsClient->Init(serverHost, port);
    if (!gKfsClient->IsInitialized()) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }

    if ((res = gKfsClient->SetReplicationFactor(kfspathname.c_str(), numReplicas)) < 0) {
	cout << "Set replication failed: " << ErrorCodeToStr(res) << endl;
    } else {
	cout << "Setting replication factor to: " << res << endl;
    }
        
}

