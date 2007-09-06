//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/tools/KfsCat_main.cc#5 $
//
// Created 2006/10/28
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
// \brief Program that behaves like cat...: 
// Kfscat -p <kfsConfig file> [filename1...n]
// and output the files in the order of appearance to stdout.
//
//----------------------------------------------------------------------------

#include <iostream>    
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fstream>
#include "libkfsClient/KfsClient.h"

using std::cout;
using std::endl;
using std::ifstream;
using std::string;

KfsClient *gKfsClient;
static ssize_t DoCat(const char *pahtname);

int
main(int argc, char **argv)
{
    string serverHost = "";
    int port = -1;
    bool help = false;
    char optchar;

    while ((optchar = getopt(argc, argv, "hs:p:")) != -1) {
        switch (optchar) {
            case 'h':
                help = true;
                break;
            case 's':
                serverHost = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            default:
                COSMIX_LOG_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help || (serverHost == "") || (port < 0)) {
        cout << "Usage: " << argv[0] << " -s <meta server name> -p <port>"
             << " [filename1...n]" << endl;
        exit(0);
    }

    gKfsClient = KfsClient::Instance();
    gKfsClient->Init(serverHost, port);
    if (!gKfsClient->IsInitialized()) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }

    int i = 1;
    while (i < argc) {
	if ((strncmp(argv[i], "-p", 2) == 0) || (strncmp(argv[i], "-s", 2) == 0)) {
            i += 2;
            continue;
        }
        // cout << "Cat'ing: " << argv[i] << endl;
        DoCat(argv[i]);
        i++;
    }
}

ssize_t
DoCat(const char *pathname)
{
    const int mByte = 1024 * 1024;
    char dataBuf[mByte];
    int res, fd;    
    size_t bytesRead = 0;
    struct stat statBuf;

    fd = gKfsClient->Open(pathname, O_RDONLY);
    if (fd < 0) {
        cout << "Open failed: " << fd << endl;
        return -ENOENT;
    }

    gKfsClient->Stat(pathname, statBuf);

    while (1) {
        res = gKfsClient->Read(fd, dataBuf, mByte);
        if (res <= 0)
            break;
        cout << dataBuf;
        bytesRead += res;
        if (bytesRead >= (size_t) statBuf.st_size)
            break;
    }
    gKfsClient->Close(fd);

    return bytesRead;
}
    
