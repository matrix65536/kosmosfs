//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/10/28
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
// \brief Program that behaves like cat: cat foo | KfsPut <filename>
//----------------------------------------------------------------------------

#include <iostream>    
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fstream>
#include "libkfsClient/KfsClient.h"

using std::cout;
using std::cin;
using std::endl;
using std::ifstream;
using std::string;

using namespace KFS;

KfsClient *gKfsClient;

static ssize_t doPut(const string &kfspathname);

int
main(int argc, char **argv)
{
    char optchar;
    string kfspathname = "";
    string serverHost = "";
    int port = -1;
    bool help = false;
    ssize_t numBytes;

    while ((optchar = getopt(argc, argv, "hs:p:d:")) != -1) {
        switch (optchar) {
            case 'f':
                kfspathname = optarg;
                break;
            case 's':
                serverHost = optarg;
                break;
            case 'p':
                port = atoi(optarg);
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
             << " -f <Kfs file> " << endl;
        exit(0);
    }

    gKfsClient = KfsClient::Instance();
    gKfsClient->Init(serverHost, port);
    if (!gKfsClient->IsInitialized()) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }

    numBytes = doPut(kfspathname);
    cout << "Wrote " << numBytes << " to " << kfspathname << endl;

    
}

ssize_t
doPut(const string &filename)
{
    const size_t mByte = 1024 * 1024;
    char dataBuf[mByte];
    int res, fd;
    size_t bytesWritten = 0;
    size_t pos = 0;
    char c;


    fd = gKfsClient->Open(filename.c_str(), O_CREAT|O_RDWR|O_APPEND);
    if (fd < 0) {
        cout << "Create failed: " << endl;
        exit(0);
    }

    while(cin.get(c)) {
        dataBuf[pos] = c;
        pos++;
        if (pos >= mByte) {
            res = gKfsClient->Write(fd, dataBuf, mByte);
            if (res != (int) mByte) {
                cout << "Write failed...expect to write: " << mByte;
                cout << " but only wrote: " << res << endl;
                return res;
            }
            bytesWritten += mByte;
            pos = 0;
        }
    }

    if (pos > 0) {
        res = gKfsClient->Write(fd, dataBuf, pos);
        if (res != (int) pos) {
            cout << "Write failed...expect to write: " << pos;
            cout << " but only wrote: " << res << endl;
            return res;
        }
        bytesWritten += pos;
    }

    gKfsClient->Close(fd);

    return bytesWritten;
}
    
