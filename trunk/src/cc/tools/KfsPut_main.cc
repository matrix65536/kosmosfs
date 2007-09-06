//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/tools/KfsPut_main.cc#5 $
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
                COSMIX_LOG_ERROR("Unrecognized flag %c", optchar);
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
    
