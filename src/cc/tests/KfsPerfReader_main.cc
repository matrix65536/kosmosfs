//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/06/23
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
// \brief Program that reads sequentially from a file in KFS.
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

using namespace KFS;
KfsClient *gKfsClient;

static long doRead(const string &kfspathname, int numMBytes, int readSizeBytes);

int
main(int argc, char **argv)
{
    char optchar;
    string kfspathname = "";
    char *kfsPropsFile = NULL;
    int numMBytes = 1, readSizeBytes = 65536;
    bool help = false;

    while ((optchar = getopt(argc, argv, "f:p:m:b:")) != -1) {
        switch (optchar) {
            case 'f':
                kfspathname = optarg;
                break;
            case 'b':
                readSizeBytes = atoi(optarg);
                break;
            case 'p':
                kfsPropsFile = optarg;
                break;
            case 'm':
                numMBytes = atoi(optarg);
                break;
            default:
                KFS_LOG_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help || (kfsPropsFile == NULL) || (kfspathname == "")) {
        cout << "Usage: " << argv[0] << " -p <Kfs Client properties file> "
             << " -m <# of MB to read> -b <read size in bytes> -f <Kfs file> " << endl;
        exit(0);
    }

    cout << "Doing reads to: " << kfspathname << " # MB = " << numMBytes;
    cout << " # of bytes per read: " << readSizeBytes << endl;

    gKfsClient = KfsClient::Instance();
    gKfsClient->Init(kfsPropsFile);
    if (!gKfsClient->IsInitialized()) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }

    string kfsdirname, kfsfilename;
    string::size_type slash = kfspathname.rfind('/');
    
    if (slash == string::npos) {
        cout << "Bad kfs path: " << kfsdirname << endl;
        exit(0);
    }

    kfsdirname.assign(kfspathname, 0, slash);
    kfsfilename.assign(kfspathname, slash + 1, kfspathname.size());

    struct timeval startTime, endTime;
    double timeTaken;
    long bytesRead;

    gettimeofday(&startTime, NULL);

    bytesRead = doRead(kfspathname, numMBytes, readSizeBytes);

    gettimeofday(&endTime, NULL);

    timeTaken = (endTime.tv_sec - startTime.tv_sec) +
        (endTime.tv_usec - startTime.tv_usec) * 1e-6;

    cout << "Read rate: " << (((double) bytesRead * 8.0) / timeTaken) / (1024.0 * 1024.0) << " (Mbps)" << endl;
    cout << "Read rate: " << ((double) (bytesRead) / timeTaken) / (1024.0 * 1024.0) << " (MBps)" << endl;
    
}

long
doRead(const string &filename, int numMBytes, int readSizeBytes)
{
    const int mByte = 1024 * 1024;
    char dataBuf[mByte];
    int res, bytesRead = 0, nMBytes = 0, fd;
    long nread = 0;

    if (readSizeBytes > mByte) {
        cout << "Setting read size to: " << mByte << endl;
        readSizeBytes = mByte;
    }

    fd = gKfsClient->Open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        cout << "Open failed: " << endl;
        exit(0);
    }

    for (nMBytes = 0; nMBytes < numMBytes; nMBytes++) {
        for (bytesRead = 0; bytesRead < mByte; bytesRead += readSizeBytes) {
            res = gKfsClient->Read(fd, dataBuf, readSizeBytes);
            if (res != readSizeBytes)
                return (bytesRead + nMBytes * 1024 * 1024);
            nread += readSizeBytes;
        }
    }
    cout << "read of " << nread / (1024 * 1024) << " (MB) is done" << endl;
    gKfsClient->Close(fd);

    return nread;
}
    
