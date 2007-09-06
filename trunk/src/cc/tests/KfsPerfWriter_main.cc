//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/tests/KfsPerfWriter_main.cc#2 $
//
// Created 2006/06/23
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
// \brief Program that writes sequentially to a file in KFS.
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
static bool doMkdir(const char *dirname);
static long doWrite(const string &kfspathname, int numMBytes, size_t writeSizeBytes);

int
main(int argc, char **argv)
{
    char optchar;
    string kfspathname = "";
    char *kfsPropsFile = NULL;
    int numMBytes = 1;
    size_t writeSizeBytes = 65536;
    bool help = false;

    while ((optchar = getopt(argc, argv, "f:p:m:b:")) != -1) {
        switch (optchar) {
            case 'f':
                kfspathname = optarg;
                break;
            case 'b':
                writeSizeBytes = atoll(optarg);
                break;
            case 'p':
                kfsPropsFile = optarg;
                break;
            case 'm':
                numMBytes = atoi(optarg);
                break;
            default:
                COSMIX_LOG_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    if (help || (kfsPropsFile == NULL) || (kfspathname == "")) {
        cout << "Usage: " << argv[0] << " -p <Kfs Client properties file> "
             << " -m <# of MB to write> -b <write size in bytes> -f <Kfs file> " << endl;
        exit(0);
    }

    cout << "Doing writes to: " << kfspathname << " # MB = " << numMBytes;
    cout << " # of bytes per write: " << writeSizeBytes << endl;

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
    doMkdir(kfsdirname.c_str());

    struct timeval startTime, endTime;
    double timeTaken;
    long bytesWritten;

    gettimeofday(&startTime, NULL);

    bytesWritten = doWrite(kfspathname, numMBytes, writeSizeBytes);

    gettimeofday(&endTime, NULL);

    timeTaken = (endTime.tv_sec - startTime.tv_sec) +
        (endTime.tv_usec - startTime.tv_usec) * 1e-6;

    cout << "Write rate: " << (((double) bytesWritten * 8.0) / timeTaken) / (1024.0 * 1024.0) << " (Mbps)" << endl;
    cout << "Write rate: " << ((double) bytesWritten / timeTaken) / (1024.0 * 1024.0) << " (MBps)" << endl;
    
}

bool
doMkdir(const char *dirname)
{
    int fd;

    cout << "Making dir: " << dirname << endl;

    fd = gKfsClient->Mkdir(dirname);
    if (fd < 0) {
        cout << "Mkdir failed: " << fd << endl;
        return false;
    }
    cout << "Mkdir returned: " << fd << endl;
    return fd > 0;
}

long
doWrite(const string &filename, int numMBytes, size_t writeSizeBytes)
{
    const size_t mByte = 1024 * 1024;
    char dataBuf[mByte];
    int res, fd;
    size_t bytesWritten = 0;
    int nMBytes = 0;
    long nwrote = 0;

    if (writeSizeBytes > mByte) {
        cout << "Setting write size to: " << mByte << endl;
        writeSizeBytes = mByte;
    }

    for (bytesWritten = 0; bytesWritten < mByte; bytesWritten++) {
        dataBuf[bytesWritten] = 'a' + bytesWritten % 26;
    }
    fd = gKfsClient->Open(filename.c_str(), O_CREAT|O_RDWR);
    // fd = gKfsClient->Create(filename.c_str());
    if (fd < 0) {
        cout << "Create failed: " << endl;
        exit(0);
    }

    for (nMBytes = 0; nMBytes < numMBytes; nMBytes++) {
        for (bytesWritten = 0; bytesWritten < mByte; bytesWritten += writeSizeBytes) {
            res = gKfsClient->Write(fd, dataBuf, writeSizeBytes);
            if (res != (int) writeSizeBytes)
                return (bytesWritten + nMBytes * 1024 * 1024);
            nwrote += writeSizeBytes;
        }
    }
    cout << "write of " << nwrote / (1024 * 1024) << " (MB) is done" << endl;
    gKfsClient->Close(fd);

    return nwrote;
}
    
