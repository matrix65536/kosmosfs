//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/tests/KfsTrunc_main.cc#2 $
//
// Created 2006/08/15
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
// \brief Program that tests file truncation in KFS.
// 
//----------------------------------------------------------------------------

#include <iostream>    
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fstream>
#include "libkfsClient/KfsClient.h"

#define MIN_FILE_SIZE 2048
#define MAX_FILE_SIZE (4096 * 8)
#define MAX_FILE_NAME_LEN 256

using std::cout;
using std::endl;
using std::ifstream;

KfsClient *gKfsClient;
bool doMkdir(char *dirname);
bool doFileOps(char *testDataFile, char *dirname, int seqNum, int numIter);
bool compareData(char *dst, char *src, int numBytes);
void generateData(char *testDataFile, char *buf, int numBytes);
int
main(int argc, char **argv)
{
    char dirname[256];

    if (argc < 3) {
        cout << "Usage: " << argv[0] << " <test-data-file> <kfs-client-properties file> " << endl;
        exit(0);
    }

    gKfsClient = KfsClient::Instance();
    gKfsClient->Init(argv[2]);
    if (!gKfsClient->IsInitialized()) {
        cout << "kfs client failed to initialize...exiting" << endl;
        exit(0);
    }
    
    srand(100);

    strcpy(dirname, "/dir1");

    doMkdir(dirname);
    if (doFileOps(argv[1], dirname, 0, 1) < 0) {
        cout << "File ops failed" << endl;
    }

#if 0
    for (int i = 0; i < numFiles; i++) {
        if (doFileOps(dirname, 0, 10) < 0) {
            cout << "File ops failed..." << endl;
            exit(0);
        }
    }
#endif
    cout << "Test passed" << endl;
}

bool
doMkdir(char *dirname)
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


bool doFileOps(char *testDataFile,
               char *parentDir, int seqNum, int numIter)
{
    char *dataBuf, *kfsBuf;
    int numBytes = 0;
    int fd;
    char fileName[MAX_FILE_NAME_LEN];
    
    while (numBytes < MIN_FILE_SIZE) {
        numBytes = rand() % MAX_FILE_SIZE;
    }

    numBytes = 8192;

    cout << "Writing " << numBytes << endl;

    dataBuf = new char[numBytes];
    generateData(testDataFile, dataBuf, numBytes);

    memset(fileName, 0, MAX_FILE_NAME_LEN);
    snprintf(fileName, MAX_FILE_NAME_LEN, "%s/foo.%d", 
             parentDir, seqNum);

    fd = gKfsClient->Create(fileName);
    if (fd < 0) {
        cout << "Create failed: " << endl;
        exit(0);
    }

    kfsBuf = new char[numBytes];
    memcpy(kfsBuf, dataBuf, numBytes);

/*
    if (gKfsClient->Write(fd, kfsBuf, numBytes) < 0) {
        cout << "Write failed: " << endl;
        exit(0);
    }
*/
    gKfsClient->Seek(fd, 65536, SEEK_CUR);
    if (gKfsClient->Write(fd, kfsBuf, numBytes) < 0) {
        cout << "Write failed: " << endl;
        exit(0);
    }

    gKfsClient->Seek(fd, 65536 * 2, SEEK_CUR);
    if (gKfsClient->Write(fd, kfsBuf, numBytes) < 0) {
        cout << "Write failed: " << endl;
        exit(0);
    }

    cout << "write is done...." << endl;

    gKfsClient->Truncate(fd, 32768);

    cout << "Truncate to 32768 is done" << endl;

    gKfsClient->Truncate(fd, 100000);

    cout << "Truncate to 100,000 is done" << endl;

    gKfsClient->Close(fd);

    struct stat result;
    gKfsClient->Stat(fileName, result);
    assert(result.st_size == 100000);
    
    delete [] kfsBuf;
    delete [] dataBuf;

    return 0;
}

void
generateData(char *testDataFile, char *buf, int numBytes)
{
    int i;
    ifstream ifs(testDataFile);

    if (!ifs) {
        cout << "Unable to open test data file" << endl;
        exit(0);
    }
    
    for (i = 0; i < numBytes; ++i) {
        ifs >> buf[i];
        if (ifs.eof()) {
            cout << "Test-data file is too small (" << i << " vs. data= " << numBytes << endl;
            exit(0);
        }
    }
}
    
bool
compareData(char *dst, char *src, int numBytes)
{
    int i;

    for (i = 0; i < numBytes; i++) {
        if (dst[i] == src[i])
            continue;
        cout << "Mismatch at index: " << i << endl;
        return false;
    }
    return true;
}
    
