//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/03/22
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
// 
//----------------------------------------------------------------------------

extern "C" {
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
}

#include <string>
#include <vector>

#include "common/properties.h"
#include "libkfsIO/DiskManager.h"
#include "libkfsIO/NetManager.h"
#include "libkfsIO/Globals.h"

#include "ChunkServer.h"
#include "ChunkManager.h"

using namespace KFS;
using namespace KFS::libkfsio;
using std::string;
using std::vector;
using std::cout;
using std::endl;

// all the globals we need...
ChunkServer KFS::gChunkServer;
ChunkManager KFS::gChunkManager;
Logger KFS::gLogger;
MetaServerSM KFS::gMetaServerSM;
ClientManager KFS::gClientManager;

string gLogDir;
vector<string> gChunkDirs;

ServerLocation gMetaServerLoc;
size_t gTotalSpace;			// max. storage space to use
int gChunkServerClientPort;	// Port at which kfs clients connect to us

Properties gProp;
const char *gClusterKey;

int gChunkServerCleanupOnStart;

int ReadChunkServerProperties(char *fileName);


int
main(int argc, char **argv)
{
    if (argc < 2) {
        cout << "Usage: " << argv[0] << " <properties file> {<msg log file>}" << endl;
        exit(0);
    }

    if (argc > 2) {
        KFS::MsgLogger::Init(argv[2]);
    } else {
        KFS::MsgLogger::Init(NULL);
    }

    if (ReadChunkServerProperties(argv[1]) != 0) {
        cout << "Bad properties file: " << argv[1] << " aborting...\n";
        exit(-1);
    }
    // Initialize things...
    libkfsio::InitGlobals();
    
    // setup the default allocation unit to be large (16MB); clients
    // are going to be reading/writing 64MB chunks, so allocate as
    // much as possible.
    libkfsio::SetIOBufferSize(1 << 24);
    
    gChunkServer.Init();
    gChunkManager.Init(gChunkDirs, gTotalSpace);
    gLogger.Init(gLogDir);
    gMetaServerSM.Init(gMetaServerLoc, gClusterKey);

    signal(SIGPIPE, SIG_IGN);

    // gChunkServerCleanupOnStart is a debugging option---it provides
    // "silent" cleanup
    if (gChunkServerCleanupOnStart == 0) {
        gChunkManager.Restart();
    }

    gChunkServer.MainLoop(gChunkServerClientPort);

    return 0;
}

static bool
make_if_needed(const char *dirname)
{
    struct stat s;

    if (stat(dirname, &s) == 0 && S_ISDIR(s.st_mode))
	return true;

    return mkdir(dirname, 0755) == 0;
}

///
/// Read and validate the configuration settings for the chunk
/// server. The configuration file is assumed to contain lines of the
/// form: xxx.yyy.zzz = <value>
/// @result 0 on success; -1 on failure
/// @param[in] fileName File that contains configuration information
/// for the chunk server.
///
int
ReadChunkServerProperties(char *fileName)
{
    string::size_type curr = 0, next;
    string chunkDirPaths;

    if (gProp.loadProperties(fileName, '=', true) != 0)
        return -1;

    gMetaServerLoc.hostname = gProp.getValue("chunkServer.metaServer.hostname", "");
    gMetaServerLoc.port = gProp.getValue("chunkServer.metaServer.port", -1);
    if (!gMetaServerLoc.IsValid()) {
        cout << "Aborting...bad meta-server host or port: ";
        cout << gMetaServerLoc.hostname << ':' << gMetaServerLoc.port << '\n';
        return -1;
    }

    gChunkServerClientPort = gProp.getValue("chunkServer.clientPort", -1);
    if (gChunkServerClientPort < 0) {
        cout << "Aborting...bad client port: " << gChunkServerClientPort << '\n';
        return -1;
    }
    cout << "Using chunk server client port: " << gChunkServerClientPort << '\n';

    // Paths are space separated directories for storing chunks
    chunkDirPaths = gProp.getValue("chunkServer.chunkDir", "chunks");

    while (curr < chunkDirPaths.size()) {
        string component;

        next = chunkDirPaths.find(' ', curr);
        if (next == string::npos)
            next = chunkDirPaths.size();

        component.assign(chunkDirPaths, curr, next - curr);

        curr = next + 1;

        if ((component == " ") || (component == "")) {
            continue;
        }

        if (!make_if_needed(component.c_str())) {
            cout << "Aborting...failed to create " << component << '\n';
            return -1;
        }

        // also, make the directory for holding stale chunks in each "partition"
        string staleChunkDir = GetStaleChunkPath(component);
        make_if_needed(staleChunkDir.c_str());

        cout << "Using chunk dir = " << component << '\n';

        gChunkDirs.push_back(component);
    }

    gLogDir = gProp.getValue("chunkServer.logDir", "logs");
    if (!make_if_needed(gLogDir.c_str())) {
	cout << "Aborting...failed to create " << gLogDir << '\n';
	return -1;
    }
    cout << "Using log dir = " << gLogDir << '\n';

    gTotalSpace = gProp.getValue("chunkServer.totalSpace", (long long) 0);
    cout << "Total space = " << gTotalSpace << '\n';

    gChunkServerCleanupOnStart = gProp.getValue("chunkServer.cleanupOnStart", 0);
    cout << "cleanup on start = " << gChunkServerCleanupOnStart << endl;

    gClusterKey = gProp.getValue("chunkServer.clusterKey", "");
    cout << "using cluster key = " << gClusterKey << endl;

    return 0;
}
