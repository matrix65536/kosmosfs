//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/ChunkServer_main.cc#2 $
//
// Created 2006/03/22
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
// 
//----------------------------------------------------------------------------

extern "C" {
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
}

#include "common/properties.h"
#include "libkfsIO/DiskManager.h"
#include "libkfsIO/NetManager.h"
#include "libkfsIO/Globals.h"

#include "ChunkServer.h"
#include "ChunkManager.h"

using namespace libkfsio;

// all the globals we need...
ChunkServer gChunkServer;
ChunkManager gChunkManager;
Logger gLogger;
MetaServerSM gMetaServerSM;
ClientManager gClientManager;

const char *gChunksDir, *gLogDir;
ServerLocation gMetaServerLoc;
size_t gTotalSpace;			// max. storage space to use
int gChunkServerClientPort;	// Port at which kfs clients connect to us

Properties gProp;

int gChunkServerCleanupOnStart;

int ReadChunkServerProperties(char *fileName);

using std::cout;
using std::endl;

int
main(int argc, char **argv)
{
    if (argc < 2) {
        cout << "Usage: " << argv[0] << " <properties file>\n";
        exit(0);
    }
    if (ReadChunkServerProperties(argv[1]) != 0) {
        cout << "Bad properties file: " << argv[1] << " aborting...\n";
        exit(-1);
    }
    // Initialize things...
    libkfsio::InitGlobals();

    gChunkServer.Init();
    gChunkManager.Init(gChunksDir, gTotalSpace);
    gLogger.Init(gLogDir);
    gMetaServerSM.Init(gMetaServerLoc);

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

    gChunksDir = gProp.getValue("chunkServer.chunkDir", "chunks");
    if (!make_if_needed(gChunksDir)) {
	cout << "Aborting...failed to create " << gChunksDir << '\n';
	return -1;
    }
    cout << "Using chunk dir = " << gChunksDir << '\n';

    gLogDir = gProp.getValue("chunkServer.logDir", "logs");
    if (!make_if_needed(gLogDir)) {
	cout << "Aborting...failed to create " << gLogDir << '\n';
	return -1;
    }
    cout << "Using log dir = " << gLogDir << '\n';

    gTotalSpace = gProp.getValue("chunkServer.totalSpace", (long long) 0);
    cout << "Total space = " << gTotalSpace << '\n';

    gChunkServerCleanupOnStart = gProp.getValue("chunkServer.cleanupOnStart", 0);
    cout << "cleanup on start = " << gChunkServerCleanupOnStart << endl;
    return 0;
}
