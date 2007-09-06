//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/metaserver_main.cc#3 $
//
// Created 2006/03/22
// Author: Blake Lewis (Kosmix Corp.)
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
// \file Main.cc
// \brief Driver code that starts up the meta server
// 
//----------------------------------------------------------------------------

#include <signal.h>

#include "common/properties.h"
#include "libkfsIO/NetManager.h"
#include "libkfsIO/Globals.h"

#include "NetDispatch.h"
#include "startup.h"

using namespace KFS;

// Port at which KFS clients connect and send RPCs
int gClientPort;

// Port at which Chunk servers connect
int gChunkServerPort;

// paths for logs and checkpoints
string gLogDir, gCPDir;

Properties gProp;
NetDispatch KFS::gNetDispatch;

int ReadMetaServerProperties(char *fileName);

using std::cout;
using std::endl;

int
main(int argc, char **argv)
{
        fd_set rfds;

        if (argc < 2) {
                cout << "Usage: " << argv[0] << " <properties file> " << endl;
                exit(0);
        }
        if (ReadMetaServerProperties(argv[1]) != 0) {
                cout << "Bad properties file: " << argv[1] << " aborting..." << endl;
                exit(-1);
        }

	libkfsio::InitGlobals();

        kfs_startup(gLogDir, gCPDir);

        // Ignore SIGPIPE's that generated when clients break TCP
        // connection.
        //
        signal(SIGPIPE, SIG_IGN);
    
        gNetDispatch.Start(gClientPort, gChunkServerPort);

        // block the main thread without consuming too much CPU
        while (1) {
                FD_ZERO(&rfds);
                FD_SET(0, &rfds);
                select(1, &rfds, NULL, NULL, NULL);
        }
        

        return 0;
}

///
/// Read and validate the configuration settings for the meta
/// server. The configuration file is assumed to contain lines of the
/// form: xxx.yyy.zzz = <value>
/// @result 0 on success; -1 on failure
/// @param[in] fileName File that contains configuration information
/// for the chunk server.
///

int
ReadMetaServerProperties(char *fileName)
{
        if (gProp.loadProperties(fileName, '=', true) != 0)
                return -1;

        gClientPort = gProp.getValue("metaServer.clientPort", -1);
        if (gClientPort < 0) {
                cout << "Aborting...bad client port: " << gClientPort << endl;
                return -1;
        }
        cout << "Using meta server client port: " << gClientPort << endl;

        gChunkServerPort = gProp.getValue("metaServer.chunkServerPort", -1);
        if (gChunkServerPort < 0) {
                cout << "Aborting...bad chunk server port: " << gChunkServerPort << endl;
                return -1;
        }
        cout << "Using meta server chunk server port: " << gChunkServerPort << endl;
	gLogDir = gProp.getValue("metaServer.logDir","");
	gCPDir = gProp.getValue("metaServer.cpDir","");
	
        return 0;
}
