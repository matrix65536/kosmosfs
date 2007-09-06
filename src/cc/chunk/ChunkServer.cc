//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/ChunkServer.cc#2 $
//
// Created 2006/03/23
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

// order of #includes here is critical.  don't change it
#include "libkfsIO/Counter.h"
#include "libkfsIO/Globals.h"
using namespace libkfsio;

#include "ChunkServer.h"

void
ChunkServer::Init()
{
    InitParseHandlers();
    // Register the counters
    RegisterCounters();
}

void
ChunkServer::MainLoop(int clientAcceptPort)
{
    static const int MAXHOSTNAMELEN = 256;
    char hostname[MAXHOSTNAMELEN];

    mClientAcceptPort = clientAcceptPort;
    if (gethostname(hostname, MAXHOSTNAMELEN)) {
        perror("gethostname: ");
        exit(-1);
    }

    COSMIX_LOG_DEBUG("Hostname: %s", hostname);
    
    mLocation.Reset(hostname, clientAcceptPort);

    gClientManager.StartAcceptor(clientAcceptPort);
    gLogger.Start();
    gChunkManager.Start();
    gMetaServerSM.SendHello(clientAcceptPort);
    globals().netManager.MainLoop();
}

class RemoteSyncSMMatcher {
    ServerLocation myLoc;
public:
    RemoteSyncSMMatcher(const ServerLocation &loc) :
        myLoc(loc) { }
    bool operator() (RemoteSyncSMPtr other) {
        return other->GetLocation() == myLoc;
    }
};

RemoteSyncSMPtr
ChunkServer::FindServer(const ServerLocation &location, bool connect)
{
    list<RemoteSyncSMPtr>::iterator i;
    RemoteSyncSMPtr peer;

    i = find_if(mRemoteSyncers.begin(), mRemoteSyncers.end(),
                RemoteSyncSMMatcher(location));
    if (i != mRemoteSyncers.end()) {
        peer = *i;
        return peer;
    }
    if (!connect)
        return peer;

    peer.reset(new RemoteSyncSM(location));
    if (peer->Connect()) {
        mRemoteSyncers.push_back(peer);
    }
    return peer;
}

void
ChunkServer::RemoveServer(RemoteSyncSM *target)
{
    list<RemoteSyncSMPtr>::iterator i;

    i = find_if(mRemoteSyncers.begin(), mRemoteSyncers.end(),
                RemoteSyncSMMatcher(target->GetLocation()));
    if (i == mRemoteSyncers.end()) {
        return;
    }
    mRemoteSyncers.erase(i);
}
