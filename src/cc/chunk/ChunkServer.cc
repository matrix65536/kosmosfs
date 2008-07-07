//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/03/23
// Author: Sriram Rao
//
// Copyright 2008 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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

// order of #includes here is critical.  don't change it
#include "libkfsIO/Counter.h"
#include "libkfsIO/Globals.h"

#include "ChunkServer.h"
#include "Utils.h"

using std::list;

using namespace KFS;
using namespace KFS::libkfsio;

// single network thread that manages connections and net I/O
static MetaThread netProcessor;

ChunkServer KFS::gChunkServer;

static void *
netWorker(void *dummy)
{
    globals().netManager.MainLoop();
    return NULL;
}

static void
StartNetProcessor()
{
    netProcessor.start(netWorker, NULL);
}

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

    // KFS_LOG_VA_DEBUG("Hostname: %s", hostname);
    
    mLocation.Reset(hostname, clientAcceptPort);

    gClientManager.StartAcceptor(clientAcceptPort);
    gLogger.Start();
    gChunkManager.Start();
    // gMetaServerSM.SendHello(clientAcceptPort);
    gMetaServerSM.Init(clientAcceptPort);

    StartNetProcessor();
    
    netProcessor.join();

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
    } else {
        // we couldn't connect...so, force destruction
        peer.reset();
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


void
KFS::verifyExecutingOnNetProcessor()
{
    assert(netProcessor.isEqual(pthread_self()));
    if (!netProcessor.isEqual(pthread_self())) {
        die("FATAL: Not executing on net processor");
    }
}

void
KFS::StopNetProcessor(int status)
{
    netProcessor.exit(status);
}
