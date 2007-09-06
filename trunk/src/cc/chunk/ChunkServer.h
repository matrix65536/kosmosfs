//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/ChunkServer.h#2 $
//
// Created 2006/03/16
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

#ifndef _CHUNKSERVER_H
#define _CHUNKSERVER_H

#include "libkfsIO/Acceptor.h"
#include "libkfsIO/DiskManager.h"
#include "libkfsIO/EventManager.h"
#include "libkfsIO/NetManager.h"
#include "libkfsIO/Globals.h"

#include "ChunkManager.h"
#include "ClientManager.h"
#include "ClientSM.h"
#include "MetaServerSM.h"
#include "RemoteSyncSM.h"

class ChunkServer {
public:
    ChunkServer() { };
    
    void Init();

    void MainLoop(int clientAcceptPort);

    bool IsLocalServer(const ServerLocation &location) const {
        return mLocation == location;
    }
    RemoteSyncSMPtr FindServer(const ServerLocation &location,
                               bool connect = true);
    void RemoveServer(RemoteSyncSM *target);

private:
    int mClientAcceptPort;
    
    ServerLocation mLocation;
    list<RemoteSyncSMPtr> mRemoteSyncers;

};

extern ChunkServer gChunkServer;

#endif // _CHUNKSERVER_H
