//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/MetaServerSM.h#3 $
//
// Created 2006/06/07
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
// \file MetaServerSM.h
// \brief State machine that interfaces with the meta server and
// handles the RPCs sent by the meta server.
//
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_METASERVERSM_H
#define CHUNKSERVER_METASERVERSM_H

#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/ITimeout.h"
#include "libkfsIO/NetConnection.h"
#include "KfsOps.h"

#include <list>
#include <string>
using std::list;
using std::string;

class MetaServerSMTimeoutImpl;

class MetaServerSM : public KfsCallbackObj {
public:
    MetaServerSM();
    ~MetaServerSM(); 

    /// Init function for configuring the metaserver SM.
    void Init(const ServerLocation &metaLoc);
    /// Send HELLO message
    /// @param[in] chunkServerPort  Port at which chunk-server is
    /// listening for connections from KFS clients.
    /// @retval 0 if we could connect/send HELLO; -1 otherwise
    int SendHello(int chunkServerPort);

    /// Generic event handler to handle RPC requests sent by the meta server.
    int HandleRequest(int code, void *data);

    void HandleMsg(IOBuffer *iobuf, int msgLen);

    void SubmitOp(KfsOp *op);

    /// If the connection to the server breaks, periodically, retry to connect
    void Timeout();

    /// Return the server name/port information
    ServerLocation GetLocation() const {
        return mLocation;
    }

    kfsSeq_t nextSeq() {
        return mCmdSeq++;
    }

private:
    kfsSeq_t mCmdSeq;
    /// where is the server located?
    ServerLocation mLocation;

    /// the port that the metaserver tells the clients to connect to us at.
    int mChunkServerPort;

    /// Track if we have sent a "HELLO" to metaserver
    bool mSentHello;

    list<KfsOp *> mDispatchedOps;

    /// Our connection to the meta server.
    NetConnectionPtr mNetConnection;

    /// A timer to periodically check that the connection to the
    /// server is good; if the connection broke, reconnect and do the
    /// handshake again
    MetaServerSMTimeoutImpl *mTimer;

    /// Connect to the meta server
    /// @retval 0 if connect was successful; -1 otherwise
    int Connect();

    /// Given a (possibly) complete op in a buffer, run it.
    void HandleCmd(IOBuffer *iobuf, int cmdLen);
    /// Handle a reply to an RPC we previously sent.
    void HandleReply(IOBuffer *iobuf, int msgLen);

    /// Op has finished execution.  Send a response to the meta server.
    void SendResponse(KfsOp *op);

    /// We reconnected to the metaserver; so, resend all the pending ops.
    void ResubmitPendingOps();
};

/// A Timeout interface object for checking connection status with the server
class MetaServerSMTimeoutImpl : public ITimeout {
public:
    MetaServerSMTimeoutImpl(MetaServerSM *mgr) {
        mMetaServerSM = mgr; 
        // check once every 5 secs
        SetTimeoutInterval(5*1000);
    };
    /// On a timeout, check that the connection with the server is good
    void Timeout() {
        mMetaServerSM->Timeout();
    };
private:
    /// Owning chunk manager
    MetaServerSM        *mMetaServerSM;
};

extern MetaServerSM gMetaServerSM;

#endif // CHUNKSERVER_METASERVERSM_H
