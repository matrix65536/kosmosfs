//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/09/27
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

#ifndef CHUNKSERVER_REMOTESYNCSM_H
#define CHUNKSERVER_REMOTESYNCSM_H

#include <list>

#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/DiskConnection.h"
#include "libkfsIO/NetConnection.h"
#include "KfsOps.h"
#include "Chunk.h"
#include "libkfsIO/ITimeout.h"
#include "meta/queue.h"

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

namespace KFS
{

class RemoteSyncSMTimeoutImpl;

// State machine that asks a remote server to commit a write
class RemoteSyncSM : public KfsCallbackObj,
                     public boost::enable_shared_from_this<RemoteSyncSM>
{
public:

    RemoteSyncSM(const ServerLocation &location) :
        mLocation(location), mSeqnum(1), mTimer(NULL),
        mKillRemoteSyncOp(1, this)
    { };

    ~RemoteSyncSM();

    bool Connect();

    kfsSeq_t NextSeqnum() {
        return mSeqnum++;
    }
    void Enqueue(KfsOp *op);

    void Finish();

    // void Dispatch();

    int HandleEvent(int code, void *data);

    ServerLocation GetLocation() const {
        return mLocation;
    }

private:
    NetConnectionPtr mNetConnection;

    ServerLocation mLocation;

    /// Assign a sequence # for each op we send to the remote server
    kfsSeq_t mSeqnum;

    /// A timer to periodically dispatch pending
    /// messages to the remote server
    RemoteSyncSMTimeoutImpl *mTimer;

    KillRemoteSyncOp mKillRemoteSyncOp;

    /// Queue of outstanding ops to be dispatched: this is the queue
    /// shared between event processor and the network threads.  When
    /// the network dispatcher runs, it pulls messages from this queue
    /// and stashes them away in the dispatched ops list.
    MetaQueue<KfsOp> mPendingOps;

    /// Queue of outstanding ops sent to remote server.
    std::list<KfsOp *> mDispatchedOps;

    /// We (may) have got a response from the peer.  If we are doing
    /// re-replication, then we need to wait until we got all the data
    /// for the op; in such cases, we need to know if we got the full
    /// response. 
    /// @retval 0 if we got the response; -1 if we need to wait
    int HandleResponse(IOBuffer *iobuf, int cmdLen);
    void FailAllOps();
};

/// A Timeout interface object for dispatching messages.
class RemoteSyncSMTimeoutImpl : public ITimeout {
public:
    RemoteSyncSMTimeoutImpl(RemoteSyncSM *mgr) {
        mRemoteSyncSM = mgr; 
    };
    /// On each timeout, check that the connection with the server is
    /// good.  Also, dispatch any pending messages.
    void Timeout() {
        // mRemoteSyncSM->Dispatch();
    };
private:
    /// Owning remote-sync SM.
    RemoteSyncSM        *mRemoteSyncSM;
};


typedef boost::shared_ptr<RemoteSyncSM> RemoteSyncSMPtr;

}

#endif // CHUNKSERVER_REMOTESYNCSM_H
