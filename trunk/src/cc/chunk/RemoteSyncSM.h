//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/09/27
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

#ifndef CHUNKSERVER_REMOTESYNCSM_H
#define CHUNKSERVER_REMOTESYNCSM_H

#include <list>
using std::list;

#include "libkfsIO/Chunk.h"
#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/DiskConnection.h"
#include "libkfsIO/NetConnection.h"
#include "KfsOps.h"

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

// State machine that asks a remote server to commit a write
class RemoteSyncSM : public KfsCallbackObj,
                     public boost::enable_shared_from_this<RemoteSyncSM>
{
public:

    RemoteSyncSM(const ServerLocation &location) :
        mLocation(location), mSeqnum(1) { };

    ~RemoteSyncSM();

    bool Connect();

    kfsSeq_t NextSeqnum() {
        return mSeqnum++;
    }
    void Enqueue(WriteCommitOp *op);

    int HandleEvent(int code, void *data);

    ServerLocation GetLocation() const {
        return mLocation;
    }

private:
    NetConnectionPtr mNetConnection;

    ServerLocation mLocation;

    /// Queue of outstanding ops sent to remote server(s)
    list<WriteCommitOp *> mDispatchedOps;

    /// Assign a sequence # for each op we send to the remote server
    kfsSeq_t mSeqnum;

    /// We got a response for a write-commit-op.  Match it with the op
    /// that was issued and restart that op.
    void HandleResponse(IOBuffer *iobuf, int cmdLen);
    void FailAllOps();

};

typedef boost::shared_ptr<RemoteSyncSM> RemoteSyncSMPtr;

#endif // CHUNKSERVER_REMOTESYNCSM_H
