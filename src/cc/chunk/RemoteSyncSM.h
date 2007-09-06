//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/RemoteSyncSM.h#3 $
//
// Created 2006/09/27
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
