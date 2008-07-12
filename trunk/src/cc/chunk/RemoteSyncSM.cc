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

#include "RemoteSyncSM.h"
#include "Utils.h"
#include "ChunkServer.h"
#include "libkfsIO/NetManager.h"
#include "libkfsIO/Globals.h"

#include "common/log.h"
#include "common/properties.h"

#include <cerrno>
#include <sstream>
#include <algorithm>
using std::find_if;
using std::for_each;
using std::istringstream;
using std::ostringstream;
using std::list;

using namespace KFS;
using namespace KFS::libkfsio;

#include <boost/scoped_array.hpp>
using boost::scoped_array;

RemoteSyncSM::~RemoteSyncSM()
{
    if (mTimer)
        globals().netManager.UnRegisterTimeoutHandler(mTimer);
    delete mTimer;

    if (mNetConnection)
        mNetConnection->Close();
    assert(mDispatchedOps.size() == 0);
    assert(mPendingOps.empty());
}

bool
RemoteSyncSM::Connect()
{
    TcpSocket *sock;
    int res;

    KFS_LOG_VA_DEBUG("Trying to connect to: %s", mLocation.ToString().c_str());

    sock = new TcpSocket();
    // do a non-blocking connect
    res = sock->Connect(mLocation, true);
    if ((res < 0) && (res != -EINPROGRESS)) {
        KFS_LOG_VA_INFO("Connect to remote server (%s) failed...",
                         mLocation.ToString().c_str());
        delete sock;
        return false;
    }

    if (mTimer == NULL) {
        mTimer = new RemoteSyncSMTimeoutImpl(this);
        globals().netManager.RegisterTimeoutHandler(mTimer);
    }

    KFS_LOG_VA_INFO("Connect to remote server (%s) succeeded...",
                    mLocation.ToString().c_str());

    SET_HANDLER(this, &RemoteSyncSM::HandleEvent);

    mNetConnection.reset(new NetConnection(sock, this));
    mNetConnection->SetDoingNonblockingConnect();
    // Add this to the poll vector
    globals().netManager.AddConnection(mNetConnection);

    return true;
}

void
RemoteSyncSM::Enqueue(KfsOp *op)
{
    ostringstream os;
        
    if (!mNetConnection) {
        if (!Connect()) {
            mDispatchedOps.push_back(op);
            FailAllOps();
            return;
        }
    }

    op->Request(os);
    mNetConnection->Write(os.str().c_str(), os.str().length());
    if (op->op == CMD_WRITE_PREPARE_FWD) {
        // send the data as well
        WritePrepareFwdOp *wpfo = static_cast<WritePrepareFwdOp *>(op);        
        mNetConnection->Write(wpfo->dataBuf, wpfo->dataBuf->BytesConsumable());
        // fire'n'forget
        op->status = 0;
        KFS::SubmitOpResponse(op);            
    }
    else
        mDispatchedOps.push_back(op);
    mNetConnection->StartFlush();
}

#if 0
void
RemoteSyncSM::Enqueue(KfsOp *op)
{
    mPendingOps.enqueue(op);
    // gChunkServer.ToggleNetThreadKicking(true);
    globals().netKicker.Kick();    
}

void
RemoteSyncSM::Dispatch()
{
    KfsOp *op;

    while ((op = mPendingOps.dequeue_nowait()) != NULL) {
        ostringstream os;
        
        if (!mNetConnection) {
            if (!Connect()) {
                mDispatchedOps.push_back(op);
                FailAllOps();
                return;
            }
        }

        op->Request(os);
        mNetConnection->Write(os.str().c_str(), os.str().length());
        if (op->op == CMD_WRITE_PREPARE_FWD) {
            // send the data as well
            WritePrepareFwdOp *wpfo = static_cast<WritePrepareFwdOp *>(op);        
            mNetConnection->Write(wpfo->dataBuf, wpfo->dataBuf->BytesConsumable());
            // fire'n'forget
            op->status = 0;
            KFS::SubmitOpResponse(op);            
        }
        else
            mDispatchedOps.push_back(op);            
    }
}
#endif
int
RemoteSyncSM::HandleEvent(int code, void *data)
{
    IOBuffer *iobuf;
    int msgLen, res;
    // take a ref to prevent the object from being deleted
    // while we are still in this function.
    RemoteSyncSMPtr self = shared_from_this();

#ifdef DEBUG
    verifyExecutingOnNetProcessor();
#endif    

    switch (code) {
    case EVENT_NET_READ:
	// We read something from the network.  Run the RPC that
	// came in if we got all the data for the RPC
	iobuf = (IOBuffer *) data;
	while (IsMsgAvail(iobuf, &msgLen)) {
	    res = HandleResponse(iobuf, msgLen);
            if (res < 0)
                // maybe the response isn't fully available
                break;
	}
	break;

    case EVENT_NET_WROTE:
	// Something went out on the network.  For now, we don't
	// track it. Later, we may use it for tracking throttling
	// and such.
	break;

    case EVENT_NET_ERROR:
	KFS_LOG_DEBUG("Closing connection");

	if (mNetConnection)
	    mNetConnection->Close();

        globals().netManager.UnRegisterTimeoutHandler(mTimer);
        delete mTimer;
        mTimer = NULL;

	// fail all the ops
	FailAllOps();

        //  submit this as an op thru the event processor to remove
        //  this one....when we remove the server, we fail any other
        //  ops at that point
        KFS::SubmitOp(&mKillRemoteSyncOp);

	break;

    default:
	assert(!"Unknown event");
	break;
    }
    return 0;

}

int
RemoteSyncSM::HandleResponse(IOBuffer *iobuf, int msgLen)
{
    const char separator = ':';
    scoped_array<char> buf(new char[msgLen + 1]);
    Properties prop;
    kfsSeq_t seqNum;
    int status;
    list<KfsOp *>::iterator i;
    size_t numBytes;
    int64_t nAvail;

    iobuf->CopyOut(buf.get(), msgLen);
    buf[msgLen] = '\0';

    istringstream ist(buf.get());

    prop.loadProperties(ist, separator, false);
    seqNum = prop.getValue("Cseq", (kfsSeq_t) -1);
    status = prop.getValue("Status", -1);
    numBytes = prop.getValue("Content-length", (long long) 0);

    // if we don't have all the data for the write, hold on...
    nAvail = iobuf->BytesConsumable() - msgLen;        
    if (nAvail < (int64_t) numBytes) {
        // the data isn't here yet...wait...
        return -1;
    }

    // now, we got everything...
    iobuf->Consume(msgLen);

    // find the matching op
    i = find_if(mDispatchedOps.begin(), mDispatchedOps.end(), 
                OpMatcher(seqNum));
    if (i != mDispatchedOps.end()) {
        KfsOp *op = *i;
        op->status = status;
        mDispatchedOps.erase(i);
        if (op->op == CMD_WRITE_ID_ALLOC) {
            WriteIdAllocOp *wiao = static_cast<WriteIdAllocOp *>(op);

            wiao->writeIdStr = prop.getValue("Write-id", "");
        } else if (op->op == CMD_READ) {
            ReadOp *rop = static_cast<ReadOp *> (op);
            if (rop->dataBuf == NULL)
                rop->dataBuf = new IOBuffer();
            rop->dataBuf->Move(iobuf, numBytes);
        } else if (op->op == CMD_SIZE) {
            SizeOp *sop = static_cast<SizeOp *>(op);
            sop->size = prop.getValue("Size", 0);
        }
        // op->HandleEvent(EVENT_DONE, op);
        KFS::SubmitOpResponse(op);
    } else {
        KFS_LOG_VA_DEBUG("Discarding a reply for unknown seq #: %d", seqNum);
    }
    return 0;
}

// Helper functor that fails an op with an error code.
class OpFailer {
    int errCode;
public:
    OpFailer(int c) : errCode(c) { };
    void operator() (KfsOp *op) {
        op->status = errCode;
        // op->HandleEvent(EVENT_DONE, op);
        KFS::SubmitOpResponse(op);
    }
};


void
RemoteSyncSM::FailAllOps()
{
    KfsOp *op;
    // get rid of the pending ones as well
    while ((op = mPendingOps.dequeue_nowait()) != NULL) {
        mDispatchedOps.push_back(op);
    }
    
    for_each(mDispatchedOps.begin(), mDispatchedOps.end(),
             OpFailer(-EHOSTUNREACH));
    mDispatchedOps.clear();
}

void
RemoteSyncSM::Finish()
{
#ifdef DEBUG
    verifyExecutingOnEventProcessor();
#endif    

    FailAllOps();
    gChunkServer.RemoveServer(this);
}
