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
#include <string>
#include <algorithm>
using std::find_if;
using std::for_each;
using std::istringstream;
using std::ostringstream;
using std::list;
using std::string;

using namespace KFS;
using namespace KFS::libkfsio;

#include <boost/scoped_array.hpp>
using boost::scoped_array;

const int kMaxCmdHeaderLength = 2 << 10;
int  RemoteSyncSM::sOpResponseTimeoutSec = 5 * 60; // 5 min op response timeout

RemoteSyncSM::~RemoteSyncSM()
{
    if (mNetConnection)
        mNetConnection->Close();
    assert(mDispatchedOps.size() == 0);
}

bool
RemoteSyncSM::Connect()
{
    TcpSocket *sock;
    int res;

    assert(! mNetConnection);

    KFS_LOG_VA_DEBUG("Trying to connect to: %s", mLocation.ToString().c_str());

    sock = new TcpSocket();
    // do a non-blocking connect
    res = sock->Connect(mLocation, true);
    if ((res < 0) && (res != -EINPROGRESS)) {
        KFS_LOG_VA_INFO("Connect to remote server (%s) failed: code = %d",
                        mLocation.ToString().c_str(), res);
        delete sock;
        return false;
    }

    KFS_LOG_VA_INFO("Connect to remote server (%s) succeeded (res = %d)...",
                    mLocation.ToString().c_str(), res);

    SET_HANDLER(this, &RemoteSyncSM::HandleEvent);

    mNetConnection.reset(new NetConnection(sock, this));
    mNetConnection->SetDoingNonblockingConnect();
    mNetConnection->SetMaxReadAhead(kMaxCmdHeaderLength);
    
    // If there is no activity on this socket, we want
    // to be notified, so that we can close connection.
    mNetConnection->SetInactivityTimeout(sOpResponseTimeoutSec);
    // Add this to the poll vector
    globals().netManager.AddConnection(mNetConnection);

    return true;
}

void
RemoteSyncSM::Enqueue(KfsOp *op)
{
    if (!mNetConnection) {
        if (!Connect()) {
            KFS_LOG_VA_INFO("Connect to peer %s failed; failing ops", mLocation.ToString().c_str());
            mDispatchedOps.push_back(op);
            FailAllOps();
            return;
        }
    }

    if (!mNetConnection->IsGood()) {
        KFS_LOG_VA_INFO("Lost the connection to peer %s; failing ops", mLocation.ToString().c_str());
        mDispatchedOps.push_back(op);
        FailAllOps();
        mNetConnection->Close();
        mNetConnection.reset();
        return;
    }

    IOBuffer::OStream os;
    op->Request(os);
    mNetConnection->Write(&os);
    if (op->op == CMD_WRITE_PREPARE_FWD) {
        // send the data as well
        WritePrepareFwdOp *wpfo = static_cast<WritePrepareFwdOp *>(op);        
        mNetConnection->Write(wpfo->dataBuf, wpfo->dataBuf->BytesConsumable());
        // fire'n'forget
        op->status = 0;
        KFS::SubmitOpResponse(op);            
    }
    else {
        mDispatchedOps.push_back(op);
    }
    mNetConnection->StartFlush();
}

int
RemoteSyncSM::HandleEvent(int code, void *data)
{
    IOBuffer *iobuf;
    int msgLen = 0, res;
    // take a ref to prevent the object from being deleted
    // while we are still in this function.
    RemoteSyncSMPtr self = shared_from_this();
    const char *reason = "error";

#ifdef DEBUG
    verifyExecutingOnNetProcessor();
#endif    

    switch (code) {
    case EVENT_NET_READ:
	// We read something from the network.  Run the RPC that
	// came in if we got all the data for the RPC
	iobuf = (IOBuffer *) data;
	while (mReplyNumBytes > 0 || IsMsgAvail(iobuf, &msgLen)) {
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

        
    case EVENT_INACTIVITY_TIMEOUT:
    	reason = "inactivity timeout";
    case EVENT_NET_ERROR:
        // If there is an error or there is no activity on the socket
        // for N mins, we close the connection. 
	KFS_LOG_VA_INFO("Closing connection to peer: %s due to %s", mLocation.ToString().c_str(), reason);

	if (mNetConnection)
	    mNetConnection->Close();

        // we are done...
        Finish();

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
    list<KfsOp *>::iterator i = mDispatchedOps.end();
    int nAvail = iobuf->BytesConsumable();

    if (mReplyNumBytes <= 0) {
        assert(msgLen >= 0 && msgLen <= nAvail);
        scoped_array<char> buf(new char[msgLen + 1]);
        iobuf->CopyOut(buf.get(), msgLen);
        buf[msgLen] = '\0';

        istringstream ist(buf.get());

        const char separator = ':';
        Properties prop;
        prop.loadProperties(ist, separator, false);
        mReplySeqNum = prop.getValue("Cseq", (kfsSeq_t) -1);
        mReplyNumBytes = prop.getValue("Content-length", (long long) 0);
        iobuf->Consume(msgLen);
        nAvail -= msgLen;
        i = find_if(mDispatchedOps.begin(), mDispatchedOps.end(), 
                    OpMatcher(mReplySeqNum));
        KfsOp* const op = i != mDispatchedOps.end() ? *i : 0;
        if (op) {
            op->status = prop.getValue("Status", -1);
            if (op->op == CMD_WRITE_ID_ALLOC) {
                WriteIdAllocOp *wiao = static_cast<WriteIdAllocOp *>(op);
                wiao->writeIdStr = prop.getValue("Write-id", "");
            } else if (op->op == CMD_READ) {
                ReadOp *rop = static_cast<ReadOp *> (op);
                const int checksumEntries = prop.getValue("Checksum-entries", 0);
                if (checksumEntries > 0) {
                    string checksums = prop.getValue("Checksums", "");
                    istringstream is(checksums.c_str());
                    uint32_t cks;
                    for (int i = 0; i < checksumEntries; i++) {
                        is >> cks;
                        rop->checksum.push_back(cks);
                    }
                }
                const int off(rop->offset % IOBufferData::GetDefaultBufferSize());
                if (off > 0) {
                    IOBuffer buf;
                    buf.ReplaceKeepBuffersFull(iobuf, off, nAvail);
                    iobuf->Move(&buf);
                    iobuf->Consume(off);
                } else {
                    iobuf->MakeBuffersFull(); 
                }
            } else if (op->op == CMD_SIZE) {
                SizeOp *sop = static_cast<SizeOp *>(op);
                sop->size = prop.getValue("Size", 0);
            } else if (op->op == CMD_GET_CHUNK_METADATA) {
                GetChunkMetadataOp *gcm = static_cast<GetChunkMetadataOp *>(op);
                gcm->chunkVersion = prop.getValue("Chunk-version", 0);
                gcm->chunkSize = prop.getValue("Size", 0);
            }
        }
    }

    // if we don't have all the data for the write, hold on...
    if (nAvail < mReplyNumBytes) {
        // the data isn't here yet...wait...
        mNetConnection->SetMaxReadAhead(mReplyNumBytes - nAvail);
        return -1;
    }

    // now, we got everything...
    mNetConnection->SetMaxReadAhead(kMaxCmdHeaderLength);

    // find the matching op
    if (i == mDispatchedOps.end()) {
        i = find_if(mDispatchedOps.begin(), mDispatchedOps.end(), 
                    OpMatcher(mReplySeqNum));
    }
    if (i != mDispatchedOps.end()) {
        KfsOp *const op = *i;
        mDispatchedOps.erase(i);
        if (op->op == CMD_READ) {
            ReadOp *rop = static_cast<ReadOp *> (op);
            if (rop->dataBuf == NULL)
                rop->dataBuf = new IOBuffer();
            rop->dataBuf->Move(iobuf, mReplyNumBytes);
            rop->numBytesIO = mReplyNumBytes;
        } else if (op->op == CMD_GET_CHUNK_METADATA) {
            GetChunkMetadataOp *gcm = static_cast<GetChunkMetadataOp *>(op);
            if (gcm->dataBuf == NULL)
                gcm->dataBuf = new IOBuffer();
            gcm->dataBuf->Move(iobuf, mReplyNumBytes);            
        }
        mReplyNumBytes = 0;
        // op->HandleEvent(EVENT_DONE, op);
        KFS::SubmitOpResponse(op);
    } else {
        KFS_LOG_VA_DEBUG("Discarding a reply for unknown seq #: %d", mReplySeqNum);
        mReplyNumBytes = 0;
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
    if (mNetConnection) {
	mNetConnection->Close();
        mNetConnection.reset();
    }
    // if the object was owned by the chunkserver, have it release the reference
    gChunkServer.RemoveServer(this);
}

//
// Utility functions to operate on a list of remotesync servers
//

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
KFS::FindServer(list<RemoteSyncSMPtr> &remoteSyncers, const ServerLocation &location, 
                bool connect)
{
    list<RemoteSyncSMPtr>::iterator i;
    RemoteSyncSMPtr peer;

    i = find_if(remoteSyncers.begin(), remoteSyncers.end(),
                RemoteSyncSMMatcher(location));
    if (i != remoteSyncers.end()) {
        peer = *i;
        return peer;
    }
    if (!connect)
        return peer;

    peer.reset(new RemoteSyncSM(location));
    if (peer->Connect()) {
        remoteSyncers.push_back(peer);
    } else {
        // we couldn't connect...so, force destruction
        peer.reset();
    }
    return peer;
}

void
KFS::RemoveServer(list<RemoteSyncSMPtr> &remoteSyncers, RemoteSyncSM *target)
{
    list<RemoteSyncSMPtr>::iterator i;

    i = find_if(remoteSyncers.begin(), remoteSyncers.end(),
                RemoteSyncSMMatcher(target->GetLocation()));
    if (i != remoteSyncers.end()) {
        RemoteSyncSMPtr r = *i;
        if (r.get() == target) {
            remoteSyncers.erase(i);
        }
    }
}

void
KFS::ReleaseAllServers(list<RemoteSyncSMPtr> &remoteSyncers)
{
    list<RemoteSyncSMPtr>::iterator i;
    while (1) {
        i = remoteSyncers.begin();
        if (i == remoteSyncers.end())
            break;
        RemoteSyncSMPtr r = *i;

        r->Finish();
        remoteSyncers.erase(i);
    }
}
