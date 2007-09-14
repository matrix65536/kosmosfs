//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/RemoteSyncSM.cc#2 $
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

#include "RemoteSyncSM.h"
#include "Utils.h"
#include "ChunkServer.h"
#include "libkfsIO/NetManager.h"
#include "libkfsIO/Globals.h"
using namespace libkfsio;

#include "common/log.h"
#include "common/properties.h"

#include <cerrno>
#include <sstream>
#include <algorithm>
using std::find_if;
using std::for_each;
using std::istringstream;
using std::ostringstream;

#include <boost/scoped_array.hpp>
using boost::scoped_array;

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

    COSMIX_LOG_DEBUG("Trying to connect to: %s", mLocation.ToString().c_str());

    sock = new TcpSocket();
    if (sock->Connect(mLocation)) {
        COSMIX_LOG_DEBUG("connect failed...");
        delete sock;
        return false;
    }
    COSMIX_LOG_INFO("Connect to remote server (%s) succeeded...",
                    mLocation.ToString().c_str());

    mNetConnection.reset(new NetConnection(sock, this));
    // Add this to the poll vector
    globals().netManager.AddConnection(mNetConnection);

    SET_HANDLER(this, &RemoteSyncSM::HandleEvent);

    return true;
}

void
RemoteSyncSM::Enqueue(WriteCommitOp *op)
{
    ostringstream os;

    mDispatchedOps.push_back(op);

    if (!mNetConnection) {
        if (!Connect()) {
            FailAllOps();
            return;
        }
    }
    op->Request(os);
    mNetConnection->Write(os.str().c_str(), os.str().length());
}

int
RemoteSyncSM::HandleEvent(int code, void *data)
{
    IOBuffer *iobuf;
    int msgLen;
    // take a ref to prevent the object from being deleted
    // while we are still in this function.
    RemoteSyncSMPtr self = shared_from_this();

    switch (code) {
    case EVENT_NET_READ:
	// We read something from the network.  Run the RPC that
	// came in.
	iobuf = (IOBuffer *) data;
	while (IsMsgAvail(iobuf, &msgLen)) {
	    HandleResponse(iobuf, msgLen);
	}
	break;

    case EVENT_NET_WROTE:
	// Something went out on the network.  For now, we don't
	// track it. Later, we may use it for tracking throttling
	// and such.
	break;

    case EVENT_NET_ERROR:
	COSMIX_LOG_DEBUG("Closing connection");

	if (mNetConnection)
	    mNetConnection->Close();

	gChunkServer.RemoveServer(this);
	// fail all the ops
	FailAllOps();

	break;

    default:
	assert(!"Unknown event");
	break;
    }
    return 0;

}

void
RemoteSyncSM::HandleResponse(IOBuffer *iobuf, int msgLen)
{
    const char separator = ':';
    scoped_array<char> buf(new char[msgLen + 1]);
    Properties prop;
    kfsSeq_t seqNum;
    int status;
    list<WriteCommitOp *>::iterator i;

    iobuf->CopyOut(buf.get(), msgLen);
    buf[msgLen] = '\0';
    iobuf->Consume(msgLen);

    istringstream ist(buf.get());

    prop.loadProperties(ist, separator, false);
    seqNum = prop.getValue("Cseq", (kfsSeq_t) -1);
    status = prop.getValue("Status", -1);

    // find the matching op
    i = find_if(mDispatchedOps.begin(), mDispatchedOps.end(), 
                OpMatcher(seqNum));
    if (i != mDispatchedOps.end()) {
        WriteCommitOp *op = *i;
        op->status = status;
        mDispatchedOps.erase(i);
        op->HandleEvent(EVENT_DONE, op);
    }
}

// Helper functor that fails an op with an error code.
class OpFailer {
    int errCode;
public:
    OpFailer(int c) : errCode(c) { };
    void operator() (KfsOp *op) {
        op->status = errCode;
        op->HandleEvent(EVENT_DONE, op);
    }
};


void
RemoteSyncSM::FailAllOps()
{
    for_each(mDispatchedOps.begin(), mDispatchedOps.end(),
             OpFailer(-EHOSTUNREACH));
    mDispatchedOps.clear();
}
