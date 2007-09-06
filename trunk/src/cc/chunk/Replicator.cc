//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/Replicator.cc#2 $
//
// Created 2007/01/17
// Author: Sriram Rao (Kosmix Corp.) 
//
// Copyright (C) 2007 Kosmix Corp.
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
// \brief Code for dealing with a replicating a chunk.  The metaserver
//asks a destination chunkserver to obtain a copy of a chunk from a source
//chunkserver; in response, the destination chunkserver pulls the data
//down and writes it out to disk.  At the end replication, the
//destination chunkserver notifies the metaserver.
//----------------------------------------------------------------------------

#include "Replicator.h"
#include "ChunkServer.h"
#include "Utils.h"
#include "libkfsIO/Globals.h"
using namespace libkfsio;

#include <string>
#include <sstream>
using std::string;
using std::ostringstream;
using std::istringstream;

#include "common/log.h"
#include <boost/scoped_array.hpp>
using boost::scoped_array;

Replicator::Replicator(ReplicateChunkOp *op) :
    mFileId(op->fid), mChunkId(op->chunkId), 
    mChunkVersion(op->chunkVersion), 
    mOwner(op), mWriteOp(op->chunkId, op->chunkVersion),
    mReadOp(0), mDone(false), mSeq(1),  mOffset(0)
{
    mReadOp.chunkId = op->chunkId;
    mReadOp.chunkVersion = op->chunkVersion;
    mReadOp.clnt = this;
    mWriteOp.clnt = this;
}

Replicator::~Replicator()
{
    if (mNetConnection)
        mNetConnection->Close();
}

bool
Replicator::Connect()
{
    TcpSocket *sock;

    COSMIX_LOG_DEBUG("Trying to connect to: %s", mOwner->location.ToString().c_str());

    sock = new TcpSocket();
    if (sock->Connect(mOwner->location)) {
        COSMIX_LOG_DEBUG("connect failed...");
        delete sock;
        return false;
    }
    COSMIX_LOG_INFO("Connect to remote server (%s) succeeded...",
                    mOwner->location.ToString().c_str());

    mNetConnection.reset(new NetConnection(sock, this));
    // Add this to the poll vector
    globals().netManager.AddConnection(mNetConnection);

    return true;
}

void
Replicator::Start()
{
    ostringstream os;
    ReplicatorPtr self = shared_from_this();
    SizeOp op(NextSeq(), mChunkId, mChunkVersion);

    op.Request(os);
    mNetConnection->Write(os.str().c_str(), os.str().length());
    
    SET_HANDLER(this, &Replicator::HandleStart);
    return;

}

int
Replicator::HandleStart(int code, void *data)
{
    IOBuffer *iobuf;
    int msgLen, res;
    ReplicatorPtr self = shared_from_this();

    switch (code) {
    case EVENT_NET_READ:
	// We read something from the network.  Run the RPC that
	// came in.
	iobuf = (IOBuffer *) data;
	if (IsMsgAvail(iobuf, &msgLen)) {
            res = ReadSetup(iobuf, msgLen);
            if (res < 0)
                break;
            // get rid of the header
            iobuf->Consume(msgLen);
            Read();
	}
	break;

    case EVENT_NET_WROTE:
	break;

    case EVENT_NET_ERROR:
	COSMIX_LOG_DEBUG("Closing connection");

	if (mNetConnection)
	    mNetConnection->Close();

        Terminate();
	break;

    default:
	assert(!"Unknown event");
	break;
    }
    return 0;
}

int
Replicator::ReadSetup(IOBuffer *iobuf, int msgLen)
{
    const char separator = ':';
    scoped_array<char> buf(new char[msgLen + 1]);
    Properties prop;
    int status;
 
    iobuf->CopyOut(buf.get(), msgLen);
    buf[msgLen] = '\0';

    istringstream ist(buf.get());

    prop.loadProperties(ist, separator, false);
    status = prop.getValue("Status", -1);
    if (status < 0) {
        Terminate();
        return -1;
    }

    mChunkSize = prop.getValue("Size", (long long) 0);

    // allocate a file to hold the chunk
    if (gChunkManager.AllocChunk(mFileId, mChunkId, mChunkVersion, true) < 0) {
        Terminate();
        return -1;
    }
    return 0;
}

void
Replicator::Read()
{
    ostringstream os;
    ReplicatorPtr self = shared_from_this();

    if (mOffset >= (off_t) mChunkSize) {
        mDone = true;
        Terminate();
        return;
    }

    mReadOp.seq = NextSeq();
    mReadOp.status = 0;
    mReadOp.offset = mOffset;
    // read an MB 
    mReadOp.numBytes = 1 << 20;
    mReadOp.Request(os);
    mNetConnection->Write(os.str().c_str(), os.str().length());
    
    SET_HANDLER(this, &Replicator::HandleRead);
    return;
}

int
Replicator::HandleRead(int code, void *data)
{
    IOBuffer *iobuf;
    int msgLen;
    size_t numBytes;
    ReplicatorPtr self = shared_from_this();

    switch (code) {
    case EVENT_NET_READ:
	// We read something from the network.  Run the RPC that
	// came in.
	iobuf = (IOBuffer *) data;
	if (IsMsgAvail(iobuf, &msgLen)) {
	    if (IsValidReadResponse(iobuf, msgLen, numBytes)) {
                // get rid of the header
                iobuf->Consume(msgLen);
                Write(iobuf, numBytes);
            }
	}
	break;

    case EVENT_NET_WROTE:
	break;

    case EVENT_NET_ERROR:
	COSMIX_LOG_DEBUG("Closing connection");

	if (mNetConnection)
	    mNetConnection->Close();

        Terminate();
	break;

    default:
	assert(!"Unknown event");
	break;
    }
    return 0;
}

bool
Replicator::IsValidReadResponse(IOBuffer *iobuf, int msgLen, size_t &numBytes)
{
    const char separator = ':';
    scoped_array<char> buf(new char[msgLen + 1]);
    Properties prop;
    kfsSeq_t seqNum;
    int status;
    int64_t nAvail;

    iobuf->CopyOut(buf.get(), msgLen);
    buf[msgLen] = '\0';

    istringstream ist(buf.get());

    prop.loadProperties(ist, separator, false);
    seqNum = prop.getValue("Cseq", (kfsSeq_t) -1);
    status = prop.getValue("Status", -1);
    numBytes = prop.getValue("Content-length", (long long) 0);

    if (status < 0) {
        Terminate();
        return false;
    }

    // if we don't have all the data for the write, hold on...
    nAvail = iobuf->BytesConsumable() - msgLen;        
    if (nAvail < (int64_t) numBytes) {
        // we couldn't process the command...so, wait
        return false;
    }

    return true;
}

void
Replicator::Write(IOBuffer *iobuf, int numBytes)
{
    if (numBytes == 0) {
        mDone = true;
        Terminate();
        return;
    }
        
    delete mWriteOp.dataBuf;
    
    mWriteOp.Reset();

    mWriteOp.dataBuf = new IOBuffer();
    mWriteOp.dataBuf->Move(iobuf, numBytes);
    mWriteOp.offset = mOffset;
    mWriteOp.numBytes = numBytes;

    SET_HANDLER(this, &Replicator::HandleWrite);

    if (gChunkManager.WriteChunk(&mWriteOp) < 0) {
        // abort everything
        Terminate();
    }
    
}

int
Replicator::HandleWrite(int code, void *data)
{
    ReplicatorPtr self = shared_from_this();

    assert(code == EVENT_CMD_DONE);

    if (mWriteOp.status < 0) {
        Terminate();
        return 0;
    }

    mOffset += mWriteOp.numBytesIO;

    if (!mNetConnection->IsGood()) {
        Terminate();
        return 0;
    }
    Read();
    return 0;
}

void
Replicator::Terminate()
{
    if (mDone) {
        COSMIX_LOG_DEBUG("Replication for %ld finished", mChunkId);
        gChunkManager.ReplicationDone(mChunkId);
        mOwner->status = 0;
    } else {
        COSMIX_LOG_DEBUG("Replication for %ld failed...cleaning up", mChunkId);
        gChunkManager.DeleteChunk(mChunkId);
        mOwner->status = -1;
    }
    // Notify the owner of completion
    mOwner->HandleEvent(EVENT_CMD_DONE, NULL);
}
