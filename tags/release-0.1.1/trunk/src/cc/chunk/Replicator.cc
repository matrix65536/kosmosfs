//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2007/01/17
// Author: Sriram Rao (Kosmix Corp.) 
//
// Copyright 2007 Kosmix Corp.
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
#include "libkfsIO/Checksum.h"

#include <string>
#include <sstream>

#include "common/log.h"
#include <boost/scoped_array.hpp>
using boost::scoped_array;

using std::string;
using std::ostringstream;
using std::istringstream;
using namespace KFS;
using namespace KFS::libkfsio;


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

    KFS_LOG_VA_DEBUG("Trying to connect to: %s", mOwner->location.ToString().c_str());

    sock = new TcpSocket();
    if (sock->Connect(mOwner->location)) {
        // KFS_LOG_DEBUG("connect failed...");
        delete sock;
        return false;
    }
    KFS_LOG_VA_INFO("Connect to remote server (%s) succeeded...",
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
	// KFS_LOG_VA_DEBUG("Closing connection");

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
	// KFS_LOG_VA_DEBUG("Closing connection");

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

    // align the writes to checksum boundaries
    if ((mWriteOp.numBytes >= CHECKSUM_BLOCKSIZE) &&
        (mWriteOp.numBytes % CHECKSUM_BLOCKSIZE) != 0)
        // round-down so to speak; whatever is left will be picked up by the next read
        mWriteOp.numBytes = (mWriteOp.numBytes / CHECKSUM_BLOCKSIZE) * CHECKSUM_BLOCKSIZE;

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
        KFS_LOG_VA_INFO("Replication for %ld finished", mChunkId);
        gChunkManager.ReplicationDone(mChunkId);
        mOwner->status = 0;
    } else {
        KFS_LOG_VA_INFO("Replication for %ld failed...cleaning up", mChunkId);
        gChunkManager.DeleteChunk(mChunkId);
        mOwner->status = -1;
    }
    // Notify the owner of completion
    mOwner->HandleEvent(EVENT_CMD_DONE, NULL);
}
