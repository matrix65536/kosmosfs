//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/Replicator.h#3 $
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
// \brief Code to deal with (re)replicating a chunk.
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_REPLICATOR_H
#define CHUNKSERVER_REPLICATOR_H

#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/NetConnection.h"
#include "KfsOps.h"

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

class Replicator : public KfsCallbackObj,
                   public boost::enable_shared_from_this<Replicator>
{
public:
    // Model for doing a chunk replication involves 3 steps:
    //  - First, figure out the size of the chunk.
    //  - Second in a loop: 
    //        - read N bytes from the source
    //        - write N bytes to disk
    // - Third, notify the metaserver of the status (0 to mean
    // success, -1 on failure). 
    // During replication, the chunk isn't part of the chunkTable data
    // structure that is maintained locally.  This is done for
    // simplifying failure handling: if we die in the midst of
    // replication, upon restart, we will find a "zombie" chunk---a
    // chunk with nothing pointing to it; this chunk will get nuked.
    // So, at the end of a succesful replication, we update the
    // chunkTable data structure and the subsequent checkpoint will
    // get the chunk info. logged.  Note that, when we do the writes
    // to disk, we are logging the writes; we are, however,
    // intentionally not logging the presence of the chunk until the
    // replication is complete.
    //
    Replicator(ReplicateChunkOp *op);
    ~Replicator();
    // Connect to peer
    bool Connect();
    // Start by sending out a size request
    void Start();
    // Handle the callback for a size request
    int HandleStart(int code, void *data);
    // Handle the callback for a remote read request
    int HandleRead(int code, void *data);
    // Handle the callback for a disk write request
    int HandleWrite(int code, void *data);
    // Cleanup...
    void Terminate();

private:

    // Inputs from the metaserver
    kfsFileId_t mFileId;
    kfsChunkId_t mChunkId;
    kfsSeq_t mChunkVersion;
    // What we obtain from the src from where we download the chunk.
    size_t mChunkSize;
    // The op that triggered this replication operation.
    ReplicateChunkOp *mOwner;
    // For efficiency, we keep a pair of ops---the write for doing the
    // writes to disk and the read for doing the reads from the remote
    // chunk server.
    WriteOp mWriteOp;
    ReadOp mReadOp;
    // Are we done yet?
    bool mDone;
    // Seq # that we increment when we send out read requests to the peer
    kfsSeq_t mSeq;
    // What is the offset we are currently reading at
    off_t mOffset;
    // A handle to the net connection to the peer.
    NetConnectionPtr mNetConnection;

    // Do the work for setting up the read such as obtaining the
    // chunk's size.
    int ReadSetup(IOBuffer *iobuf, int msgLen);

    // We got a reply.  Is it good?
    bool IsValidReadResponse(IOBuffer *iobuf, int msgLen, size_t &numBytes);
    // Send out a read request to the peer
    void Read();
    // Send out a write request to disk.
    void Write(IOBuffer *iobuf, int numBytes);
    kfsSeq_t NextSeq() { return mSeq++; }

};

typedef boost::shared_ptr<Replicator> ReplicatorPtr;

#endif // CHUNKSERVER_REPLICATOR_H
