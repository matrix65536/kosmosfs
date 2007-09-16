//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/10/02
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
// All the code to deal with writes from the client.
//----------------------------------------------------------------------------

#include "KfsClient.h"
#include "common/properties.h"
#include "common/log.h"
#include "meta/kfstypes.h"
#include "libkfsIO/Checksum.h"
#include "Utils.h"

#include <cerrno>
#include <iostream>
#include <string>

using std::string;
using std::ostringstream;
using std::istringstream;
using std::min;
using std::max;

using std::cout;
using std::endl;

using namespace KFS;

static bool
NeedToRetryWrite(int status)
{
    return ((status == -EHOSTUNREACH) ||
	    (status == -ETIMEDOUT) ||
	    (status == -KFS::ESERVERBUSY));
}

static bool
NeedToRetryAllocation(int status)
{
    return ((status == -EHOSTUNREACH) ||
	    (status == -ETIMEDOUT) ||
	    (status == -EBUSY) ||
	    (status == -KFS::EALLOCFAILED));
}

ssize_t
KfsClient::Write(int fd, const char *buf, size_t numBytes)
{
    MutexLock l(&mMutex);

    size_t nwrote = 0;
    ssize_t numIO = 0;
    int res;

    if (!valid_fd(fd) || mFileTable[fd]->openMode == O_RDONLY)
	return -EBADF;

    FileAttr *fa = FdAttr(fd);
    if (fa->isDirectory)
	return -EISDIR;

    FilePosition *pos = FdPos(fd);
    //
    // Loop thru chunk after chunk until we write the desired #
    // of bytes.
    while (nwrote < numBytes) {

	size_t nleft = numBytes - nwrote;

	LocateChunk(fd, pos->chunkNum);

	// need to retry here...
	if ((res = DoAllocation(fd)) < 0) {
	    // allocation failed...bail
	    break;
	}

	if (pos->preferredServer == NULL) {
	    numIO = OpenChunk(fd);
	    if (numIO < 0) {
		COSMIX_LOG_DEBUG("OpenChunk(%ld)", numIO);
		break;
	    }
	}

	if (nleft < ChunkBuffer::BUF_SIZE || FdBuffer(fd)->dirty) {
	    // either the write is small or there is some dirty
	    // data...so, aggregate as much as possible and then it'll
	    // get flushed
	    numIO = WriteToBuffer(fd, buf + nwrote, nleft);
	} else {
	    // write is big and there is nothing dirty...so,
	    // write-thru
	    numIO = WriteToServer(fd, pos->chunkOffset, buf + nwrote, nleft);
	}

	if (numIO < 0) {
	    string errstr = ErrorCodeToStr(numIO);
	    COSMIX_LOG_DEBUG("WriteToXXX:%s", errstr.c_str());
	    break;
	}

	nwrote += numIO;
	numIO = Seek(fd, numIO, SEEK_CUR);
	if (numIO < 0) {
	    COSMIX_LOG_DEBUG("Seek(%ld)", numIO);
	    break;
	}
    }

    if (nwrote == 0 && numIO < 0)
	return numIO;

    if (nwrote != numBytes) {
	COSMIX_LOG_DEBUG("----Write done: asked: %lu, got: %lu-----",
			  numBytes, nwrote);
    }
    return nwrote;
}

ssize_t
KfsClient::WriteToBuffer(int fd, const char *buf, size_t numBytes)
{
    ssize_t numIO;
    size_t lastByte;
    FilePosition *pos = FdPos(fd);
    ChunkBuffer *cb = FdBuffer(fd);

    // if the buffer has dirty data and this write doesn't abut it,
    // or if buffer is full, flush the buffer before proceeding.
    // XXX: Reconsider buffering to do a read-modify-write of
    // large amounts of data.
    if (cb->dirty && cb->chunkno != pos->chunkNum) {
	int status = FlushBuffer(fd);
	if (status < 0)
	    return status;
    }

    off_t start = pos->chunkOffset - cb->start;
    size_t previous = cb->length;
    if (cb->dirty && ((start != (off_t) previous) ||
	              (previous == ChunkBuffer::BUF_SIZE))) {
	int status = FlushBuffer(fd);
	if (status < 0)
	    return status;
    }

    if (!cb->dirty) {
	cb->chunkno = pos->chunkNum;
	cb->start = pos->chunkOffset;
	cb->length = 0;
	cb->dirty = true;
    }

    // ensure that write doesn't straddle chunk boundaries
    numBytes = min(numBytes, KFS::CHUNKSIZE - pos->chunkOffset);
    if (numBytes == 0)
	return 0;

    // max I/O we can do
    numIO = min(ChunkBuffer::BUF_SIZE - cb->length, numBytes);
    assert(numIO > 0);

    // COSMIX_LOG_DEBUG("Buffer absorbs write...%d bytes", numIO);

    // chunkBuf[0] corresponds to some offset in the chunk,
    // which is defined by chunkBufStart.
    // chunkOffset corresponds to the position in the chunk
    // where the "filepointer" is currently at.
    // Figure out where the data we want copied into starts
    start = pos->chunkOffset - cb->start;
    assert(start >= 0 && start < (off_t) ChunkBuffer::BUF_SIZE);
    memcpy(&cb->buf[start], buf, numIO);

    lastByte = start + numIO;

    // update the size according to where the last byte just
    // got written.
    if (lastByte > cb->length)
	cb->length = lastByte;

    return numIO;
}

ssize_t
KfsClient::FlushBuffer(int fd)
{
    ssize_t numIO = 0;
    ChunkBuffer *cb = FdBuffer(fd);

    if (cb->dirty) {
	numIO = WriteToServer(fd, cb->start, cb->buf, cb->length);
	if (numIO >= 0)
	    cb->dirty = false;
    }
    return numIO;
}

ssize_t
KfsClient::WriteToServer(int fd, off_t offset, const char *buf, size_t numBytes)
{
    assert(KFS::CHUNKSIZE - offset >= 0);

    size_t numAvail = min(numBytes, KFS::CHUNKSIZE - offset);
    int res = 0;

    for (int retryCount = 0; retryCount < NUM_RETRIES_PER_OP; retryCount++) {
	// Same as read: align writes to checksum block boundaries
	if (offset + numAvail <= OffsetToChecksumBlockEnd(offset))
	    res = DoSmallWriteToServer(fd, offset, buf, numBytes);
	else
	    res = DoLargeWriteToServer(fd, offset, buf, numBytes);

	if (res >= 0)
	    break;

	if ((res == -EHOSTUNREACH) ||
	    (res == -KFS::EBADVERS)) {
	    // one of the hosts is non-reachable (aka dead); so, wait
	    // and retry.  Since one of the servers has a write-lease, we
	    // need to wait for the lease to expire before retrying.
	    Sleep(KFS::LEASE_INTERVAL_SECS);
	}

	if (res == -KFS::ELEASEEXPIRED) {
	    COSMIX_LOG_DEBUG("Server says lease expired...re-doing allocation");
	    Sleep(KFS::LEASE_INTERVAL_SECS / 2);
	}
	if ((res == -EHOSTUNREACH) ||
	    (res == -KFS::EBADVERS) ||
	    (res == -KFS::ELEASEEXPIRED)) {
	    if (DoAllocation(fd, true) < 0)
		return res;
	    continue;
	}

	if (res < 0)
	    // any other error
	    break;
    }

    return res;
}

int
KfsClient::DoAllocation(int fd, bool force)
{
    ChunkAttr *chunk = NULL;
    FileAttr *fa = FdAttr(fd);
    int res = 0;

    if (IsCurrChunkAttrKnown(fd))
	chunk = GetCurrChunk(fd);

    if (chunk && force)
	chunk->didAllocation = false;

    if ((chunk == NULL) || (chunk->chunkId == (kfsChunkId_t) -1) ||
	(!chunk->didAllocation)) {
	// if we couldn't locate the chunk, it must be a new one.
	// also, if it is an existing chunk, force an allocation
	// if needed (which'll cause version # bumps, lease
	// handouts etc).
	for (uint8_t retryCount = 0; retryCount < NUM_RETRIES_PER_OP; retryCount++) {
	    if (retryCount) {
		COSMIX_LOG_DEBUG("Allocation failed...will retry after a few secs");
		if (res == -EBUSY)
		    // the metaserver says it can't get us a lease for
		    // the chunk.  so, wait for a lease interval to
		    // expire and then retry
		    Sleep(KFS::LEASE_INTERVAL_SECS);
		else
		    Sleep(RETRY_DELAY_SECS);
	    }
	    res = AllocChunk(fd);
	    if ((res >= 0) || (!NeedToRetryAllocation(res))) {
		break;
	    }
	    // allocation failed...retry
	}
	if (res < 0)
	    // allocation failed...bail
	    return res;
	chunk = GetCurrChunk(fd);
	assert(chunk != NULL);
	chunk->didAllocation = true;
	if (force) {
	    COSMIX_LOG_DEBUG("Forced allocation version: %ld",
                             chunk->chunkVersion);
	}
	// XXX: This is incorrect...you may double-count for
	// allocations that occurred due to lease expirations.
	++fa->chunkCount;
    }
    return 0;

}

ssize_t
KfsClient::DoSmallWriteToServer(int fd, off_t offset, const char *buf, size_t numBytes)
{
    ssize_t numIO;
    FilePosition *pos = FdPos(fd);
    ChunkAttr *chunk = GetCurrChunk(fd);
    vector<WriteInfo> w;
    WritePrepareOp op(nextSeq(), chunk->chunkId, chunk->chunkVersion);
    // get the socket for the master
    ServerLocation loc = chunk->chunkServerLoc[0];
    TcpSocket *masterSock = pos->GetChunkServerSocket(loc);

    op.offset = offset;
    op.numBytes = min(numBytes, KFS::CHUNKSIZE);
    op.AttachContentBuf(buf, numBytes);
    op.contentLength = op.numBytes;

    w.resize(chunk->chunkServerLoc.size());

    for (uint8_t retryCount = 0; retryCount < NUM_RETRIES_PER_OP; retryCount++) {
	if (retryCount) {
	    COSMIX_LOG_DEBUG("Will retry write after %d secs",
	                     RETRY_DELAY_SECS);
	    Sleep(RETRY_DELAY_SECS);
	    op.seq = nextSeq();
	}

	for (uint32_t i = 0; i < chunk->chunkServerLoc.size(); i++) {
	    ServerLocation l = chunk->chunkServerLoc[i];
	    TcpSocket *s = pos->GetChunkServerSocket(l);

	    assert(op.contentLength == op.numBytes);

	    numIO = DoOpCommon(&op, s);

	    w[i].serverLoc = l;

	    if (op.status == 0) {
		w[i].writeId = op.writeId;
		continue;
	    }

	    if (NeedToRetryWrite(op.status)) {
		break;
	    }

	    w[i].writeId = -1;
	}

	if (NeedToRetryWrite(op.status)) {
	    // do a retry
	    continue;
	}

	WriteSyncOp cop(nextSeq(), chunk->chunkId, chunk->chunkVersion, w);

	numIO = DoOpCommon(&cop, masterSock);
	op.status = cop.status;

	if (!NeedToRetryWrite(op.status)) {
	    break;
	} // else ...retry
    }

    if (op.status >= 0 && (off_t)chunk->chunkSize < offset + op.status) {
	// grow the chunksize only if we wrote past the last byte in the chunk
	chunk->chunkSize = offset + op.status;

	// if we wrote past the last byte of the file, then grow the
	// file size.  Note that, chunks 0..chunkNum-1 are assumed to
	// be full.  So, take the size of the last chunk and to that
	// add the size of the "full" chunks to get the size
	FileAttr *fa = FdAttr(fd);
	ssize_t eow = chunk->chunkSize + (pos->chunkNum  * KFS::CHUNKSIZE);
	fa->fileSize = max(fa->fileSize, eow);
    }

    numIO = op.status;
    op.ReleaseContentBuf();

    if (numIO >= 0) {
	COSMIX_LOG_DEBUG("Wrote to server (fd = %d), %ld bytes",
	                 fd, numIO);
    }
    return numIO;
}

ssize_t
KfsClient::DoLargeWriteToServer(int fd, off_t offset, const char *buf, size_t numBytes)
{
    size_t numAvail, numWrote = 0;
    ssize_t numIO;
    FilePosition *pos = FdPos(fd);
    ChunkAttr *chunk = GetCurrChunk(fd);
    vector<WritePrepareOp *> ops;

    assert(KFS::CHUNKSIZE - offset >= 0);

    numAvail = min(numBytes, KFS::CHUNKSIZE - offset);

    // cout << "Pushing to server: " << offset << ' ' << numBytes << endl;

    // Split the write into a bunch of smaller ops
    while (numWrote < numAvail) {
	WritePrepareOp *op = new WritePrepareOp(nextSeq(), chunk->chunkId,
	                                        chunk->chunkVersion);

	// Try to push out 1MB at a time: is too slow
	// op->numBytes = min(MAX_BYTES_PER_WRITE, numAvail - numWrote);

	op->numBytes = min(KFS::CHUNKSIZE, numAvail - numWrote);

	if ((op->numBytes % CHECKSUM_BLOCKSIZE) != 0) {
	    // if the write isn't aligned to end on a checksum block
	    // boundary, then break the write into two parts:
	    //(1) start at offset and end on a checksum block boundary
	    //(2) is the rest, which is less than the size of checksum
	    //block
	    // This simplifies chunkserver code: either the writes are
	    // multiples of checksum blocks or a single write which is
	    // smaller than a checksum block.
	    if (op->numBytes > CHECKSUM_BLOCKSIZE)
		op->numBytes = (op->numBytes / CHECKSUM_BLOCKSIZE) * CHECKSUM_BLOCKSIZE;
	    // else case #2 from above comment and op->numBytes is setup right
	}

	assert(op->numBytes > 0);

	op->offset = offset + numWrote;

	// similar to read, breakup the write if it is straddling
	// checksum block boundaries.
	if (OffsetToChecksumBlockStart(op->offset) != op->offset) {
	    op->numBytes = min(OffsetToChecksumBlockEnd(op->offset) - op->offset,
	                       (long) op->numBytes);
	}

	op->AttachContentBuf(buf + numWrote, op->numBytes);
	op->contentLength = op->numBytes;

	numWrote += op->numBytes;
	ops.push_back(op);
    }

    // basics here:
    // 1. fill the pipe:  push an op to multiple servers, in sequence
    // 2. issue commits for all ops in the pipe;
    // 3. whenever we hear an op is commited, push the next op and commit; loop
    // 4. wait for all ops to commit

    for (int retryCount = 0; retryCount < NUM_RETRIES_PER_OP; retryCount++) {
	if (retryCount != 0) {
	    COSMIX_LOG_DEBUG("Will retry write after %d secs",
	                     RETRY_DELAY_SECS);
	    Sleep(RETRY_DELAY_SECS);

	    COSMIX_LOG_DEBUG("Starting retry sequence...");

	    // for each op bump the sequence #
	    for (vector<WritePrepareOp *>::size_type i = 0; i < ops.size(); i++) {
		ops[i]->seq = nextSeq();
		ops[i]->status = 0;
		assert(ops[i]->contentLength == ops[i]->numBytes);
	    }
	}
	numIO = DoPipelinedWrite(fd, ops);

	assert(numIO != -KFS::EBADVERS);

	if ((numIO == 0) || (numIO == -KFS::ELEASEEXPIRED)) {
	    // all good or the server lease expired; so, we have to
	    // redo the allocation and such
	    break;
	}
	if ((numIO == -EHOSTUNREACH) || (numIO == -ETIMEDOUT)) {
	    // retry
	    continue;
	}
	if (numIO < 0) {
	    COSMIX_LOG_DEBUG("Write failed...chunk = %ld, version = %ld, offset = %ld, bytes = %ld",
	                     ops[0]->chunkId, ops[0]->chunkVersion, ops[0]->offset,
	                     ops[0]->numBytes);
	    assert(numIO != -EBADF);
	    break;
	}
    }

    // figure out how much was committed
    numIO = 0;
    for (vector<KfsOp *>::size_type i = 0; i < ops.size(); ++i) {
	WritePrepareOp *op = static_cast<WritePrepareOp *> (ops[i]);
	if (op->status < 0)
	    numIO = op->status;
	else if (numIO >= 0)
	    numIO += op->status;
	op->ReleaseContentBuf();
	delete op;
    }

    if (numIO >= 0 && (off_t)chunk->chunkSize < offset + numIO) {
	// grow the chunksize only if we wrote past the last byte in the chunk
	chunk->chunkSize = offset + numIO;

	// if we wrote past the last byte of the file, then grow the
	// file size.  Note that, chunks 0..chunkNum-1 are assumed to
	// be full.  So, take the size of the last chunk and to that
	// add the size of the "full" chunks to get the size
	FileAttr *fa = FdAttr(fd);
	ssize_t eow = chunk->chunkSize + (pos->chunkNum  * KFS::CHUNKSIZE);
	fa->fileSize = max(fa->fileSize, eow);
    }

    if (numIO != (ssize_t) numBytes) {
	COSMIX_LOG_DEBUG("Wrote to server (fd = %d), %ld bytes, was asked %lu bytes",
	                 fd, numIO, numBytes);
    }

    COSMIX_LOG_DEBUG("Wrote to server (fd = %d), %ld bytes",
                     fd, numIO);

    return numIO;
}

int
KfsClient::DoPipelinedWrite(int fd, vector<WritePrepareOp *> &ops)
{
    vector<WriteSyncOp *> syncOps;
    vector<WritePrepareOp *>::size_type first = 0, next = 0;
    WritePrepareOp *op;
    int res;
    ChunkAttr *chunk = GetCurrChunk(fd);
    // get the socket for the master
    ServerLocation loc = chunk->chunkServerLoc[0];
    TcpSocket *masterSock = FdPos(fd)->GetChunkServerSocket(loc);
    WriteSyncOp *sop = NULL;

    // plumb the pipe...
    op = ops[next];
    res = PushDataForWrite(fd, op);
    if ((res < 0) || (op->status < 0)) {
	res = op->status;
	goto out;
    }

    ++next;
    op = ops[first];
    // send out a commit
    res = IssueWriteCommit(fd, op, &sop, masterSock);
    if (res < 0)
	goto out;

    // we could have a single "large" write.
    if (next < ops.size()) {
	// while commit is on, push out more data
	op = ops[next];
	res = PushDataForWrite(fd, op);
	if ((res < 0) || (op->status < 0)) {
	    res = op->status;
	    goto out;
	}

	++next;
    }

    // run the pipe: whenever one op finishes, queue another
    while (next < ops.size()) {
	// what we have so far: 2 data pushes and 1 commit.  so, when
	// the commit finishes, issue another commit and do the next
	// push, and thereby keep 2 data push and 1 commit outstanding.

	// get the commit response
	res = DoOpResponse(sop, masterSock);
	if (res < 0) {
	    goto out;
	}
	op = ops[first];
	op->status = sop->status;
	if (op->status < 0) {
	    res = op->status;
	    goto out;
	}

	++first;
	delete sop;
	sop = NULL;

	op = ops[first];
	// send out a commit
	res = IssueWriteCommit(fd, op, &sop, masterSock);
	if (res < 0)
	    goto out;

	op = ops[next];
	res = PushDataForWrite(fd, op);
	if ((res < 0) || (op->status < 0)) {
	    res = op->status;
	    goto out;
	}
	++next;
    }

    // get the response for the remaining ones
    while (first < ops.size()) {
	res = DoOpResponse(sop, masterSock);
	if (res < 0)
	    goto out;

	op = ops[first];
	op->status = sop->status;
	if (op->status < 0) {
	    res = op->status;
	    goto out;
	}

	delete sop;
	sop = NULL;
	++first;

	if (first >= ops.size()) {
	    res = 0;
	    break;
	}

	op = ops[first];
	// send out a commit
	res = IssueWriteCommit(fd, op, &sop, masterSock);
	if (res < 0)
	    goto out;

    }

  out:
    delete sop;

    return res;
}

int
KfsClient::PushDataForWrite(int fd, WritePrepareOp *op)
{
    int res;
    ChunkAttr *chunk = GetCurrChunk(fd);
    ServerLocation loc;
    TcpSocket *sock;
    vector<ServerLocation>::size_type i;

    // push the write out to all the servers
    for (i = 0; i < chunk->chunkServerLoc.size(); i++) {
	loc = chunk->chunkServerLoc[i];
	sock = FdPos(fd)->GetChunkServerSocket(loc);
	if (sock == NULL) {
	    op->status = -EHOSTUNREACH;
	    return op->status;
	}

	assert(op->contentLength == op->numBytes);

        COSMIX_LOG_DEBUG("%s", op->Show().c_str());

	res = DoOpSend(op, sock);
	if (res < 0)
	    return res;
    }
    return 0;
}

int
KfsClient::IssueWriteCommit(int fd, WritePrepareOp *op, WriteSyncOp **sop,
	                    TcpSocket *masterSock)
{
    vector<ServerLocation>::size_type i;
    int res;
    ChunkAttr *chunk = GetCurrChunk(fd);
    ServerLocation loc;
    TcpSocket *sock;
    vector<WriteInfo> w;

    w.reserve(chunk->chunkServerLoc.size());

    *sop = NULL;
    for (i = 0; i < chunk->chunkServerLoc.size(); i++) {
	loc = chunk->chunkServerLoc[i];
	sock = FdPos(fd)->GetChunkServerSocket(loc);
	if (sock == NULL) {
	    return -EHOSTUNREACH;
	}

	res = DoOpResponse(op, sock);
	if ((res < 0) || (op->status < 0))
	    return op->status;
	w.push_back(WriteInfo(loc, op->writeId));
    }

    *sop = new WriteSyncOp(nextSeq(), chunk->chunkId, chunk->chunkVersion, w);
    COSMIX_LOG_DEBUG("%s", (*sop)->Show().c_str());

    res = DoOpSend(*sop, masterSock);
    if (res < 0) {
	delete *sop;
	*sop = NULL;
	return res;
    }
    return 0;
}


