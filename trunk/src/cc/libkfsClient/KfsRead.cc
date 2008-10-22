//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/10/02
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
// All the code to deal with read.
//----------------------------------------------------------------------------

#include "KfsClientInt.h"

#include "common/config.h"
#include "common/properties.h"
#include "common/log.h"
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

using namespace KFS;

static double ComputeTimeDiff(const struct timeval &startTime, const struct timeval &endTime)
{
    float timeSpent;

    timeSpent = (endTime.tv_sec * 1e6 + endTime.tv_usec) - 
        (startTime.tv_sec * 1e6 + startTime.tv_usec);
    return timeSpent / 1e6;
}

static bool
NeedToRetryRead(int status)
{
    return ((status == -KFS::EBADVERS) ||
            (status == -KFS::EBADCKSUM) ||
            (status == -KFS::ESERVERBUSY) ||
            (status == -EHOSTUNREACH) ||
            (status == -EINVAL) ||
            (status == -EIO) ||
            (status == -EAGAIN) ||
            (status == -ETIMEDOUT));
}

static bool
NeedToChangeReplica(int errcode)
{
    return ((errcode == -EHOSTUNREACH) || (errcode == -ETIMEDOUT) || (errcode == -EIO));
}

ssize_t
KfsClientImpl::Read(int fd, char *buf, size_t numBytes)
{
    MutexLock l(&mMutex);

    size_t nread = 0, nleft;
    ssize_t numIO = 0;

    if (!valid_fd(fd) || mFileTable[fd] == NULL || mFileTable[fd]->openMode == O_WRONLY) {
        KFS_LOG_VA_INFO("Read to fd: %d failed---fd is likely closed", fd);        
	return -EBADF;
    }

    FilePosition *pos = FdPos(fd);
    FileAttr *fa = FdAttr(fd);
    if (fa->isDirectory)
	return -EISDIR;

    // flush buffer so sizes are updated properly
    ChunkBuffer *cb = FdBuffer(fd);
    if (cb->dirty)
	FlushBuffer(fd);

    cb->allocate();
    
    // Loop thru chunk after chunk until we either get the desired #
    // of bytes or we hit EOF.
    while (nread < numBytes) {
        //
        // Basic invariant: when we enter this loop, the connections
        // we have to the chunkservers (if any) are correct.  As we
        // read thru a file, we call seek whenever we have data to
        // hand out to the client.  As we cross chunk boundaries, seek
        // will invalidate our current set of connections and force us
        // to get new ones via a call to OpenChunk().   This same
        // principle holds for write code path as well.
        //
	if (!IsChunkReadable(fd))
	    break;

	if (pos->fileOffset >= (off_t) fa->fileSize) {
	    KFS_LOG_VA_DEBUG("Current pointer (%lld) is past EOF (%lld) ...so, done",
	                     pos->fileOffset, fa->fileSize);
	    break;
	}

	nleft = numBytes - nread;
	numIO = ReadChunk(fd, buf + nread, nleft);
	if (numIO < 0)
	    break;

	nread += numIO;
	Seek(fd, numIO, SEEK_CUR);
    }

    if ((pos->fileOffset < (off_t) fa->fileSize) && (nread < numBytes)) {
        FilePosition *pos = FdPos(fd);
        string s;

        if ((pos != NULL) && (pos->preferredServer != NULL)) {
            s = pos->GetPreferredServerLocation().ToString();
        }
        
        KFS_LOG_VA_INFO("Read done from %s on %s: @offset: %lld: asked: %d, returning %d, errorcode = %d",
                        s.c_str(), mFileTable[fd]->pathname.c_str(), pos->fileOffset, numBytes, (int) nread, (int) numIO);
    }
    return nread;
}

bool
KfsClientImpl::IsChunkReadable(int fd)
{
    FilePosition *pos = FdPos(fd);
    int res = -1;
    ChunkAttr *chunk = NULL;

    for (int retryCount = 0; retryCount < NUM_RETRIES_PER_OP; retryCount++) {
        res = LocateChunk(fd, pos->chunkNum);

        if (res >= 0) {
            chunk = GetCurrChunk(fd);
            if (pos->preferredServer == NULL && chunk->chunkId != (kfsChunkId_t)-1) {
                // use nonblocking connect to chunkserver; if one fails to
                // connect, we switch to another replica. 
                res = OpenChunk(fd, true);
                if (res < 0) {
                    if (pos->preferredServer != NULL)
                        pos->AvoidServer(pos->preferredServerLocation);
                    continue;
                }
            }
            break;
        }
        if (res == -EAGAIN) {
            // could be that all 3 servers are temporarily down
            Sleep(RETRY_DELAY_SECS);
            continue;
        } else {
            // we can't locate the chunk...fail
            return false;
        }

    }

    if (res < 0)
        return false;


    return IsChunkLeaseGood(chunk->chunkId);
}

bool
KfsClientImpl::IsChunkLeaseGood(kfsChunkId_t chunkId)
{
    if (chunkId > 0) {
	if ((!mLeaseClerk.IsLeaseValid(chunkId)) &&
	    (GetLease(chunkId) < 0)) {
	    // couldn't get a valid lease
	    return false;
	}
	if (mLeaseClerk.ShouldRenewLease(chunkId)) {
	    RenewLease(chunkId);
	}
    }
    return true;
}

ssize_t
KfsClientImpl::ReadChunk(int fd, char *buf, size_t numBytes)
{
    ssize_t numIO;
    ChunkAttr *chunk;
    FilePosition *pos = FdPos(fd);
    int retryCount = 0;

    assert(valid_fd(fd));
    assert(pos->fileOffset < (off_t) mFileTable[fd]->fattr.fileSize);

    numIO = CopyFromChunkBuf(fd, buf, numBytes);
    if (numIO > 0)
	return numIO;

    chunk = GetCurrChunk(fd);

    while (retryCount < NUM_RETRIES_PER_OP) {
	if (pos->preferredServer == NULL) {
            int status;

            // we come into this function with a connection to some
            // chunkserver; as part of the read, the connection
            // broke.  so, we need to "re-figure" where the chunk is.
            if (chunk->chunkId < 0) {
                status = LocateChunk(fd, pos->chunkNum);
                if (status < 0) {
                    retryCount++;
                    Sleep(RETRY_DELAY_SECS);
                    continue;
                }
            }
            // we know where the chunk is....
            assert(chunk->chunkId != (kfsChunkId_t) -1);
            // we are here because we are handling failover/version #
            // mismatch
	    retryCount++;
	    Sleep(RETRY_DELAY_SECS);

	    status = OpenChunk(fd, true);
            if (NeedToChangeReplica(status)) {
                // we couldn't read the data off the disk from the server;
                // when we retry, we need to pick another replica
                
                if (pos->preferredServer != NULL) {
                    string s = pos->GetPreferredServerLocation().ToString();
                    KFS_LOG_VA_INFO("Got error=%d from server %s for %s @offset: %lld; avoiding server",
                                numIO, s.c_str(), mFileTable[fd]->pathname.c_str(), pos->fileOffset);
                }
                chunk->AvoidServer(pos->preferredServerLocation);
                pos->AvoidServer(pos->preferredServerLocation);

                continue;
            }

	    if (status < 0) {
                // open failed..so, bail
	        return status;
            }
	}

	numIO = ZeroFillBuf(fd, buf, numBytes);
	if (numIO > 0)
	    return numIO;

	if (numBytes < ChunkBuffer::BUF_SIZE) {
	    // small reads...so buffer the data
	    ChunkBuffer *cb = FdBuffer(fd);
	    numIO = ReadFromServer(fd, cb->buf, ChunkBuffer::BUF_SIZE);
	    if (numIO > 0) {
	        cb->chunkno = pos->chunkNum;
	        cb->start = pos->chunkOffset;
	        cb->length = numIO;
	        numIO = CopyFromChunkBuf(fd, buf, numBytes);
	    }
	} else {
	    // big read...forget buffering
	    numIO = ReadFromServer(fd, buf, numBytes);
	}

        if ((numIO >= 0) || (!NeedToRetryRead(numIO))) {
            // either we got data or it is an error which doesn't
            // require a retry of the read.
            break;
        }

        if (NeedToChangeReplica(numIO)) {
            // we couldn't read the data off the disk from the server;
            // when we retry, we need to pick another replica

            if (pos->preferredServer != NULL) {
                string s = pos->GetPreferredServerLocation().ToString();
                KFS_LOG_VA_INFO("Got error=%d from server %s for %s @offset: %lld; avoiding server",
                                numIO, s.c_str(), mFileTable[fd]->pathname.c_str(), pos->fileOffset);
            }
            chunk->AvoidServer(pos->preferredServerLocation);
            pos->AvoidServer(pos->preferredServerLocation);

            continue;
        }

        // KFS_LOG_DEBUG("Need to retry read...");
        // Ok...so, we need to retry the read.  so, re-determine where
        // the chunk went and then retry.
        chunk->chunkId = -1;
        pos->ResetServers();
    }
    return numIO;
}

ssize_t
KfsClientImpl::ReadFromServer(int fd, char *buf, size_t numBytes)
{
    size_t numAvail;
    ChunkAttr *chunk = GetCurrChunk(fd);
    FilePosition *pos = FdPos(fd);
    int res;

    assert(chunk->chunkSize - pos->chunkOffset >= 0);

    numAvail = min((size_t) (chunk->chunkSize - pos->chunkOffset),
                   numBytes);

    // Align the reads to checksum block boundaries, so that checksum
    // verification on the server can be done efficiently: if the read falls
    // within a checksum block, issue it as one read; otherwise, split
    // the read into multiple reads.
    if (pos->chunkOffset + numAvail <=
	OffsetToChecksumBlockEnd(pos->chunkOffset))
	res = DoSmallReadFromServer(fd, buf, numBytes);
    else
	res = DoLargeReadFromServer(fd, buf, numBytes);


    return res;
}


//
// Issue a single read op to the server and get data back.
//
ssize_t
KfsClientImpl::DoSmallReadFromServer(int fd, char *buf, size_t numBytes)
{
    ChunkAttr *chunk = GetCurrChunk(fd);

    ReadOp op(nextSeq(), chunk->chunkId, chunk->chunkVersion);
    op.offset = mFileTable[fd]->currPos.chunkOffset;

    op.numBytes = min(chunk->chunkSize, (off_t) numBytes);
    op.AttachContentBuf(buf, numBytes);

    // make sure we aren't overflowing...
    assert(buf + op.numBytes <= buf + numBytes);

    (void)DoOpCommon(&op, mFileTable[fd]->currPos.preferredServer);
    ssize_t numIO = (op.status >= 0) ? op.contentLength : op.status;
    op.ReleaseContentBuf();

    return numIO;
}

size_t
KfsClientImpl::ZeroFillBuf(int fd, char *buf, size_t numBytes)
{
    size_t numIO, bytesInFile, bytesInChunk;
    ChunkAttr *chunk = GetCurrChunk(fd);

    if (mFileTable[fd]->currPos.chunkOffset < (off_t) chunk->chunkSize)
	return 0;		// more data in chunk


    // We've hit End-of-chunk.  There are two cases here:
    // 1. There is more data in the file and that data is in
    // the next chunk
    // 2. This chunk was filled with less data than what was
    // "promised".  (Maybe, write got lost).
    // In either case, zero-fill: the amount to zero-fill is
    // in the min. of the two.
    //
    // Also, we can hit the end-of-chunk if we fail to locate a
    // chunk.  This can happen if there is a hole in the file.
    //

    assert(mFileTable[fd]->currPos.fileOffset <=
           (off_t) mFileTable[fd]->fattr.fileSize);

    bytesInFile = mFileTable[fd]->fattr.fileSize -
        mFileTable[fd]->currPos.fileOffset;

    assert(chunk->chunkSize <= (off_t) KFS::CHUNKSIZE);

    bytesInChunk = KFS::CHUNKSIZE - chunk->chunkSize;
    numIO = min(bytesInChunk, bytesInFile);
    // Fill in 0's based on space in the buffer....
    numIO = min(numIO, numBytes);

    // KFS_LOG_DEBUG("Zero-filling %d bytes for read @ %lld", numIO, mFileTable[fd]->currPos.chunkOffset);

    memset(buf, 0, numIO);
    return numIO;
}

size_t
KfsClientImpl::CopyFromChunkBuf(int fd, char *buf, size_t numBytes)
{
    size_t numIO;
    FilePosition *pos = FdPos(fd);
    ChunkBuffer *cb = FdBuffer(fd);
    size_t start = pos->chunkOffset - cb->start;

    // Wrong chunk in buffer or if the starting point in the buffer is
    // "BEYOND" the current location of the file pointer, we don't
    // have the data.  "BEYOND" => offset is before the starting point
    // or offset is after the end of the buffer
    if ((pos->chunkNum != cb->chunkno) ||
        (pos->chunkOffset < cb->start) ||
        (pos->chunkOffset >= (off_t) (cb->start + cb->length)))
	return 0;

    // first figure out how much data is available in the buffer
    // to be copied out.
    numIO = min(cb->length - start, numBytes);
    // chunkBuf[0] corresponds to some offset in the chunk,
    // which is defined by chunkBufStart.
    // chunkOffset corresponds to the position in the chunk
    // where the "filepointer" is currently at.
    // Figure out where the data we want copied out starts
    memcpy(buf, &cb->buf[start], numIO);

    // KFS_LOG_DEBUG("Copying out data from chunk buf...%d bytes", numIO);

    return numIO;
}

ssize_t
KfsClientImpl::DoLargeReadFromServer(int fd, char *buf, size_t numBytes)
{
    FilePosition *pos = FdPos(fd);
    ChunkAttr *chunk = GetCurrChunk(fd);
    vector<ReadOp *> ops;

    assert(chunk->chunkSize - pos->chunkOffset >= 0);

    size_t numAvail = min((size_t) (chunk->chunkSize - pos->chunkOffset), numBytes);
    size_t numRead = 0;

    while (numRead < numAvail) {
	ReadOp *op = new ReadOp(nextSeq(), chunk->chunkId, chunk->chunkVersion);

	op->numBytes = min(MAX_BYTES_PER_READ_IO, numAvail - numRead);
        // op->numBytes = min(KFS::CHUNKSIZE, numAvail - numRead);
	assert(op->numBytes > 0);

	op->offset = pos->chunkOffset + numRead;

	// if the read is going to straddle checksum block boundaries,
	// break up the read into multiple reads: this simplifies
	// server side code.  for each read request, a single checksum
	// block will need to be read and after the checksum verifies,
	// the server can "trim" the data that wasn't asked for.
	if (OffsetToChecksumBlockStart(op->offset) != op->offset) {
	    op->numBytes = OffsetToChecksumBlockEnd(op->offset) - op->offset;
	}

	op->AttachContentBuf(buf + numRead, op->numBytes);
	numRead += op->numBytes;

	ops.push_back(op);
    }
    // make sure we aren't overflowing...
    assert(buf + numRead <= buf + numBytes);

    struct timeval readStart, readEnd;

    gettimeofday(&readStart, NULL);
    {
        ostringstream os;

        os << pos->GetPreferredServerLocation().ToString().c_str() << ':' 
           << " c=" << chunk->chunkId << " o=" << pos->chunkOffset << " n=" << numBytes;
        KFS_LOG_VA_DEBUG("Reading from %s", os.str().c_str());
    }

    ssize_t numIO = DoPipelinedRead(ops, pos->preferredServer);
    /*
    if (numIO < 0) {
	KFS_LOG_DEBUG("Pipelined read from server failed...");
    }
    */

    int retryStatus = 0;

    for (vector<KfsOp *>::size_type i = 0; i < ops.size(); ++i) {
	ReadOp *op = static_cast<ReadOp *> (ops[i]);
	if (op->status < 0) {
            if (NeedToRetryRead(op->status)) {
                // preserve EIO so that we can avoid that server
                if (retryStatus != -EIO)
                    retryStatus = op->status;
            }
	    numIO = op->status;
        }
	else if (numIO >= 0)
	    numIO += op->status;
	op->ReleaseContentBuf();
	delete op;
    }

    // If the op needs to be retried, pass that up
    if (retryStatus != 0)
        numIO = retryStatus;

    gettimeofday(&readEnd, NULL);

    double timeSpent = ComputeTimeDiff(readStart, readEnd);
    {
        ostringstream os;

        os << pos->GetPreferredServerLocation().ToString().c_str() << ':' 
           << " c=" << chunk->chunkId << " o=" << pos->chunkOffset << " n=" << numBytes
           << " got=" << numIO << " time=" << timeSpent;
        
        if (timeSpent > 5.0) {
            struct sockaddr_in saddr;

            KFS_LOG_VA_INFO("Read done from %s", os.str().c_str());

            if (pos->GetPreferredServerAddr(saddr) == 0) {
                KFS_LOG_VA_DEBUG("Sending telemetry report about: %s", os.str().c_str());
                double diskIOTime[MAX_IO_INFO_PER_PKT];
                double elapsedTime[MAX_IO_INFO_PER_PKT];
                vector<KfsOp *>::size_type count = 0;
                for (; (count < ops.size()) && (count < MAX_IO_INFO_PER_PKT); count++) {
                    ReadOp *op = static_cast<ReadOp *> (ops[count]);

                    diskIOTime[count] = op->diskIOTime;
                    elapsedTime[count] = op->elapsedTime;
                }
                mTelemetryReporter.publish(saddr.sin_addr, timeSpent, "READ", 
                                           count, diskIOTime, elapsedTime);
            }
        }
        
    }

    return numIO;
}

///
/// Common work for a read op that can be pipelined.
/// The idea is to plumb the pipe with a set of requests; then,
/// whenever one finishes, submit a new request.
///
/// @param[in] ops the vector of ops to be done
/// @param[in] sock the socket on which we communicate with server
///
/// @retval 0 on success; -1 on failure
///
int
KfsClientImpl::DoPipelinedRead(vector<ReadOp *> &ops, TcpSocket *sock)
{
    vector<ReadOp *>::size_type first = 0, next, minOps;
    int res = 0;
    ReadOp *op;
    bool leaseExpired = false;

    // plumb the pipe with 1MB
    minOps = min((size_t) (MIN_BYTES_PIPELINE_IO / MAX_BYTES_PER_READ_IO), ops.size());
    // plumb the pipe with a few ops
    for (next = 0; next < minOps; ++next) {
        op = ops[next];

        gettimeofday(&op->submitTime, NULL);

	res = DoOpSend(op, sock);
	if (res < 0)
	    return -1;
    }

    // run the pipe: whenever one op finishes, queue another
    while (next < ops.size()) {
        struct timeval now;
	op = ops[first];

	res = DoOpResponse(op, sock);
	if (res < 0)
	    return -1;

        gettimeofday(&now, NULL);

        op->elapsedTime = ComputeTimeDiff(op->submitTime, now);

	++first;

	op = ops[next];

	if (!IsChunkLeaseGood(op->chunkId)) {
	    leaseExpired = true;
	    break;
	}

        gettimeofday(&op->submitTime, NULL);

	res = DoOpSend(op, sock);
	if (res < 0)
	    return -1;
	++next;
    }

    // get the response for the remaining ones
    while (first < next) {
        struct timeval now;
	op = ops[first];

	res = DoOpResponse(op, sock);
	if (res < 0)
	    return -1;

        gettimeofday(&now, NULL);

        op->elapsedTime = ComputeTimeDiff(op->submitTime, now);

	if (leaseExpired)
	    op->status = 0;

	++first;

    }

    // do checksum verification
    if (res >= 0) {
        for (next = 0; next < ops.size(); next++) {
            op = ops[next];
            if (op->checksums.size() == 0)
                continue;

            for (size_t pos = 0; pos < op->contentLength; pos += CHECKSUM_BLOCKSIZE) {
                size_t len = min(CHECKSUM_BLOCKSIZE, (uint32_t) (op->contentLength - pos));
                uint32_t cksum = ComputeBlockChecksum(op->contentBuf + pos, len);
                uint32_t cksumIndex = pos / CHECKSUM_BLOCKSIZE;
                if (op->checksums.size() < cksumIndex) {
                    // didn't get all the checksums
                    KFS_LOG_VA_DEBUG("Didn't get checksum for offset: %lld",
                                     op->offset + pos);
                    continue;
                }

                uint32_t serverCksum = op->checksums[cksumIndex];
                if (serverCksum != cksum) {
                    struct sockaddr_in saddr;
                    char ipname[INET_ADDRSTRLEN];

                    sock->GetPeerName((struct sockaddr *) &saddr);
                    inet_ntop(AF_INET, &(saddr.sin_addr), ipname, INET_ADDRSTRLEN);

                    KFS_LOG_VA_INFO("Checksum mismatch from %s starting @pos = %lld: got = %d, computed = %d for %s",
                                    ipname, op->offset + pos, serverCksum, cksum, op->Show().c_str());
                    op->status = -KFS::EBADCKSUM;
                    mTelemetryReporter.publish(saddr.sin_addr, -1.0, "CHECKSUM_MISMATCH");
                }
            }
        }
    }
    return 0;
}
