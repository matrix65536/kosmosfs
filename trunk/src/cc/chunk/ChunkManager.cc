//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/03/28
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

extern "C" {
#include <dirent.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/statvfs.h>
}

#include "common/log.h"
#include "common/kfstypes.h"

#include "ChunkManager.h"
#include "MetaServerSM.h"
#include "Utils.h"

#include "libkfsIO/Counter.h"
#include "libkfsIO/Checksum.h"
#include "libkfsIO/Globals.h"

#include <fstream>
#include <sstream>
#include <algorithm>
#include <string>

#include <boost/lexical_cast.hpp>

using std::ofstream;
using std::ifstream;
using std::istringstream;
using std::ostringstream;
using std::ios_base;
using std::list;
using std::min;
using std::max;
using std::endl;
using std::find_if;
using std::string;
using std::vector;

using namespace KFS;
using namespace KFS::libkfsio;

// Cleanup fds on which no I/O has been done for the past N secs
const int INACTIVE_FDS_CLEANUP_INTERVAL_SECS = 300;

// The # of fd's that we allow to be open before cleanup kicks in.
// This value will be set to : # of files that the process can open / 2
int OPEN_FDS_LOW_WATERMARK = 0;

ChunkManager::ChunkManager()
{
    mTotalSpace = mUsedSpace = 0;
    mChunkManagerTimeoutImpl = new ChunkManagerTimeoutImpl(this);
    mIsChunkTableDirty = false;
}

ChunkManager::~ChunkManager()
{
    ChunkInfoHandle_t *cih;

    for (CMI iter = mChunkTable.begin(); iter != mChunkTable.end(); ++iter) {
        cih = iter->second;
        delete cih;
    }
    mChunkTable.clear();
    globals().netManager.UnRegisterTimeoutHandler(mChunkManagerTimeoutImpl);
    delete mChunkManagerTimeoutImpl;
}

void 
ChunkManager::Init(const vector<string> &chunkDirs, int64_t totalSpace)
{
    mTotalSpace = totalSpace;
    mChunkDirs = chunkDirs;
}

int
ChunkManager::AllocChunk(kfsFileId_t fileId, kfsChunkId_t chunkId, 
                         int64_t chunkVersion,
                         bool isBeingReplicated)
{
    string chunkPathname;
    int fd;
    ChunkInfoHandle_t *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    mIsChunkTableDirty = true;

    chunkPathname = MakeChunkPathname(chunkId);

    if (tableEntry != mChunkTable.end()) {
        KFS_LOG_VA_DEBUG("Chunk %s already exists; changing version # to %ld",
                         chunkPathname.c_str(), chunkVersion);
        cih = tableEntry->second;
        cih->chunkInfo.chunkVersion = chunkVersion;
        mChunkTable[chunkId] = cih;
        return 0;
    }
    
    KFS_LOG_VA_DEBUG("Creating chunk: %s", chunkPathname.c_str());

    CleanupInactiveFds();

    if ((fd = creat(chunkPathname.c_str(), S_IRUSR | S_IWUSR)) < 0) {
        perror("Create failed: ");
        return -KFS::ESERVERBUSY;
    }
    close(fd);

    cih = new ChunkInfoHandle_t();
    cih->chunkInfo.fileId = fileId;
    cih->chunkInfo.chunkId = chunkId;
    cih->chunkInfo.chunkVersion = chunkVersion;
    cih->isBeingReplicated = isBeingReplicated;
    mChunkTable[chunkId] = cih;
    return 0;
}

int
ChunkManager::DeleteChunk(kfsChunkId_t chunkId)
{
    string chunkPathname;
    ChunkInfoHandle_t *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) 
        return -EBADF;

    mIsChunkTableDirty = true;

    chunkPathname = MakeChunkPathname(chunkId);
    
    unlink(chunkPathname.c_str());

    cih = tableEntry->second;
    mUsedSpace -= cih->chunkInfo.chunkSize;
    mChunkTable.erase(chunkId);
    delete cih;
    return 0;
}

int
ChunkManager::StaleChunk(kfsChunkId_t chunkId)
{
    string chunkPathname, staleChunkPathname;
    ChunkInfoHandle_t *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) 
        return -EBADF;

    mIsChunkTableDirty = true;

    chunkPathname = MakeChunkPathname(chunkId);
    staleChunkPathname = MakeStaleChunkPathname(chunkId);
    
    rename(chunkPathname.c_str(), staleChunkPathname.c_str());

    KFS_LOG_VA_INFO("Moving chunk %ld to staleChunks dir", chunkId);

    cih = tableEntry->second;
    mUsedSpace -= cih->chunkInfo.chunkSize;
    mChunkTable.erase(chunkId);
    delete cih;
    return 0;
}

int
ChunkManager::TruncateChunk(kfsChunkId_t chunkId, off_t chunkSize)
{
    string chunkPathname;
    ChunkInfoHandle_t *cih;
    int res;
    uint32_t lastChecksumBlock;
    CMI tableEntry = mChunkTable.find(chunkId);

    // the truncated size should not exceed chunk size.
    if (chunkSize > KFS::CHUNKSIZE)
        return -EINVAL;

    if (tableEntry == mChunkTable.end())
        return -EBADF;

    mIsChunkTableDirty = true;

    chunkPathname = MakeChunkPathname(chunkId);
    
    res = truncate(chunkPathname.c_str(), chunkSize);
    if (res < 0) {
        res = errno;
        return -res;
    }
    cih = tableEntry->second;
    mUsedSpace -= cih->chunkInfo.chunkSize;
    mUsedSpace += chunkSize;
    cih->chunkInfo.chunkSize = chunkSize;

    lastChecksumBlock = OffsetToChecksumBlockNum(chunkSize);
    cih->chunkInfo.chunkBlockChecksum.resize(lastChecksumBlock + 1);

    // XXX: Could do better; recompute the checksum for this last block
    cih->chunkInfo.chunkBlockChecksum[lastChecksumBlock] = 0;

    return 0;
}

int
ChunkManager::ChangeChunkVers(kfsFileId_t fileId,
                              kfsChunkId_t chunkId, int64_t chunkVersion)
{
    string chunkPathname;
    ChunkInfoHandle_t *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) {
        return -1;
    }

    chunkPathname = MakeChunkPathname(chunkId);

    mIsChunkTableDirty = true;

    KFS_LOG_VA_INFO("Chunk %s already exists; changing version # to %ld",
                     chunkPathname.c_str(), chunkVersion);

    cih = tableEntry->second;
    cih->chunkInfo.chunkVersion = chunkVersion;
    mChunkTable[chunkId] = cih;
    return 0;
}

void
ChunkManager::ReplicationDone(kfsChunkId_t chunkId)
{
    ChunkInfoHandle_t *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) {
        return;
    }

#ifdef DEBUG
    string chunkPathname = MakeChunkPathname(chunkId);
    KFS_LOG_VA_DEBUG("Replication for chunk %s is complete...",
                     chunkPathname.c_str());
#endif

    mIsChunkTableDirty = true;
    cih = tableEntry->second;
    cih->isBeingReplicated = false;
    mChunkTable[chunkId] = cih;
}

void
ChunkManager::Start()
{
    globals().netManager.RegisterTimeoutHandler(mChunkManagerTimeoutImpl);
}

string
ChunkManager::MakeChunkPathname(const char *chunkId)
{
    kfsChunkId_t c = atoll(chunkId);
    return MakeChunkPathname(c);
}

string
ChunkManager::MakeChunkPathname(kfsChunkId_t chunkId)
{
    assert(mChunkDirs.size() > 0);

    ostringstream os;
    uint32_t chunkSubdir = chunkId % mChunkDirs.size();

    os << mChunkDirs[chunkSubdir] << '/' << chunkId;
    return os.str();
}

string
ChunkManager::MakeStaleChunkPathname(kfsChunkId_t chunkId)
{
    ostringstream os;
    uint32_t chunkSubdir = chunkId % mChunkDirs.size();
    string staleChunkDir = GetStaleChunkPath(mChunkDirs[chunkSubdir]);

    os << staleChunkDir << '/' << chunkId;

    return os.str();
}

int
ChunkManager::OpenChunk(kfsChunkId_t chunkId, 
                        int openFlags)
{
    string chunkPathname;
    int fd;
    ChunkInfoHandle_t *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) {
        KFS_LOG_VA_DEBUG("No such chunk: %s", chunkPathname.c_str());
        return -EBADF;
    }
    chunkPathname = MakeChunkPathname(chunkId);

    cih = tableEntry->second;
    if (cih->chunkHandle->mFileId < 0) {
        fd = open(chunkPathname.c_str(), openFlags, S_IRUSR|S_IWUSR);
        if (fd < 0) {
            perror("open: ");
            return -EBADF;
        }
        globals().ctrOpenDiskFds.Update(1);

        cih->chunkHandle->mChunkId = chunkId;
        cih->chunkHandle->mFileId = fd;

    } else {
        fd = cih->chunkHandle->mFileId;
    }

    /*
    KFS_LOG_VA_DEBUG("opening %s with flags %d; fd = %d", 
                     chunkPathname.c_str(), openFlags, fd);
    */

    return 0;
}

void
ChunkManager::CloseChunk(kfsChunkId_t chunkId)
{
    ChunkInfoHandle_t *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) {
        return;
    }

    cih = tableEntry->second;
    // If there are at most 2 references to the handle---a reference
    // from mChunkTable and a reference from cih->chunkHandle, then
    // we can safely close the fileid.
    if (cih->chunkHandle.use_count() <= 2) {
        if (cih->chunkHandle->mFileId < 0)
            return;
        KFS_LOG_VA_DEBUG("closing fileid = %d, for chunk = %ld",
                         cih->chunkHandle->mFileId, 
                         cih->chunkHandle->mChunkId);
        close(cih->chunkHandle->mFileId);
        cih->chunkHandle->mFileId = -1;

        globals().ctrOpenDiskFds.Update(-1);
    }
}

int
ChunkManager::ChunkSize(kfsChunkId_t chunkId, off_t *chunkSize)
{
    ChunkInfoHandle_t *cih;

    if (GetChunkInfoHandle(chunkId, &cih) < 0)
        return -EBADF;

    *chunkSize = cih->chunkInfo.chunkSize;

    return 0;
}

int
ChunkManager::ReadChunk(ReadOp *op)
{
    ssize_t res;
    DiskConnection *d;
    ChunkInfoHandle_t *cih;
    off_t offset;
    size_t numBytesIO;

    if (GetChunkInfoHandle(op->chunkId, &cih) < 0)
        return -EBADF;

    d = SetupDiskConnection(op->chunkId, op);
    if (d == NULL)
        return -KFS::ESERVERBUSY;

    if (op->chunkVersion != cih->chunkInfo.chunkVersion) {
        KFS_LOG_VA_INFO("Version # mismatch(have=%u vs asked=%ld...failing a read",
                         cih->chunkInfo.chunkVersion, op->chunkVersion);
        return -KFS::EBADVERS;
    }
    op->diskConnection.reset(d);

    // schedule a read based on the chunk size
    if (op->offset >= (off_t) cih->chunkInfo.chunkSize) {
        op->numBytesIO = 0;
    } else if (op->offset + op->numBytes > cih->chunkInfo.chunkSize) {
        op->numBytesIO = cih->chunkInfo.chunkSize - op->offset;
    } else {
        op->numBytesIO = op->numBytes;
    }

    if (op->numBytesIO == 0)
        return -EIO;

    // for checksumming to work right, reads should be in terms of
    // checksum-blocks.
    offset = OffsetToChecksumBlockStart(op->offset);

    numBytesIO = (op->numBytesIO / CHECKSUM_BLOCKSIZE) * CHECKSUM_BLOCKSIZE;
    if (op->numBytesIO % CHECKSUM_BLOCKSIZE)
        numBytesIO += CHECKSUM_BLOCKSIZE;

    // Make sure we don't try to read past EOF; the checksumming will
    // do the necessary zero-padding. 
    if (offset + numBytesIO > cih->chunkInfo.chunkSize)
        numBytesIO = cih->chunkInfo.chunkSize - offset;
    
    if ((res = op->diskConnection->Read(offset, numBytesIO)) < 0)
        return -EIO;

    // read was successfully scheduled
    return 0;
}

int
ChunkManager::WriteChunk(WriteOp *op)
{
    ChunkInfoHandle_t *cih;

    if (GetChunkInfoHandle(op->chunkId, &cih) < 0)
        return -EBADF;

    // schedule a write based on the chunk size.  Make sure that a
    // write doesn't overflow the size of a chunk.
    op->numBytesIO = min((size_t) (KFS::CHUNKSIZE - op->offset), op->numBytes);

    if (op->numBytesIO == 0)
        return -EINVAL;

#if defined(__APPLE__)
    size_t addedBytes = max((long long) 0,
		    op->offset + op->numBytesIO - cih->chunkInfo.chunkSize);
#else
    size_t addedBytes = max((size_t) 0,
			    (size_t) (op->offset + op->numBytesIO - cih->chunkInfo.chunkSize));
#endif

    if (mUsedSpace + addedBytes >= mTotalSpace)
	return -ENOSPC;

    if ((OffsetToChecksumBlockStart(op->offset) == op->offset) &&
        ((size_t) op->numBytesIO >= (size_t) CHECKSUM_BLOCKSIZE)) {
        assert(op->numBytesIO % CHECKSUM_BLOCKSIZE == 0);
        if (op->numBytesIO % CHECKSUM_BLOCKSIZE != 0) {
            return -EINVAL;
        }
        // Hopefully, common case: write covers an entire block and
        // so, we just compute checksum and get on with the write.
        op->checksums = ComputeChecksums(op->dataBuf, op->numBytesIO);
    } else {

        assert((size_t) op->numBytesIO < (size_t) CHECKSUM_BLOCKSIZE);

        // The checksum block we are after is beyond the current
        // end-of-chunk.  So, treat that as a 0-block and splice in.
        if (OffsetToChecksumBlockStart(op->offset) >= cih->chunkInfo.chunkSize) {
            IOBuffer *data = new IOBuffer();

            data->ZeroFill(CHECKSUM_BLOCKSIZE);
            data->Splice(op->dataBuf,
                         op->offset % CHECKSUM_BLOCKSIZE,
                         op->numBytesIO);
            delete op->dataBuf;
            op->dataBuf = data;
            goto do_checksum;
            
        }
        // Need to read the data block over which the checksum is
        // computed. 
        if (op->rop == NULL) {
            // issue a read
            ReadOp *rop = new ReadOp(op, OffsetToChecksumBlockStart(op->offset),
                                     CHECKSUM_BLOCKSIZE);
            KFS_LOG_VA_DEBUG("Write triggered a read for offset = %ld",
                             op->offset);
            rop->Execute();

            if (rop->status < 0) {
                int res = rop->status;

                rop->wop = NULL;
                delete rop;
                return res;
            }
            op->rop = rop;

            return 0;
        }
        // If the read failed, cleanup and bail
        if (op->rop->status < 0) {
            op->status = op->rop->status;
            op->rop->wop = NULL;
            delete op->rop;
            op->rop = NULL;
            return op->HandleDone(EVENT_DISK_ERROR, NULL);
        }

        // All is good.  So, get on with checksumming
        op->rop->dataBuf->Splice(op->dataBuf,
                                 op->offset % CHECKSUM_BLOCKSIZE,
                                 op->numBytesIO);

        delete op->dataBuf;
        op->dataBuf = op->rop->dataBuf;
        op->rop->dataBuf = NULL;
        // If the buffer doesn't have a full CHECKSUM_BLOCKSIZE worth
        // of data, zero-pad the end.  We don't need to zero-pad the
        // front because the underlying filesystem will zero-fill when
        // we read a hole.
        ZeroPad(op->dataBuf);

      do_checksum:
        assert(op->dataBuf->BytesConsumable() == (int) CHECKSUM_BLOCKSIZE);

        uint32_t cksum = ComputeBlockChecksum(op->dataBuf, CHECKSUM_BLOCKSIZE); 
        op->checksums.push_back(cksum);

        // eat away the stuff at the beginning, so that we write out
        // exactly where we were asked from.
        off_t extra = op->offset - OffsetToChecksumBlockStart(op->offset);
        if (extra > 0)
            op->dataBuf->Consume(extra);
    }

    DiskConnection *d = SetupDiskConnection(op->chunkId, op);
    if (d == NULL)
        return -KFS::ESERVERBUSY;

    op->diskConnection.reset(d);

    /*
    KFS_LOG_VA_DEBUG("Checksum for chunk: %ld, offset = %ld, bytes = %ld, # of cksums = %u",
                  op->chunkId, op->offset, op->numBytesIO, op->checksums.size());
    */

    return op->diskConnection->Write(op->offset, op->numBytesIO, op->dataBuf);
}

void
ChunkManager::WriteChunkDone(WriteOp *op)
{
    ChunkInfoHandle_t *cih;
    
    if (GetChunkInfoHandle(op->chunkId, &cih) < 0)
        return;

    mIsChunkTableDirty = true;

    off_t endOffset = op->offset + op->numBytesIO;

    for (vector<uint32_t>::size_type i = 0; i < op->checksums.size(); i++) {
        off_t offset = op->offset + i * CHECKSUM_BLOCKSIZE;
        uint32_t checksumBlock = OffsetToChecksumBlockNum(offset);
        
        if (cih->chunkInfo.chunkBlockChecksum.size() <= checksumBlock)
            cih->chunkInfo.chunkBlockChecksum.resize(checksumBlock + 1);
        
        cih->chunkInfo.chunkBlockChecksum[checksumBlock] = op->checksums[i];
    }

    if (cih->chunkInfo.chunkSize < endOffset) {

	mUsedSpace += endOffset - cih->chunkInfo.chunkSize;
        cih->chunkInfo.chunkSize = endOffset;
    }
    assert(0 <= mUsedSpace && mUsedSpace <= mTotalSpace);
}

void
ChunkManager::ReadChunkDone(ReadOp *op)
{
    ChunkInfoHandle_t *cih = NULL;
    
    if ((GetChunkInfoHandle(op->chunkId, &cih) < 0) ||
        (op->chunkVersion != cih->chunkInfo.chunkVersion)) {
        AdjustDataRead(op);
        if (cih) {
            KFS_LOG_VA_INFO("Version # mismatch(have=%u vs asked=%ld...",
                             cih->chunkInfo.chunkVersion, op->chunkVersion);
        }
        op->status = -KFS::EBADVERS;
        return;
    }

    ZeroPad(op->dataBuf);

    assert(op->dataBuf->BytesConsumable() >= (int) CHECKSUM_BLOCKSIZE);

    // either nothing to verify or it better match

    bool mismatch = false;

    // figure out the block we are starting from and grab all the checksums
    vector<uint32_t>::size_type i, checksumBlock = OffsetToChecksumBlockStart(op->offset);
    vector<uint32_t> checksums = ComputeChecksums(op->dataBuf, op->dataBuf->BytesConsumable());

    for (i = 0; i < checksums.size() &&
             checksumBlock < cih->chunkInfo.chunkBlockChecksum.size(); 
         checksumBlock++, i++) {
        if ((cih->chunkInfo.chunkBlockChecksum[checksumBlock] == 0) ||
            (checksums[i] == cih->chunkInfo.chunkBlockChecksum[checksumBlock])) {
            continue;
        }
        mismatch = true;
        break;
    }

    if (!mismatch) {
        // for checksums to verify, we did reads in multiples of
        // checksum block sizes.  so, get rid of the extra
        AdjustDataRead(op);
        return;
    }

    // die ("checksum mismatch");

    KFS_LOG_VA_ERROR("Checksum mismatch for chunk=%ld, offset=%ld, bytes = %ld: expect: %u, computed: %u ",
                  op->chunkId, op->offset, op->numBytesIO,
                  cih->chunkInfo.chunkBlockChecksum[checksumBlock],
                  checksums[i]);

    op->status = -KFS::EBADCKSUM;

    // Notify the metaserver that the chunk we have is "bad"; the
    // metaserver will re-replicate this chunk.
    NotifyMetaCorruptedChunk(op->chunkId);
    
    // Take out the chunk from our side
    StaleChunk(op->chunkId);
}



void
ChunkManager::NotifyMetaCorruptedChunk(kfsChunkId_t chunkId)
{
    ChunkInfoHandle_t *cih;

    if (GetChunkInfoHandle(chunkId, &cih) < 0) {
        KFS_LOG_VA_ERROR("Unable to notify metaserver of corrupt chunk: %ld",
                      chunkId);
        return;
    }

    KFS_LOG_VA_INFO("Notifying metaserver of corrupt chunk (%ld) in file %ld",
                 cih->chunkInfo.fileId, chunkId);

    // This op will get deleted when we get an ack from the metaserver
    CorruptChunkOp *ccop = new CorruptChunkOp(0, cih->chunkInfo.fileId, 
                                              chunkId);
    gMetaServerSM.SubmitOp(ccop);
}

void
ChunkManager::ZeroPad(IOBuffer *buffer)
{
    int bytesFilled = buffer->BytesConsumable();
    if ((bytesFilled % CHECKSUM_BLOCKSIZE) == 0)
        return;

    int numToZero = CHECKSUM_BLOCKSIZE - (bytesFilled % CHECKSUM_BLOCKSIZE);
    if (numToZero > 0) {
        // pad with 0's
        buffer->ZeroFill(numToZero);
    }
}

void
ChunkManager::AdjustDataRead(ReadOp *op)
{
    size_t extraRead = op->offset - OffsetToChecksumBlockStart(op->offset);
    if (extraRead > 0)
        op->dataBuf->Consume(extraRead);
    if (op->dataBuf->BytesConsumable() > op->numBytesIO)
        op->dataBuf->Trim(op->numBytesIO);
}

uint32_t 
ChunkManager::GetChecksum(kfsChunkId_t chunkId, off_t offset)
{
    uint32_t checksumBlock = OffsetToChecksumBlockNum(offset);
    ChunkInfoHandle_t *cih;

    if (GetChunkInfoHandle(chunkId, &cih) < 0)
        return 0;

    if (cih->chunkInfo.chunkBlockChecksum.size() <= checksumBlock)
        return 0;
    return cih->chunkInfo.chunkBlockChecksum[checksumBlock];
}

DiskConnection *
ChunkManager::SetupDiskConnection(kfsChunkId_t chunkId,
                                  KfsOp *op)
{
    ChunkInfoHandle_t *cih;
    DiskConnection *diskConnection;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) {
        return NULL;
    }

    cih = tableEntry->second;
    if (cih->chunkHandle->mFileId < 0) {
        CleanupInactiveFds();
        if (OpenChunk(chunkId, O_RDWR) < 0) 
            return NULL;
    }
 
    cih->lastIOTime = time(0);
    diskConnection = new DiskConnection(cih->chunkHandle, op);
    return diskConnection;
}
    
void
ChunkManager::CancelChunkOp(KfsCallbackObj *cont, kfsChunkId_t chunkId)
{
    // Cancel the chunk operations scheduled by KfsCallbackObj on chunkId.
    // XXX: Fill it...
}

//
// dump out the contents of the chunkTable to disk
//
void
ChunkManager::Checkpoint()
{
    ChunkInfoHandle_t *cih;
    CheckpointOp *cop;
    // on the macs, i can't declare CMI iter;
    CMI iter = mChunkTable.begin();

    if (!mIsChunkTableDirty)
        return;

    // KFS_LOG_VA_DEBUG("Checkpointing state");
    cop = new CheckpointOp(1);
    
    for (iter = mChunkTable.begin(); iter != mChunkTable.end(); ++iter) {
        cih = iter->second;
        // If a chunk is being replicated, then it is not yet a part
        // of the namespace.  When replication is done, it becomes a
        // part of the namespace.  This model keeps recovery simple:
        // if we die in the midst of replicating a chunk, on restart,
        // we will the chunk as an orphan and throw it away.
        if (cih->isBeingReplicated)
            continue;
        cop->data << cih->chunkInfo.fileId << ' ';
        cop->data << cih->chunkInfo.chunkId << ' ';
        cop->data << cih->chunkInfo.chunkSize << ' ';
        cop->data << cih->chunkInfo.chunkVersion << ' ';

        cop->data << cih->chunkInfo.chunkBlockChecksum.size() << ' ';
        for (vector<uint32_t>::size_type i = 0; i < cih->chunkInfo.chunkBlockChecksum.size();
             ++i) {
            cop->data << cih->chunkInfo.chunkBlockChecksum[i] << ' ';
        }
        cop->data << endl;
    }
    
    gLogger.Submit(cop);

    // Now, everything is clean...
    mIsChunkTableDirty = false;
}

//
// Get all the chunk directory entries from all the places we can
// store the chunks into a single array.
//
int
ChunkManager::GetChunkDirsEntries(struct dirent ***namelist)
{
    struct dirent **entries;
    vector<struct dirent **> dirEntries;
    vector<int> dirEntriesCount;
    int res, numChunkFiles = 0;
    uint32_t i;

    *namelist = NULL;
    for (i = 0; i < mChunkDirs.size(); i++) {
        res = scandir(mChunkDirs[i].c_str(), &entries, 0, alphasort);
        if (res < 0) {
            KFS_LOG_VA_INFO("Unable to open %s", mChunkDirs[i].c_str());
            for (i = 0; i < dirEntries.size(); i++) {
                entries = dirEntries[i];
                for (int j = 0; j < dirEntriesCount[i]; j++)
                    free(entries[j]);
                free(entries);
            }
            dirEntries.clear();
            return -1;
        }
        dirEntries.push_back(entries);
        dirEntriesCount.push_back(res);
        numChunkFiles += res;
    }
    
    // Get all the directory entries into one giganto array
    *namelist = (struct dirent **) malloc(sizeof(struct dirent **) * numChunkFiles);

    numChunkFiles = 0;
    for (i = 0; i < dirEntries.size(); i++) {
        int count = dirEntriesCount[i];
        entries = dirEntries[i];

        memcpy((*namelist) + numChunkFiles, entries, count * sizeof(struct dirent **));
        numChunkFiles += count;
    }
    return numChunkFiles;
}

//
// Restart from a checkpoint. Validate that the files in the
// checkpoint exist in the chunks directory.
//
void
ChunkManager::Restart()
{
    ChunkInfoHandle_t *cih;
    string chunkIdStr, chunkPathname;
    ChunkInfo_t entry;
    int i, res, numChunkFiles;
    bool found;
    struct stat buf;
    struct dirent **namelist;
    CMI iter = mChunkTable.begin();
    vector<kfsChunkId_t> orphans;
    vector<kfsChunkId_t>::size_type j;

    // sort all the chunk names alphabetically in each of the
    // directories
    numChunkFiles = GetChunkDirsEntries(&namelist);
    if (numChunkFiles < 0)
        return;

    gLogger.Restore();

    // Now, validate: for each entry in the chunk table, verify that
    // the backing file exists. also, if there any "zombies" lying
    // around---that is, the file exists, but there is no associated
    // entry in the chunk table, nuke the backing file.

    for (iter = mChunkTable.begin(); iter != mChunkTable.end(); ++iter) {
        entry = iter->second->chunkInfo;

        chunkIdStr = boost::lexical_cast<std::string>(entry.chunkId);

        found = false;
        for (i = 0; i < numChunkFiles; ++i) {
            if (namelist[i] &&
                (chunkIdStr == namelist[i]->d_name)) {
                free(namelist[i]);
                namelist[i] = NULL;
                found = true;
                break;
            }
        }
        if (!found) {
            KFS_LOG_VA_INFO("Orphaned chunk as the file doesn't exist: %s",
                             chunkIdStr.c_str());
            orphans.push_back(entry.chunkId);
            continue;
        }

        chunkPathname = MakeChunkPathname(entry.chunkId);
        res = stat(chunkPathname.c_str(), &buf);
        if (res < 0)
            continue;

        // stat buf's st_size is of type off_t.  Typecast to avoid compiler warnings.
        if (buf.st_size != (off_t) entry.chunkSize) {
            KFS_LOG_VA_INFO("Truncating file: %s to size: %zd",
                             chunkPathname.c_str(), entry.chunkSize);
            if (truncate(chunkPathname.c_str(), entry.chunkSize) < 0) {
                perror("Truncate");
            }
        }
    }
    
    if (orphans.size() > 0) {
        // Take a checkpoint after we are done replay
        mIsChunkTableDirty = true;
    }

    // Get rid of the orphans---valid entries but no backing file
    for (j = 0; j < orphans.size(); ++j) {

        KFS_LOG_VA_DEBUG("Found orphan entry: %ld", orphans[j]);

        iter = mChunkTable.find(orphans[j]);
        if (iter != mChunkTable.end()) {
            cih = iter->second;
            mUsedSpace -= cih->chunkInfo.chunkSize;
            mChunkTable.erase(orphans[j]);
            delete cih;
        }
    }

    // Get rid of zombies---backing file exists, but no entry in logs/ckpt
    for (i = 0; i < numChunkFiles; ++i) {
        if (namelist[i] == NULL)
            // entry was found (above)
            continue;
        if ((strcmp(namelist[i]->d_name, ".") == 0) ||
            (strcmp(namelist[i]->d_name, "..") == 0)) {
            free(namelist[i]);
            namelist[i] = NULL;
            continue;
        }

        // zombie
        chunkPathname = MakeChunkPathname(namelist[i]->d_name);

        // there could be directories here...such as lost+found etc...
        res = stat(chunkPathname.c_str(), &buf);
        if ((res == 0) && (S_ISREG(buf.st_mode))) {
            unlink(chunkPathname.c_str());
            KFS_LOG_VA_DEBUG("Found zombie entry: %s", chunkPathname.c_str());
        }

        free(namelist[i]);
        namelist[i] = NULL;
    }

    free(namelist);
    if (mIsChunkTableDirty) {
        Checkpoint();
    }

    assert((mUsedSpace >= 0) && (mUsedSpace <= mTotalSpace));
    // if there are no chunks, used space better be 0
    if (mChunkTable.size() == 0)
        assert(mUsedSpace == 0);
}

void
ChunkManager::AddMapping(ChunkInfoHandle_t *cih)
{
    mChunkTable[cih->chunkInfo.chunkId] = cih;
    mUsedSpace += cih->chunkInfo.chunkSize;
}

void
ChunkManager::ReplayAllocChunk(kfsFileId_t fileId, kfsChunkId_t chunkId,
                               int64_t chunkVersion)
{
    ChunkInfoHandle_t *cih;

    mIsChunkTableDirty = true;

    if (GetChunkInfoHandle(chunkId, &cih) == 0) {
        // If the entry exists, just update the version
        cih->chunkInfo.chunkVersion = chunkVersion;
        mChunkTable[chunkId] = cih;
        return;
    }
    // after replay is done, when we verify entries in the table, we
    // stat the file and fix up the sizes then.  so, no need to do
    // anything here.
    cih = new ChunkInfoHandle_t();
    cih->chunkInfo.fileId = fileId;
    cih->chunkInfo.chunkId = chunkId;
    cih->chunkInfo.chunkVersion = chunkVersion;
    mChunkTable[chunkId] = cih;
}

void
ChunkManager::ReplayChangeChunkVers(kfsFileId_t fileId, kfsChunkId_t chunkId,
                                    int64_t chunkVersion)
{
    ChunkInfoHandle_t *cih;

    if (GetChunkInfoHandle(chunkId, &cih) != 0) 
        return;

    KFS_LOG_VA_DEBUG("Chunk %ld already exists; changing version # to %ld",
                     chunkId, chunkVersion);
    
    // Update the version #
    cih->chunkInfo.chunkVersion = chunkVersion;
    mChunkTable[chunkId] = cih;
    mIsChunkTableDirty = true;
}

void
ChunkManager::ReplayDeleteChunk(kfsChunkId_t chunkId)
{
    ChunkInfoHandle_t *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    mIsChunkTableDirty = true;

    if (tableEntry != mChunkTable.end()) {
        cih = tableEntry->second;
        mUsedSpace -= cih->chunkInfo.chunkSize;
        mChunkTable.erase(chunkId);
        delete cih;
    }
}

void
ChunkManager::ReplayWriteDone(kfsChunkId_t chunkId, off_t chunkSize,
                              off_t offset, vector<uint32_t> checksums)
{
    ChunkInfoHandle_t *cih;
    int res;

    res = GetChunkInfoHandle(chunkId, &cih);
    if (res < 0)
        return;

    mIsChunkTableDirty = true;
    mUsedSpace -= cih->chunkInfo.chunkSize;    
    cih->chunkInfo.chunkSize = chunkSize;
    mUsedSpace += cih->chunkInfo.chunkSize;
    
    for (vector<uint32_t>::size_type i = 0; i < checksums.size(); i++) {
        off_t currOffset = offset + i * CHECKSUM_BLOCKSIZE;
        off_t checksumBlock = OffsetToChecksumBlockNum(currOffset);
        
        if (cih->chunkInfo.chunkBlockChecksum.size() <= checksumBlock)
            cih->chunkInfo.chunkBlockChecksum.resize(checksumBlock + 1);
        
        cih->chunkInfo.chunkBlockChecksum[checksumBlock] = checksums[i];
    }
}

void
ChunkManager::ReplayTruncateDone(kfsChunkId_t chunkId, off_t chunkSize)
{
    ChunkInfoHandle_t *cih;
    int res;
    off_t lastChecksumBlock;

    res = GetChunkInfoHandle(chunkId, &cih);
    if (res < 0)
        return;

    mIsChunkTableDirty = true;
    mUsedSpace -= cih->chunkInfo.chunkSize;    
    cih->chunkInfo.chunkSize = chunkSize;
    mUsedSpace += cih->chunkInfo.chunkSize;

    lastChecksumBlock = OffsetToChecksumBlockNum(chunkSize);
    cih->chunkInfo.chunkBlockChecksum.resize(lastChecksumBlock + 1);

    // XXX: Could do better; recomputed checksum should be in the log
    cih->chunkInfo.chunkBlockChecksum[lastChecksumBlock] = 0;
}

void
ChunkManager::GetHostedChunks(vector<ChunkInfo_t> &result)
{
    ChunkInfoHandle_t *cih;

    // walk thru the table and pick up the chunk-ids
    for (CMI iter = mChunkTable.begin(); iter != mChunkTable.end(); ++iter) {
        cih = iter->second;
        result.push_back(cih->chunkInfo);
    }
}

int
ChunkManager::GetChunkInfoHandle(kfsChunkId_t chunkId, ChunkInfoHandle_t **cih)
{
    CMI iter = mChunkTable.find(chunkId);

    if (iter == mChunkTable.end()) {
        *cih = NULL;
        return -EBADF;
    }

    *cih = iter->second;
    return 0;
}

int
ChunkManager::EnqueueWrite(WritePrepareOp *wp)
{
    WriteOp *op;
    ChunkInfoHandle_t *cih;

    if (GetChunkInfoHandle(wp->chunkId, &cih) < 0)
        return -EBADF;

    if (wp->chunkVersion != cih->chunkInfo.chunkVersion) {
        KFS_LOG_VA_INFO("Version # mismatch(have=%d vs asked=%lu...failing a write",
                         cih->chunkInfo.chunkVersion, wp->chunkVersion);
        return -EINVAL;
    }

    mWriteId++;
    op = new WriteOp(wp->seq, wp->chunkId, wp->chunkVersion,
                     wp->offset, wp->numBytes,
                     wp->dataBuf, mWriteId);
    op->enqueueTime = time(NULL);
    wp->dataBuf = NULL;
    wp->writeId = mWriteId;
    mPendingWrites.push_back(op);
    return 0;
}

// Helper functor that matches pending writes by chunkid's
class ChunkIdMatcher {
    kfsChunkId_t myid;
public:
    ChunkIdMatcher(kfsChunkId_t s) : myid(s) { }
    bool operator() (const WriteOp *r) {
        return (r->chunkId == myid);
    }
};

bool
ChunkManager::IsWritePending(kfsChunkId_t chunkId)
{
    list<WriteOp *>::iterator i;

    i = find_if(mPendingWrites.begin(), mPendingWrites.end(), 
                ChunkIdMatcher(chunkId));
    if (i == mPendingWrites.end())
        return false;
    return true;
}

int64_t
ChunkManager::GetChunkVersion(kfsChunkId_t c)
{
    ChunkInfoHandle_t *cih;

    if (GetChunkInfoHandle(c, &cih) < 0)
        return -1;

    return cih->chunkInfo.chunkVersion;
}

// Helper functor that matches write id's by sequence #'s
class WriteIdMatcher {
    int64_t myid;
public:
    WriteIdMatcher(int64_t s) : myid(s) { }
    bool operator() (const WriteOp *r) {
        return (r->writeId == myid);
    }
};

WriteOp *
ChunkManager::GetWriteOp(int64_t writeId)
{
    list<WriteOp *>::iterator i;
    WriteOp *op;

    i = find_if(mPendingWrites.begin(), mPendingWrites.end(), 
                WriteIdMatcher(writeId));
    if (i == mPendingWrites.end())
        return NULL;
    op = *i;
    mPendingWrites.erase(i);
    return op;
}

void
ChunkManager::ScavengePendingWrites()
{
    list<WriteOp *>::iterator i;
    WriteOp *op;
    time_t now = time(NULL);

    i = mPendingWrites.begin();
    while (i != mPendingWrites.end()) {
        op = *i;
        // The list is sorted by enqueue time
        if (now - op->enqueueTime < MAX_PENDING_WRITE_LRU_SECS) {
            break;
        }
        // if it exceeds 5 mins, retire the op
        KFS_LOG_VA_DEBUG("Retiring write with id=%ld as it has been too long",
                         op->writeId);
        mPendingWrites.pop_front();
        CloseChunk(op->chunkId);
        delete op;
        i = mPendingWrites.begin();
    }
}

int
ChunkManager::Sync(WriteOp *op)
{
    if (!op->diskConnection) {
        return -1;
    }
    return op->diskConnection->Sync();
}

class InactiveFdCleaner {
    time_t now;
public:
    InactiveFdCleaner(time_t n) : now(n) { }
    void operator() (const std::tr1::unordered_map<kfsChunkId_t, ChunkInfoHandle_t *>::value_type v) {
        ChunkInfoHandle_t *cih  = v.second;
        
        if ((cih->chunkHandle->mFileId < 0) ||
            (now - cih->lastIOTime < INACTIVE_FDS_CLEANUP_INTERVAL_SECS))
            return;

        // we have a valid file-id and it has been over 5 mins since we last did I/O on it.
        KFS_LOG_VA_DEBUG("cleanup: closing fileid = %d, for chunk = %ld",
                         cih->chunkHandle->mFileId, 
                         cih->chunkHandle->mChunkId);
        close(cih->chunkHandle->mFileId);
        cih->chunkHandle->mFileId = -1;

        globals().ctrOpenDiskFds.Update(-1);
            
    }
};

void
ChunkManager::CleanupInactiveFds()
{
    static time_t lastCleanupTime = time(0);
    time_t now = time(0);

    if (OPEN_FDS_LOW_WATERMARK == 0) {
        struct rlimit rlim;
        int res;

        res = getrlimit(RLIMIT_NOFILE, &rlim);
        if (res == 0) {
            OPEN_FDS_LOW_WATERMARK = rlim.rlim_cur / 2;
            // bump the soft limit to the hard limit
            rlim.rlim_cur = rlim.rlim_max;
            if (setrlimit(RLIMIT_NOFILE, &rlim) == 0) {
                KFS_LOG_VA_DEBUG("Setting # of open files to: %ld",
                                 rlim.rlim_cur);
                OPEN_FDS_LOW_WATERMARK = rlim.rlim_cur / 2;
            }
        }
    }
    int totalOpenFds = globals().ctrOpenDiskFds.GetValue() +
        globals().ctrOpenNetFds.GetValue();

    // if we haven't cleaned up in 5 mins or if we too many fd's that
    // are open, clean up.
    if ((now - lastCleanupTime < INACTIVE_FDS_CLEANUP_INTERVAL_SECS) ||
        (totalOpenFds < OPEN_FDS_LOW_WATERMARK)) { 
        return;
    }

    lastCleanupTime = time(0);

    for_each(mChunkTable.begin(), mChunkTable.end(), InactiveFdCleaner(now));
}

string
KFS::GetStaleChunkPath(const string &partition)
{
    return partition + "/lost+found/";
}

int64_t
ChunkManager::GetTotalSpace() const
{

    int64_t availableSpace;
    if (mChunkDirs.size() > 1) {
        return mTotalSpace;
    }

    // report the space based on availability
#if defined(__i386__)
    struct statvfs64 result;

    if (statvfs64(mChunkDirs[0].c_str(), &result) < 0) {
        KFS_LOG_VA_DEBUG("statvfs failed...returning %ld", mTotalSpace);
        return mTotalSpace;
    }

#else
    struct statvfs result;

    if (statvfs(mChunkDirs[0].c_str(), &result) < 0) {
        KFS_LOG_VA_DEBUG("statvfs failed...returning %ld", mTotalSpace);
        return mTotalSpace;
    }

#endif

    if (result.f_frsize == 0)
        return mTotalSpace;

    availableSpace = result.f_bavail * result.f_frsize;
    // we got all the info...so report true value
    return min(availableSpace, mTotalSpace);
}
