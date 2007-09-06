//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/ChunkManager.cc#3 $
//
// Created 2006/03/28
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

extern "C" {
#include <dirent.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
}
#include "ChunkManager.h"
#include "common/log.h"
#include "Utils.h"
#include "common/kfstypes.h"
#include "libkfsIO/Counter.h"
#include "libkfsIO/Checksum.h"
#include "libkfsIO/Globals.h"
using namespace libkfsio;

#include <fstream>
#include <sstream>
#include <algorithm>
#include <string>

#include <boost/lexical_cast.hpp>

using std::ofstream;
using std::ifstream;
using std::istringstream;
using std::ios_base;
using std::min;
using std::max;
using std::endl;
using std::find_if;
using std::string;
using std::vector;

// Cleanup fds on which no I/O has been done for the past N secs
const int INACTIVE_FDS_CLEANUP_INTERVAL_SECS = 300;

// The # of fd's that we allow to be open before cleanup kicks in.
// This value will be set to : # of files that the process can open / 2
int OPEN_FDS_LOW_WATERMARK = 0;

ChunkManager::ChunkManager()
{
    mTotalSpace = mUsedSpace = 0;
    mChunkBaseDir = NULL;
    mChunkManagerTimeoutImpl = new ChunkManagerTimeoutImpl(this);
    mIsChunkTableDirty = false;
}

ChunkManager::~ChunkManager()
{
    CMI iter;
    ChunkInfoHandle_t *cih;

    mChunkBaseDir = NULL;
    
    for (iter = mChunkTable.begin(); iter != mChunkTable.end(); ++iter) {
        cih = iter->second;
        delete cih;
    }
    mChunkTable.clear();
    globals().netManager.UnRegisterTimeoutHandler(mChunkManagerTimeoutImpl);
    delete mChunkManagerTimeoutImpl;
}

void 
ChunkManager::Init(const char *chunkBaseDir, size_t totalSpace)
{
    mTotalSpace = totalSpace;
    mChunkBaseDir = chunkBaseDir;
}

int
ChunkManager::AllocChunk(kfsFileId_t fileId, kfsChunkId_t chunkId, 
                         int64_t chunkVersion,
                         bool isBeingReplicated)
{
    string chunkPathname;
    int fd;
    ChunkInfoHandle_t *cih;
    CMI tableEntry;

    mIsChunkTableDirty = true;

    chunkPathname = MakeChunkPathname(chunkId);

    tableEntry = mChunkTable.find(chunkId);
    if (tableEntry != mChunkTable.end()) {
        COSMIX_LOG_DEBUG("Chunk %s already exists; changing version # to %ld",
                         chunkPathname.c_str(), chunkVersion);
        cih = tableEntry->second;
        cih->chunkInfo.chunkVersion = chunkVersion;
        mChunkTable[chunkId] = cih;
        return 0;
    }
    
    COSMIX_LOG_DEBUG("Creating chunk: %s", chunkPathname.c_str());

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
    CMI tableEntry;

    tableEntry = mChunkTable.find(chunkId);
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
ChunkManager::TruncateChunk(kfsChunkId_t chunkId, size_t chunkSize)
{
    string chunkPathname;
    ChunkInfoHandle_t *cih;
    CMI tableEntry;
    int res;
    uint32_t lastChecksumBlock;

    // the truncated size should not exceed chunk size.
    if (chunkSize > KFS::CHUNKSIZE)
        return -EINVAL;

    tableEntry = mChunkTable.find(chunkId);
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
    CMI tableEntry;

    chunkPathname = MakeChunkPathname(chunkId);

    tableEntry = mChunkTable.find(chunkId);
    if (tableEntry == mChunkTable.end()) {
        return -1;
    }

    mIsChunkTableDirty = true;

    COSMIX_LOG_DEBUG("Chunk %s already exists; changing version # to %ld",
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
    CMI tableEntry;

    tableEntry = mChunkTable.find(chunkId);
    if (tableEntry == mChunkTable.end()) {
        return;
    }

#ifdef DEBUG
    string chunkPathname = MakeChunkPathname(chunkId);
    COSMIX_LOG_DEBUG("Replication for chunk %s is complete...",
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
ChunkManager::MakeChunkPathname(kfsChunkId_t chunkId)
{
    ostringstream os;

    os << mChunkBaseDir << '/' << chunkId;
    return os.str();
}

string
ChunkManager::MakeChunkPathname(const char *chunkId)
{
    ostringstream os;

    os << mChunkBaseDir << '/' << chunkId;
    return os.str();
}

int
ChunkManager::OpenChunk(kfsChunkId_t chunkId, 
                        int openFlags)
{
    string chunkPathname;
    int fd;
    ChunkInfoHandle_t *cih;
    CMI tableEntry;

    chunkPathname = MakeChunkPathname(chunkId);
    tableEntry = mChunkTable.find(chunkId);
    if (tableEntry == mChunkTable.end()) {
        COSMIX_LOG_DEBUG("No such chunk: %s", chunkPathname.c_str());
        return -EBADF;
    }

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

    COSMIX_LOG_DEBUG("opening %s with flags %d; fd = %d", 
                     chunkPathname.c_str(), openFlags, fd);

    return 0;
}

void
ChunkManager::CloseChunk(kfsChunkId_t chunkId)
{
    ChunkInfoHandle_t *cih;
    CMI tableEntry;

    tableEntry = mChunkTable.find(chunkId);
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
        COSMIX_LOG_DEBUG("closing fileid = %d, for chunk = %ld",
                         cih->chunkHandle->mFileId, 
                         cih->chunkHandle->mChunkId);
        close(cih->chunkHandle->mFileId);
        cih->chunkHandle->mFileId = -1;

        globals().ctrOpenDiskFds.Update(-1);
    }
}

int
ChunkManager::ChunkSize(kfsChunkId_t chunkId, size_t *chunkSize)
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
        COSMIX_LOG_DEBUG("Version # mismatch(have=%u vs asked=%ld...failing a read",
                         cih->chunkInfo.chunkVersion, op->chunkVersion);
        return -KFS::EBADVERS;
    }
    op->diskConnection.reset(d);

    // schedule a read based on the chunk size
    if (op->offset >= (off_t) cih->chunkInfo.chunkSize) {
        op->numBytesIO = 0;
    } else if (cih->chunkInfo.chunkSize < op->offset + op->numBytes) {
        op->numBytesIO = cih->chunkInfo.chunkSize - op->offset;
    } else {
        op->numBytesIO = op->numBytes;
    }

    if (op->numBytesIO == 0)
        return -EIO;

    // for checksumming to work right, reads should be in terms of
    // checksum-blocks.
    offset = OffsetToChecksumBlockStart(op->offset);
    numBytesIO = CHECKSUM_BLOCKSIZE;
        
    if ((res = op->diskConnection->Read(offset, numBytesIO)) < 0)
        return -1;

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
    op->numBytesIO = min(KFS::CHUNKSIZE - op->offset, op->numBytes);

    if (op->numBytesIO == 0)
        return -EINVAL;

    size_t addedBytes = max((size_t)0,
		    op->offset + op->numBytesIO - cih->chunkInfo.chunkSize);
    if (mUsedSpace + addedBytes >= mTotalSpace)
	return -ENOSPC;

    if ((OffsetToChecksumBlockStart(op->offset) == op->offset) &&
        (op->numBytesIO >= CHECKSUM_BLOCKSIZE)) {
        assert(op->numBytesIO % CHECKSUM_BLOCKSIZE == 0);
        if (op->numBytesIO % CHECKSUM_BLOCKSIZE != 0) {
            return -EINVAL;
        }
        // Hopefully, common case: write covers an entire block and
        // so, we just compute checksum and get on with the write.
        op->checksums = ComputeChecksums(op->dataBuf, op->numBytesIO);
    } else {

        assert(op->numBytesIO < CHECKSUM_BLOCKSIZE);

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
            COSMIX_LOG_DEBUG("Write triggered a read for offset = %ld",
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

    COSMIX_LOG_DEBUG("Checksum for chunk: %ld, offset = %ld, bytes = %ld, cksum = %u",
                     op->chunkId, op->offset, op->numBytesIO, op->checksums[0]);

    return op->diskConnection->Write(op->offset, op->numBytesIO, op->dataBuf);
}

void
ChunkManager::WriteChunkDone(WriteOp *op)
{
    ChunkInfoHandle_t *cih;
    
    if (GetChunkInfoHandle(op->chunkId, &cih) < 0)
        return;

    mIsChunkTableDirty = true;

    size_t endOffset = op->offset + op->numBytesIO;

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
    ChunkInfoHandle_t *cih;
    
    if ((GetChunkInfoHandle(op->chunkId, &cih) < 0) ||
        (op->chunkVersion != cih->chunkInfo.chunkVersion)) {
        AdjustDataRead(op);
        if (cih) {
            COSMIX_LOG_DEBUG("Version # mismatch(have=%u vs asked=%ld...",
                             cih->chunkInfo.chunkVersion, op->chunkVersion);
        }
        op->status = -KFS::EBADVERS;
        return;
    }

    ZeroPad(op->dataBuf);

    assert(op->dataBuf->BytesConsumable() == (int) CHECKSUM_BLOCKSIZE);

    vector<uint32_t>::size_type checksumBlock = OffsetToChecksumBlockStart(op->offset);
    uint32_t checksum = ComputeBlockChecksum(op->dataBuf, CHECKSUM_BLOCKSIZE);
    
    // either nothing to verify or it better match
    if ((checksumBlock > cih->chunkInfo.chunkBlockChecksum.size()) ||
        (cih->chunkInfo.chunkBlockChecksum[checksumBlock] == 0) ||
        (checksum == cih->chunkInfo.chunkBlockChecksum[checksumBlock])) {
        // for checksums to verify, we did reads in multiples of
        // checksum block sizes.  so, get rid of the extra
        AdjustDataRead(op);
        return;
    }

    COSMIX_LOG_DEBUG("Checksum mismatch for chunk=%ld, offset=%ld, bytes = %ld: expect: %u, computed: %u ",
                     op->chunkId, op->offset, op->numBytesIO,
                     cih->chunkInfo.chunkBlockChecksum[checksumBlock],
                     checksum);
    die("Checksum mismatch");

    op->status = -KFS::EBADCKSUM;
}

void
ChunkManager::ZeroPad(IOBuffer *buffer)
{
    int numToZero = CHECKSUM_BLOCKSIZE - buffer->BytesConsumable();
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
    CMI tableEntry;
    DiskConnection *diskConnection;

    tableEntry = mChunkTable.find(chunkId);
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
    CMI iter;
    ChunkInfoHandle_t *cih;
    CheckpointOp *cop;

    if (!mIsChunkTableDirty)
        return;

    COSMIX_LOG_DEBUG("Checkpointing state");
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
    CMI iter;
    vector<kfsChunkId_t> orphans;
    vector<kfsChunkId_t>::size_type j;

    // sort all the chunk names alphabetically
    numChunkFiles = scandir(mChunkBaseDir, &namelist, 0, alphasort);
    if (numChunkFiles < 0) {
        COSMIX_LOG_INFO("Unable to open %s", mChunkBaseDir);
        return;
    }

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
            COSMIX_LOG_DEBUG("Orphaned chunk as the file doesn't exist: %s",
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
            COSMIX_LOG_DEBUG("Truncating file: %s to size: %zd",
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

        COSMIX_LOG_DEBUG("Found orphan entry: %ld",
                         orphans[j]);

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
        unlink(chunkPathname.c_str());
        free(namelist[i]);
        namelist[i] = NULL;

        COSMIX_LOG_DEBUG("Found zombie entry: %s", chunkPathname.c_str());
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

    COSMIX_LOG_DEBUG("Chunk %ld already exists; changing version # to %ld",
                     chunkId, chunkVersion);
    
    // Update the version #
    cih->chunkInfo.chunkVersion = chunkVersion;
    mChunkTable[chunkId] = cih;
    mIsChunkTableDirty = true;
}

void
ChunkManager::ReplayDeleteChunk(kfsChunkId_t chunkId)
{
    CMI tableEntry;
    ChunkInfoHandle_t *cih;

    mIsChunkTableDirty = true;

    tableEntry = mChunkTable.find(chunkId);
    if (tableEntry != mChunkTable.end()) {
        cih = tableEntry->second;
        mUsedSpace -= cih->chunkInfo.chunkSize;
        mChunkTable.erase(chunkId);
        delete cih;
    }
}

void
ChunkManager::ReplayWriteDone(kfsChunkId_t chunkId, size_t chunkSize,
                              uint32_t offset, vector<uint32_t> checksums)
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
        uint32_t checksumBlock = OffsetToChecksumBlockNum(currOffset);
        
        if (cih->chunkInfo.chunkBlockChecksum.size() <= checksumBlock)
            cih->chunkInfo.chunkBlockChecksum.resize(checksumBlock + 1);
        
        cih->chunkInfo.chunkBlockChecksum[checksumBlock] = checksums[i];
    }
}

void
ChunkManager::ReplayTruncateDone(kfsChunkId_t chunkId, size_t chunkSize)
{
    ChunkInfoHandle_t *cih;
    int res;
    uint32_t lastChecksumBlock;

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
    CMI iter;
    ChunkInfoHandle_t *cih;

    // walk thru the table and pick up the chunk-ids
    for (iter = mChunkTable.begin(); iter != mChunkTable.end(); ++iter) {
        cih = iter->second;
        result.push_back(cih->chunkInfo);
    }
}

int
ChunkManager::GetChunkInfoHandle(kfsChunkId_t chunkId, ChunkInfoHandle_t **cih)
{
    CMI iter;

    iter = mChunkTable.find(chunkId);
    if (iter == mChunkTable.end())
        return -EBADF;

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
        COSMIX_LOG_DEBUG("Version # mismatch(have=%d vs asked=%lu...failing a write",
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
        COSMIX_LOG_DEBUG("Retiring write with id=%ld as it has been too long",
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
        COSMIX_LOG_DEBUG("cleanup: closing fileid = %d, for chunk = %ld",
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
                COSMIX_LOG_DEBUG("Setting # of open files to: %ld",
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
