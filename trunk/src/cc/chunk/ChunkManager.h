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
// \file ChunkManager.h
// \brief Handles all chunk related ops.
//
//----------------------------------------------------------------------------

#ifndef _CHUNKMANAGER_H
#define _CHUNKMANAGER_H

#include <tr1/unordered_map>
#include <vector>
#include <string>

#include "libkfsIO/ITimeout.h"
#include "libkfsIO/Chunk.h"
#include "libkfsIO/DiskManager.h"
#include "KfsOps.h"
#include "Logger.h"
#include "common/cxxutil.h"

namespace KFS
{

/// Encapsulate a chunk file descriptor and information about the
/// chunk such as name and version #.
struct ChunkInfoHandle_t {
    ChunkInfoHandle_t() : chunkHandle(new ChunkHandle_t), 
                          lastIOTime(0), isBeingReplicated(false) {  };

    struct ChunkInfo_t chunkInfo;
    ChunkHandlePtr	chunkHandle;
    time_t lastIOTime;  // when was the last I/O done on this chunk
    bool isBeingReplicated;  // is the chunk being replicated from another server
};

/// Map from a chunk id to a chunk handle
///
typedef std::tr1::unordered_map<kfsChunkId_t, ChunkInfoHandle_t *> CMap;
typedef std::tr1::unordered_map<kfsChunkId_t, ChunkInfoHandle_t *>::const_iterator CMI;

/// Periodically write out the chunk manager state to disk
class ChunkManagerTimeoutImpl;

/// The chunk manager writes out chunks as individual files on disk.
/// The location of the chunk directory is defined by chunkBaseDir.
/// The file names of chunks is a string representation of the chunk
/// id.  The chunk manager performs disk I/O asynchronously---that is,
/// it schedules disk requests to the Disk manager which uses aio() to
/// perform the operations.
///
class ChunkManager {
public:
    ChunkManager();
    ~ChunkManager();
    
    /// Init function to configure the chunk manager object.
    void Init(const std::vector<std::string> &chunkDirs, int64_t totalSpace);

    /// Allocate a file to hold a chunk on disk.  The filename is the
    /// chunk id itself.
    /// @param[in] fileId  id of the file that has chunk chunkId
    /// @param[in] chunkId id of the chunk being allocated.
    /// @param[in] chunkVersion  the version assigned by the metaserver to this chunk
    /// @param[in] isBeingReplicated is the allocation for replicating a chunk?
    /// @retval status code
    int 	AllocChunk(kfsFileId_t fileId, kfsChunkId_t chunkId, 
                           int64_t chunkVersion,
                           bool isBeingReplicated = false);

    /// Delete a previously allocated chunk file.
    /// @param[in] chunkId id of the chunk being deleted.
    /// @retval status code
    int		DeleteChunk(kfsChunkId_t chunkId);

    /// A previously created chunk is stale; move it to stale chunks
    /// dir; space can be reclaimed later
    ///
    /// @param[in] chunkId id of the chunk being moved
    /// @retval status code
    int		StaleChunk(kfsChunkId_t chunkId);

    /// Truncate a chunk to the specified size
    /// @param[in] chunkId id of the chunk being truncated.
    /// @param[in] chunkSize  size to which chunk should be truncated.
    /// @retval status code
    int		TruncateChunk(kfsChunkId_t chunkId, size_t chunkSize);

    /// Change a chunk's version # to what the server says it should be.
    /// @param[in] fileId  id of the file that has chunk chunkId
    /// @param[in] chunkId id of the chunk being allocated.
    /// @param[in] chunkVersion  the version assigned by the metaserver to this chunk
    /// @retval status code
    int 	ChangeChunkVers(kfsFileId_t fileId, kfsChunkId_t chunkId, 
                           int64_t chunkVersion);

    /// Open a chunk for I/O.
    /// @param[in] chunkId id of the chunk being opened.
    /// @param[in] openFlags  O_RDONLY, O_WRONLY
    /// @retval status code
    int		OpenChunk(kfsChunkId_t chunkId, int openFlags);

    /// Close a previously opened chunk and release resources.
    /// @param[in] chunkId id of the chunk being closed.
    void	CloseChunk(kfsChunkId_t chunkId);

    /// Schedule a read on a chunk.
    /// @param[in] op  The read operation being scheduled.
    /// @retval 0 if op was successfully scheduled; -1 otherwise
    int		ReadChunk(ReadOp *op);

    /// Schedule a write on a chunk.
    /// @param[in] op  The write operation being scheduled.
    /// @retval 0 if op was successfully scheduled; -1 otherwise
    int		WriteChunk(WriteOp *op);

    /// A previously scheduled write op just finished.  Update chunk
    /// size and the amount of used space.
    /// @param[in] op  The write op that just finished
    ///
    void	WriteChunkDone(WriteOp *op);
    void	ReadChunkDone(ReadOp *op);
    void	ReplicationDone(kfsChunkId_t chunkId);
    /// Determine the size of a chunk.
    /// @param[in] chunkId  The chunk whose size is needed
    /// @param[out] chunkSize  The size of the chunk
    /// @retval status code
    int 	ChunkSize(kfsChunkId_t chunkId, size_t *chunkSize);

    /// Cancel a previously scheduled chunk operation.
    /// @param[in] cont   The callback object that scheduled the
    ///  operation
    /// @param[in] chunkId  The chunk on which ops were scheduled
    void 	CancelChunkOp(KfsCallbackObj *cont, kfsChunkId_t chunkId);

    /// Register a timeout handler with the net manager for taking
    /// checkpoints.  Also, get the logger going
    void	Start();
    
    /// Write out the chunk table data structure to disk
    void	Checkpoint();

    /// Read the chunk table from disk following a restart.  See
    /// comments in the method for issues relating to validation (such
    /// as, checkpoint contains a chunk name, but the associated file
    /// is not there on disk, etc.).
    void	Restart();

    /// On a restart following a dirty shutdown, do log replay.  This
    /// involves updating the Chunk table map to reflect operations
    /// that are in the log.

    /// When a checkpoint file is read, update the mChunkTable[] to
    /// include a mapping for cih->chunkInfo.chunkId.
    void AddMapping(ChunkInfoHandle_t *cih);

    /// Replay a chunk allocation.
    /// 
    /// @param[in] fileId  id of the file that has chunk chunkId
    /// @param[in] chunkId  Update the mChunkTable[] to include this
    /// chunk id
    /// @param[in] chunkVersion  the version assigned by the
    /// metaserver to this chunk. 
    void ReplayAllocChunk(kfsFileId_t fileId, kfsChunkId_t chunkId,
                          int64_t chunkVersion);

    /// Replay a chunk version # change.
    /// 
    /// @param[in] fileId  id of the file that has chunk chunkId
    /// @param[in] chunkId  Update the mChunkTable[] with the changed
    /// version # for this chunkId
    /// @param[in] chunkVersion  the version assigned by the
    /// metaserver to this chunk. 
    void ReplayChangeChunkVers(kfsFileId_t fileId, kfsChunkId_t chunkId,
                               int64_t chunkVersion);

    /// Replay a chunk deletion
    /// @param[in] chunkId  Update the mChunkTable[] to remove this
    /// chunk id
    void ReplayDeleteChunk(kfsChunkId_t chunkId);


    /// Replay a write done on a chunk.
    /// @param[in] chunkId  Update the size of chunk to reflect the
    /// completion of a write.
    /// @param[in] chunkSize The new size of the chunk
    void ReplayWriteDone(kfsChunkId_t chunkId, size_t chunkSize,
                         off_t offset, std::vector<uint32_t> checksum);

    /// Replay a truncation done on a chunk.
    /// @param[in] chunkId  Update the size of chunk to reflect the
    /// completion of a truncation
    /// @param[in] chunkSize The new size of the chunk
    void ReplayTruncateDone(kfsChunkId_t chunkId, size_t chunkSize);
    
    /// Retrieve the chunks hosted on this chunk server.
    /// @param[out] result  A vector containing info of all chunks
    /// hosted on this server.
    void GetHostedChunks(std::vector<ChunkInfo_t> &result);

    /// Return the total space that is exported by this server.  If
    /// chunks are stored in a single directory, we use statvfs to
    /// determine the total space avail; we report the min of statvfs
    /// value and the configured mTotalSpace.
    int64_t GetTotalSpace() const;
    int64_t GetUsedSpace() const { return mUsedSpace; };

    /// For a write, the client has pushed data to us.  This is queued
    /// for a commit later on.
    /// @param[in] wp  The op that needs to be queued
    /// @retval status code
    int EnqueueWrite(WritePrepareOp *wp);

    /// Check if a write is pending to a chunk.
    /// @param[in] chunkId  The chunkid for which we are checking for
    /// pending write(s). 
    /// @retval True if a write is pending; false otherwise
    bool IsWritePending(kfsChunkId_t chunkId);

    /// Given a chunk id, return its version
    int64_t GetChunkVersion(kfsChunkId_t c);

    /// Retrieve the write op given a write id.
    /// @param[in] writeId  The id corresponding to a previously
    /// enqueued write.
    /// @retval WriteOp if one exists; NULL otherwise
    WriteOp *GetWriteOp(int64_t writeId);

    void Timeout() {
        Checkpoint();
        // if any writes have been around for "too" long, remove them
        // and reclaim memory
        ScavengePendingWrites();
        // cleanup inactive fd's and thereby free up fd's
        CleanupInactiveFds();
    };

    /// Push the changes from the write out to disk
    int Sync(WriteOp *op);

private:
    /// How long should a pending write be held in LRU
    static const int MAX_PENDING_WRITE_LRU_SECS = 300;

    /// space available for allocation 
    int64_t	mTotalSpace;
    /// how much is used up by chunks
    int64_t	mUsedSpace;
    
    /// directories for storing the chunks
    std::vector<std::string> mChunkDirs;

    /// See the comments in KfsOps.cc near WritePreapreOp related to write handling
    int64_t mWriteId;
    std::list<WriteOp *> mPendingWrites;

    /// on a timeout, the timeout interface will force a checkpoint
    ChunkManagerTimeoutImpl	*mChunkManagerTimeoutImpl;

    /// when taking checkpoings, write one out only if the chunk table
    /// is dirty. 
    bool mIsChunkTableDirty;
    /// table that maps chunkIds to their associated state
    CMap	mChunkTable;

    /// Utility function that given a chunkId, returns the full path
    /// to the chunk filename.
    std::string MakeChunkPathname(kfsChunkId_t chunkId);
    std::string MakeChunkPathname(const char *chunkId);

    /// Utility function that given a chunkId, returns the full path
    /// to the chunk filename in the "stalechunks" dir
    std::string MakeStaleChunkPathname(kfsChunkId_t chunkId);

    /// Utility function that sets up a disk connection for an
    /// I/O operation on a chunk.
    /// @param[in] chunkId  Id of the chunk on which we are doing I/O
    /// @param[in] op   The KfsOp that is being on the chunk
    /// @retval A disk connection pointer allocated via a call to new;
    /// it is the caller's responsibility to free the memory
    DiskConnection *SetupDiskConnection(kfsChunkId_t chunkId, KfsOp *op);

    /// Utility function that returns a pointer to mChunkTable[chunkId].
    /// @param[in] chunkId  the chunk id for which we want info
    /// @param[out] cih  the resulting pointer from mChunkTable[chunkId]
    /// @retval  0 on success; -EBADF if we can't find mChunkTable[chunkId]
    int GetChunkInfoHandle(kfsChunkId_t chunkId, ChunkInfoHandle_t **cih);

    /// Checksums are computed on 64K blocks.  To verify checksums on
    /// reads, reads are aligned at 64K boundaries and data is read in
    /// 64K blocks.  So, for reads that are un-aligned/read less data,
    /// adjust appropriately.
    void AdjustDataRead(ReadOp *op);

    /// Pad the buffer with sufficient 0's so that checksumming works
    /// out.
    /// @param[in/out] buffer  The buffer to be padded with 0's
    void ZeroPad(IOBuffer *buffer);

    /// Given a chunkId and offset, return the checksum of corresponding
    /// "checksum block"---i.e., the 64K block that contains offset.
    uint32_t GetChecksum(kfsChunkId_t chunkId, off_t offset);

    /// For any writes that have been held for more than 2 mins,
    /// scavenge them and reclaim memory.
    void ScavengePendingWrites();

    /// If we have too many open fd's close out whatever we can.
    void CleanupInactiveFds();

    /// Notify the metaserver that chunk chunkId is corrupted; the
    /// metaserver will re-replicate this chunk and for now, won't
    /// send us traffic for this chunk.
    void NotifyMetaCorruptedChunk(kfsChunkId_t chunkId);

    /// Get all the chunk filenames into a single array.
    /// @retval on success, # of entries in the array;
    ///         on failures, -1
    int GetChunkDirsEntries(struct dirent ***namelist);
};

/// A Timeout interface object for taking checkpoints on the
/// ChunkManager object.
class ChunkManagerTimeoutImpl : public ITimeout {
public:
    ChunkManagerTimeoutImpl(ChunkManager *mgr) {
        mChunkManager = mgr; 
        // set a checkpoint once every min.
        SetTimeoutInterval(60*1000);
    };
    /// On a timeout, force a checkpoint
    void Timeout() {
        mChunkManager->Timeout();
    };
private:
    /// Owning chunk manager
    ChunkManager	*mChunkManager;
};

extern ChunkManager gChunkManager;

/// Given a partition that holds chunks, get the path to the directory
/// that is used to keep the stale chunks (from this partition)
std::string GetStaleChunkPath(const std::string &partition);

}

#endif // _CHUNKMANAGER_H
