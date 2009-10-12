//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/28
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
#include "ChunkServer.h"
#include "MetaServerSM.h"
#include "LeaseClerk.h"
#include "Utils.h"
#include "Logger.h"
#include "DiskIo.h"

#include "libkfsIO/Counter.h"
#include "libkfsIO/Checksum.h"
#include "libkfsIO/Globals.h"
#include "qcdio/qcdllist.h"

#include <fstream>
#include <sstream>
#include <algorithm>
#include <string>
#include <set>

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
using std::set;

using namespace KFS::libkfsio;

namespace KFS
{

ChunkManager gChunkManager;
typedef QCDLList<ChunkInfoHandle, 0> ChunkLru;
typedef QCDLList<ChunkInfoHandle, 1> PendingMetaSyncQueue;

inline bool ChunkManager::IsInLru(const ChunkInfoHandle& cih) const {
    return ChunkLru::IsInList(mChunkInfoLists, cih);
}

/// Encapsulate a chunk file descriptor and information about the
/// chunk such as name and version #.
class ChunkInfoHandle : public KfsCallbackObj
{
public:
    ChunkInfoHandle()
        : KfsCallbackObj(),
          lastIOTime(0),
          readChunkMetaOp(NULL),
          isBeingReplicated(false),
          createFile(false),
          mMetaSyncPending(false),
          mMetaSyncInFlight(false),
          mDeleteFlag(false)
    {
        ChunkLru::Init(*this);
        PendingMetaSyncQueue::Init(*this);
    }
    void Delete(ChunkInfoHandle** chunkInfoLists)
    {
        ChunkLru::Remove(chunkInfoLists, *this);
        PendingMetaSyncQueue::Remove(chunkInfoLists, *this);
        if (mMetaSyncInFlight) {
            mDeleteFlag = true;
        } else {
            delete this;
        }
    }

    struct ChunkInfo_t chunkInfo;
    /// Chunks are stored as files in he underlying filesystem; each
    /// chunk file is named by the chunkId.  Each chunk has a header;
    /// this header is hidden from clients; all the client I/O is
    /// offset by the header amount
    DiskIo::FilePtr dataFH;
    time_t lastIOTime;  // when was the last I/O done on this chunk

    /// keep track of the op that is doing the read
    ReadChunkMetaOp *readChunkMetaOp;

    void Release(ChunkInfoHandle** chunkInfoLists) {
        chunkInfo.UnloadChecksums();
        if (! IsFileOpen()) {
            return;
        }
        std::string errMsg;
        if (! dataFH->Close(
                chunkInfo.chunkSize + KFS_CHUNK_HEADER_SIZE,
                &errMsg)) {
            KFS_LOG_STREAM_ERROR <<
                "chunk " << chunkInfo.chunkId << " close error: " << errMsg <<
            KFS_LOG_EOM;
            dataFH.reset();
        }
        ChunkLru::Remove(chunkInfoLists, *this);
        PendingMetaSyncQueue::Remove(chunkInfoLists, *this);
        libkfsio::globals().ctrOpenDiskFds.Update(-1);
    }

    bool IsFileOpen() const {
        return (dataFH && dataFH->IsOpen());
    }

    bool IsFileInUse() const {
        return (IsFileOpen() && ! dataFH.unique());
    }

    bool SyncMeta();
    void LruUpdate(ChunkInfoHandle** chunkInfoLists) {
        lastIOTime = globals().netManager.Now();
        if (! isBeingReplicated && ! mMetaSyncPending) {
            ChunkLru::PushBack(chunkInfoLists, *this);
            assert(gChunkManager.IsInLru(*this));
        } else {
            ChunkLru::Remove(chunkInfoLists, *this);
            assert(! gChunkManager.IsInLru(*this));
        }
    }
    void ScheduleSyncMeta(ChunkInfoHandle** chunkInfoLists) {
        mMetaSyncPending = true;
        LruUpdate(chunkInfoLists); // pretent that we've scheduled io
        PendingMetaSyncQueue::PushBack(chunkInfoLists, *this);
    }
    void SetMetaWriteInFlight(ChunkInfoHandle** chunkInfoLists, KfsOp* op) {
        mMetaSyncPending = false;
        PendingMetaSyncQueue::Remove(chunkInfoLists, *this);
        mMetaSyncInFlight = op->clnt == this;
        LruUpdate(chunkInfoLists);
    }

    bool isBeingReplicated:1;  // is the chunk being replicated from
                               // another server
    bool createFile:1;
private:
    bool             mMetaSyncPending:1;
    bool             mMetaSyncInFlight:1;
    bool             mDeleteFlag:1;
    ChunkInfoHandle* mPrevPtr[ChunkManager::kChunkInfoHandleListCount];
    ChunkInfoHandle* mNextPtr[ChunkManager::kChunkInfoHandleListCount];

    int HandleChunkMetaWriteDone(int code, void *data);
    virtual ~ChunkInfoHandle() {
        if (mMetaSyncInFlight) {
            // Object is the "client" of this op.
            die("attempt to delete chunk info handle "
                "with meta data write in flight");
        }
    }
    friend class QCDLListOp<ChunkInfoHandle, 0>;
    friend class QCDLListOp<ChunkInfoHandle, 1>;
private:
    ChunkInfoHandle(const  ChunkInfoHandle&);
    ChunkInfoHandle& operator=(const  ChunkInfoHandle&);
};

inline void ChunkManager::LruUpdate(ChunkInfoHandle& cih) {
    cih.LruUpdate(mChunkInfoLists);
}

inline void ChunkManager::Release(ChunkInfoHandle& cih) {
    cih.Release(mChunkInfoLists);
}

inline void ChunkManager::Delete(ChunkInfoHandle& cih) {
    cih.Delete(mChunkInfoLists);
}

int
ChunkInfoHandle::HandleChunkMetaWriteDone(int code, void *data)
{
    mMetaSyncInFlight = false;
    if (mDeleteFlag) {
        delete this;
        return 0;
    }
    int status;
    if (data && (status = *reinterpret_cast<int*>(data)) < 0) {
        KFS_LOG_STREAM_INFO <<
            "failed to sync meta for chunk " << chunkInfo.chunkId <<
            " status: " << status <<
        KFS_LOG_EOM;
        if (! isBeingReplicated &&
                ! gChunkManager.IsWritePending(chunkInfo.chunkId)) {
            gChunkManager.ChunkIOFailed(chunkInfo.chunkId, -EIO);
            // "this" might have already been deleted here
            return 0;
        }
    }
    gChunkManager.LruUpdate(*this);
    return 0;
}

bool
ChunkInfoHandle::SyncMeta()
{
    if (! mMetaSyncPending || mMetaSyncInFlight ||
            isBeingReplicated || ! IsFileOpen()) {
        return mMetaSyncInFlight;
    }
    int     freeRequestCount;
    int     requestCount;
    int64_t readBlockCount;
    int64_t writeBlockCount;
    int     blockSize;
    dataFH->GetDiskQueuePendingCount(freeRequestCount, requestCount,
        readBlockCount, writeBlockCount, blockSize);
    if (freeRequestCount <= 0 || requestCount * 2 > freeRequestCount ||
            int64_t(writeBlockCount) * blockSize > (int64_t(128) << 20)) {
        KFS_LOG_STREAM_INFO << "deferring chunk meta data sync for: " <<
            chunkInfo.chunkId <<
            " requests: " << requestCount <<
            " write blocks: " << writeBlockCount <<
        KFS_LOG_EOM;
        return true;
    }
    SET_HANDLER(this, &ChunkInfoHandle::HandleChunkMetaWriteDone);
    int status = gChunkManager.WriteChunkMetadata(chunkInfo.chunkId, this);
    if (status < 0) {
        HandleChunkMetaWriteDone(EVENT_CMD_DONE, &status);
        // "this" might have already been deleted here
        return true;
    }
    return mMetaSyncInFlight;
}

/// A Timeout interface object for taking checkpoints on the
/// ChunkManager object.
class ChunkManager::ChunkManagerTimeoutImpl : public ITimeout {
public:
    ChunkManagerTimeoutImpl(ChunkManager *mgr) : mTimeoutOp(0) {
        mChunkManager = mgr; 
        // set a checkpoint once every min.
        // SetTimeoutInterval(60*1000);
    };
    virtual void Timeout() {
        SubmitOp(&mTimeoutOp);
    }
private:
    /// Owning chunk manager
    ChunkManager	*mChunkManager;
    TimeoutOp		mTimeoutOp;
};

static int
GetMaxOpenFds()
{
    struct rlimit rlim;
    int maxOpenFds = 0;

    if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
        maxOpenFds = rlim.rlim_cur;
        // bump the soft limit to the hard limit
        rlim.rlim_cur = rlim.rlim_max;
        if (setrlimit(RLIMIT_NOFILE, &rlim) == 0) {
            KFS_LOG_STREAM_DEBUG <<
                "setting # of open files to: " << rlim.rlim_cur <<
            KFS_LOG_EOM;
            maxOpenFds = rlim.rlim_cur;
        }
    }
    return maxOpenFds;
}

ChunkManager::ChunkManager()
{
    mTotalSpace = mUsedSpace = 0;
    mNumChunks = 0;
    mChunkManagerTimeoutImpl = new ChunkManagerTimeoutImpl(this);
    // we want a timeout once in 10 secs
    // mChunkManagerTimeoutImpl->SetTimeoutInterval(10 * 1000);
    mIsChunkTableDirty = false;
    mLastDriveChosen = -1;
    time_t now = time(NULL);
    srand48(now);
    mMaxOpenChunkFiles = 1 << 10;
    mMaxIORequestSize = 4 << 20;
    ChunkLru::Init(mChunkInfoLists);
    PendingMetaSyncQueue::Init(mChunkInfoLists);
    mNextPendingMetaSyncScanTime = 0;
    mMetaSyncDelayTimeSecs = 5;
    mNextInactiveFdCleanupTime = 0;
    mInactiveFdsCleanupIntervalSecs = 300;
    mNextInactiveFdCleanupTime = 0;
    mMaxPendingWriteLruSecs = 300;
    mNextCheckpointTime = 0;
    mCheckpointIntervalSecs = 120;
    // check once every 6 hours
    mNextChunkDirsCheckTime = 0;
    mChunkDirsCheckIntervalSecs = 6 * 3600;
}

ChunkManager::~ChunkManager()
{
    assert(mChunkTable.empty() && ! mChunkManagerTimeoutImpl);
}

void
ChunkManager::Shutdown()
{
    ScavengePendingWrites(time(0) + 2 * mMaxPendingWriteLruSecs);
    for (; ;) {
        for (CMI iter = mChunkTable.begin(); iter != mChunkTable.end(); ) {
            ChunkInfoHandle * const cih = iter->second;
            if (cih->IsFileInUse()) {
                break;
            }
            mChunkTable.erase(iter++);
            Release(*cih);
            Delete(*cih);
        }
        if (mChunkTable.empty()) {
            break;
        }
        DiskIo::RunIoCompletion();
    }
    globals().netManager.UnRegisterTimeoutHandler(mChunkManagerTimeoutImpl);
    delete mChunkManagerTimeoutImpl;
    mChunkManagerTimeoutImpl = 0;
    string errMsg;
    if (! DiskIo::Shutdown(&errMsg)) {
        KFS_LOG_STREAM_INFO <<
            "DiskIo::Shutdown falure: " << errMsg <<
        KFS_LOG_EOM;
    }
}

bool 
ChunkManager::Init(const vector<string> &chunkDirs, int64_t totalSpace, const Properties& prop)
{
    mInactiveFdsCleanupIntervalSecs = prop.getValue(
        "chunkServer.inactiveFdsCleanupIntervalSecs",
        mInactiveFdsCleanupIntervalSecs);
    mMetaSyncDelayTimeSecs = std::max(1, prop.getValue(
        "chunkServer.metaSyncDelayTimeSecs",
        mMetaSyncDelayTimeSecs));
    mMaxPendingWriteLruSecs = std::max(1, prop.getValue(
        "chunkServer.maxPendingWriteLruSecs",
        mMaxPendingWriteLruSecs));
    mCheckpointIntervalSecs = std::max(1, prop.getValue(
        "chunkServer.checkpointIntervalSecs",
        mCheckpointIntervalSecs));
    mChunkDirsCheckIntervalSecs = std::max(1, prop.getValue(
        "chunkServer.chunkDirsCheckIntervalSecs",
        mChunkDirsCheckIntervalSecs));

    mTotalSpace = totalSpace;
    for (uint32_t i = 0; i < chunkDirs.size(); i++) {
        ChunkDirInfo_t c;

        c.dirname = chunkDirs[i];
        mChunkDirs.push_back(c);
    }
    string errMsg;
    if (! DiskIo::Init(prop, &errMsg)) {
        KFS_LOG_STREAM_ERROR <<
            "DiskIo::Init failure: " << errMsg <<
        KFS_LOG_EOM;
        return false;
    }
    mMaxOpenChunkFiles = std::max(128, std::min(
        GetMaxOpenFds() / DiskIo::GetFdCountPerFile(),
        prop.getValue("chunkServer.maxOpenChunkFiles", 64 << 10)));
    // force a stat of the dirs and update space usage counts
    return (GetTotalSpace(true) >= 0);
}

int
ChunkManager::AllocChunk(kfsFileId_t fileId, kfsChunkId_t chunkId, 
                         kfsSeq_t chunkVersion,
                         bool isBeingReplicated)
{
    string chunkdir;
    ChunkInfoHandle *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    mIsChunkTableDirty = true;

    if (tableEntry != mChunkTable.end()) {
        cih = tableEntry->second;
        ChangeChunkVers(cih->chunkInfo.fileId, cih->chunkInfo.chunkId, chunkVersion);
        return 0;
    }

    // Find the directory to use
    chunkdir = GetDirForChunk();
    if (chunkdir == "") {
        KFS_LOG_STREAM_INFO <<
            "No directory has space to host chunk " << chunkId <<
        KFS_LOG_EOM;
        return -ENOSPC;
    }
        
    const string s = MakeChunkPathname(chunkdir, fileId, chunkId, chunkVersion);
    KFS_LOG_STREAM_INFO << "Creating chunk: " << s << KFS_LOG_EOM;

    CleanupInactiveFds();
    mNumChunks++;

    cih = new ChunkInfoHandle();
    cih->chunkInfo.Init(fileId, chunkId, chunkVersion);
    cih->chunkInfo.SetDirname(chunkdir);
    cih->isBeingReplicated = isBeingReplicated;
    cih->createFile = true;
    mChunkTable[chunkId] = cih;
    return 0;
}

int
ChunkManager::DeleteChunk(kfsChunkId_t chunkId)
{
    string s;
    ChunkInfoHandle *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) 
        return -EBADF;

    mIsChunkTableDirty = true;

    cih = tableEntry->second;

    s = MakeChunkPathname(cih);
    unlink(s.c_str());

    KFS_LOG_STREAM_INFO << "Deleting chunk: " << s << KFS_LOG_EOM;

    mNumChunks--;
    assert(mNumChunks >= 0);
    if (mNumChunks < 0)
        mNumChunks = 0;

    UpdateDirSpace(cih, -cih->chunkInfo.chunkSize);

    mUsedSpace -= cih->chunkInfo.chunkSize;
    mPendingWrites.Delete(chunkId, cih->chunkInfo.chunkVersion);
    mChunkTable.erase(chunkId);
    Delete(*cih);
    return 0;
}

void
ChunkManager::DumpChunkMap()
{
    ChunkInfoHandle *cih;
    ofstream ofs;

    ofs.open("chunkdump.txt");
    // Dump chunk map in the format of
    // chunkID fileID chunkSize
    for (CMI tableEntry = mChunkTable.begin(); tableEntry != mChunkTable.end();
         ++tableEntry) {
        cih = tableEntry->second;
        ofs << cih->chunkInfo.chunkId << " " << cih->chunkInfo.fileId << " " << cih->chunkInfo.chunkSize << endl;
    }

    ofs.flush();
    ofs.close();
}

void
ChunkManager::DumpChunkMap(ostringstream &ofs)
{
   ChunkInfoHandle *cih;

   // Dump chunk map in the format of
   // chunkID fileID chunkSize
   for (CMI tableEntry = mChunkTable.begin(); tableEntry != mChunkTable.end();
       ++tableEntry) {
       cih = tableEntry->second;
       ofs << cih->chunkInfo.chunkId << " " << cih->chunkInfo.fileId << " " << cih->chunkInfo.chunkSize << endl;
   }
}

int
ChunkManager::WriteChunkMetadata(kfsChunkId_t chunkId, KfsCallbackObj *cb)
{
    CMI tableEntry = mChunkTable.find(chunkId);
    int res;

    if (tableEntry == mChunkTable.end()) 
        return -EBADF;

    ChunkInfoHandle *cih = tableEntry->second;
    WriteChunkMetaOp *wcm = new WriteChunkMetaOp(chunkId, cb);
    DiskIo *d = SetupDiskIo(chunkId, wcm);
    if (d == NULL) {
        delete wcm;
        return -KFS::ESERVERBUSY;
    }

    wcm->diskIo.reset(d);
    wcm->dataBuf = new IOBuffer();
    cih->chunkInfo.Serialize(wcm->dataBuf);
    wcm->dataBuf->ZeroFillLast();
    const size_t numBytes = wcm->dataBuf->BytesConsumable();
    if (KFS_CHUNK_HEADER_SIZE < numBytes) {
        die("bad io buffer size");
    }
    LruUpdate(*cih);

    res = wcm->diskIo->Write(0, numBytes, wcm->dataBuf);
    if (res < 0) {
        delete wcm;
    } else {
        cih->SetMetaWriteInFlight(mChunkInfoLists, wcm);
    }
    return res >= 0 ? 0 : res;
}

int
ChunkManager::ScheduleWriteChunkMetadata(kfsChunkId_t chunkId)
{
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) 
        return -EBADF;
    ChunkInfoHandle *cih = tableEntry->second;
    cih->ScheduleSyncMeta(mChunkInfoLists);
    return 0;
}

int
ChunkManager::ReadChunkMetadata(kfsChunkId_t chunkId, KfsOp *cb)
{
    CMI tableEntry = mChunkTable.find(chunkId);
    int res = 0;

    if (tableEntry == mChunkTable.end()) 
        return -EBADF;

    ChunkInfoHandle *cih = tableEntry->second;

    LruUpdate(*cih);
    if (cih->chunkInfo.AreChecksumsLoaded())
        return cb->HandleEvent(EVENT_CMD_DONE, (void *) &res);

    if (cih->readChunkMetaOp) {
        // if we have issued a read request for this chunk's metadata,
        // don't submit another one; otherwise, we will simply drive
        // up memory usage for useless IO's
        cih->readChunkMetaOp->AddWaiter(cb);
        return 0;
    }

    ReadChunkMetaOp *rcm = new ReadChunkMetaOp(chunkId, cb);
    DiskIo *d = SetupDiskIo(chunkId, rcm);
    if (d == NULL) {
        delete rcm;
        return -KFS::ESERVERBUSY;
    }
    rcm->diskIo.reset(d);

    res = rcm->diskIo->Read(0, KFS_CHUNK_HEADER_SIZE);
    if (res < 0) {
        delete rcm;
        return res;
    }
    cih->readChunkMetaOp = rcm;
    return 0;
}

void
ChunkManager::ReadChunkMetadataDone(kfsChunkId_t chunkId)
{
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) 
        return;

    ChunkInfoHandle *cih = tableEntry->second;

    LruUpdate(*cih);
    cih->readChunkMetaOp = 0;
}

int
ChunkManager::SetChunkMetadata(const DiskChunkInfo_t &dci, kfsChunkId_t chunkId)
{
    ChunkInfoHandle *cih;
    int res;

    if ((res = dci.Validate(chunkId)) < 0) {
        KFS_LOG_STREAM_ERROR <<
            "chunk metadata validation mismatch on chunk " << chunkId <<
        KFS_LOG_EOM;
        NotifyMetaCorruptedChunk(chunkId);
        StaleChunk(chunkId);
        return res;
    }

    if (GetChunkInfoHandle(dci.chunkId, &cih) < 0)
        return -EBADF;

    cih->chunkInfo.SetChecksums(dci.chunkBlockChecksum);
    if (cih->chunkInfo.chunkSize > dci.chunkSize) {
        const off_t extra = cih->chunkInfo.chunkSize - dci.chunkSize;
        mUsedSpace -= extra;
        UpdateDirSpace(cih, -extra);
        cih->chunkInfo.chunkSize = dci.chunkSize;
    }

    return 0;
}

bool
ChunkManager::IsChunkMetadataLoaded(kfsChunkId_t chunkId)
{
    ChunkInfoHandle *cih = NULL;

    if (GetChunkInfoHandle(chunkId, &cih) < 0)
        return false;
    return cih->chunkInfo.AreChecksumsLoaded();
}

int
ChunkManager::GetChunkChecksums(kfsChunkId_t chunkId, uint32_t **checksums)
{
    ChunkInfoHandle *cih = NULL;

    if (GetChunkInfoHandle(chunkId, &cih) < 0)
        return -EBADF;
    *checksums = cih->chunkInfo.chunkBlockChecksum;
    return 0;
}

void
ChunkManager::MarkChunkStale(ChunkInfoHandle *cih)
{
    string s = MakeChunkPathname(cih);
    string staleChunkPathname = MakeStaleChunkPathname(cih);
    
    rename(s.c_str(), staleChunkPathname.c_str());
    KFS_LOG_STREAM_INFO <<
        "Moving chunk " << cih->chunkInfo.chunkId << " to staleChunks dir" <<
    KFS_LOG_EOM;
}

int
ChunkManager::StaleChunk(kfsChunkId_t chunkId)
{
    ChunkInfoHandle *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) 
        return -EBADF;

    mIsChunkTableDirty = true;

    cih = tableEntry->second;

    MarkChunkStale(cih);

    mNumChunks--;
    assert(mNumChunks >= 0);
    if (mNumChunks < 0)
        mNumChunks = 0;

    UpdateDirSpace(cih, -cih->chunkInfo.chunkSize);
    
    mUsedSpace -= cih->chunkInfo.chunkSize;
    mChunkTable.erase(chunkId);
    Delete(*cih);
    return 0;
}

int
ChunkManager::TruncateChunk(kfsChunkId_t chunkId, off_t chunkSize)
{
    string chunkPathname;
    ChunkInfoHandle *cih;
    uint32_t lastChecksumBlock;
    CMI tableEntry = mChunkTable.find(chunkId);

    // the truncated size should not exceed chunk size.
    if (chunkSize > (off_t) KFS::CHUNKSIZE)
        return -EINVAL;

    if (tableEntry == mChunkTable.end())
        return -EBADF;

    mIsChunkTableDirty = true;

    cih = tableEntry->second;
    chunkPathname = MakeChunkPathname(cih);

    // Cnunk close will truncate it to the cih->chunkInfo.chunkSize

    UpdateDirSpace(cih, -cih->chunkInfo.chunkSize);

    mUsedSpace -= cih->chunkInfo.chunkSize;
    mUsedSpace += chunkSize;
    cih->chunkInfo.chunkSize = chunkSize;

    UpdateDirSpace(cih, cih->chunkInfo.chunkSize);

    lastChecksumBlock = OffsetToChecksumBlockNum(chunkSize);

    // XXX: Could do better; recompute the checksum for this last block
    cih->chunkInfo.chunkBlockChecksum[lastChecksumBlock] = 0;

    return 0;
}

int
ChunkManager::ChangeChunkVers(kfsFileId_t fileId,
                              kfsChunkId_t chunkId, int64_t chunkVersion)
{
    string chunkPathname;
    ChunkInfoHandle *cih;
    CMI tableEntry = mChunkTable.find(chunkId);
    string oldname, newname, s;

    if (tableEntry == mChunkTable.end()) {
        return -1;
    }

    cih = tableEntry->second;
    oldname = MakeChunkPathname(cih);

    mIsChunkTableDirty = true;

    KFS_LOG_STREAM_INFO <<
        "Chunk " << oldname << " already exists; changing version # " <<
        " from << " << cih->chunkInfo.chunkVersion << " to " << chunkVersion <<
        " Checksums are " <<
            (cih->chunkInfo.AreChecksumsLoaded() ? "" : "not ") << "loaded" <<
    KFS_LOG_EOM;        

    mPendingWrites.Delete(chunkId, cih->chunkInfo.chunkVersion);
    cih->chunkInfo.chunkVersion = chunkVersion;

    newname = MakeChunkPathname(cih);

    rename(oldname.c_str(), newname.c_str());

    return 0;
}

void
ChunkManager::ReplicationDone(kfsChunkId_t chunkId)
{
    ChunkInfoHandle *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) {
        return;
    }

    cih = tableEntry->second;

#ifdef DEBUG
    string chunkPathname = MakeChunkPathname(cih);
    KFS_LOG_STREAM_DEBUG <<
        "Replication for chunk %s is complete..." << chunkPathname <<
    KFS_LOG_EOM;
#endif

    mIsChunkTableDirty = true;
    cih->isBeingReplicated = false;
    LruUpdate(*cih); // Add it to lru.
}

void
ChunkManager::Start()
{
    globals().netManager.RegisterTimeoutHandler(mChunkManagerTimeoutImpl);
}

void
ChunkManager::UpdateDirSpace(ChunkInfoHandle *cih, off_t nbytes)
{
    for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
        if (mChunkDirs[i].dirname == cih->chunkInfo.GetDirname()) {
            mChunkDirs[i].usedSpace += nbytes;
            if (mChunkDirs[i].usedSpace < 0)
                mChunkDirs[i].usedSpace = 0;
        }
    }
}

string
ChunkManager::GetDirForChunk()
{
    if (mChunkDirs.size() == 1)
        return mChunkDirs[0].dirname;

    // do weighted random, so that we can fill all drives
    off_t totalFreeSpace = 0;
    for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
        if ((mChunkDirs[i].availableSpace == 0) ||
            ((mChunkDirs[i].availableSpace - mChunkDirs[i].usedSpace) < (off_t) CHUNKSIZE)) {
            continue;
        }
        totalFreeSpace += (mChunkDirs[i].availableSpace - mChunkDirs[i].usedSpace);
    }

    bool found = false;
    int dirToUse;
    double bucketLow = 0.0, bucketHi = 0.0;
    double randVal = drand48();
    for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
        if ((mChunkDirs[i].availableSpace == 0) ||
            ((mChunkDirs[i].availableSpace - mChunkDirs[i].usedSpace) < (off_t) CHUNKSIZE)) {
            continue;
        }
        bucketHi = bucketLow + ((double) (mChunkDirs[i].availableSpace - mChunkDirs[i].usedSpace) / (double) totalFreeSpace);
        if ((bucketLow <= randVal) && (randVal <= bucketHi)) {
            dirToUse = i;
            found = true;
            break;
        }
        bucketLow = bucketHi;
    }

    if (!found) {
        // to account for rounding errors, if we didn't pick a drive, but some drive has space, use it.
        for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
            if ((mChunkDirs[i].availableSpace == 0) ||
                ((mChunkDirs[i].availableSpace - mChunkDirs[i].usedSpace) < (off_t) CHUNKSIZE)) {
                continue;
            }
            dirToUse = i;
            found = true;
            break;
        }
    }        

    if (!found) {
        KFS_LOG_INFO("All drives are full; dir allocation failed");
        return "";
    }

    mLastDriveChosen = dirToUse;
    return mChunkDirs[dirToUse].dirname;
}

string
ChunkManager::MakeChunkPathname(ChunkInfoHandle *cih)
{
    ostringstream os;

    os << cih->chunkInfo.GetDirname() << '/' << cih->chunkInfo.fileId << '.' << cih->chunkInfo.chunkId 
       << '.' << cih->chunkInfo.chunkVersion;
    return os.str();
}

string
ChunkManager::MakeChunkPathname(const string &chunkdir, kfsFileId_t fid, kfsChunkId_t chunkId, kfsSeq_t chunkVersion)
{
    ostringstream os;

    os << chunkdir << '/' << fid << '.' << chunkId << '.' << chunkVersion;
    return os.str();
}

string
ChunkManager::MakeStaleChunkPathname(ChunkInfoHandle *cih)
{
    ostringstream os;
    string staleChunkDir = GetStaleChunkPath(cih->chunkInfo.GetDirname());

    os << staleChunkDir << '/' << cih->chunkInfo.fileId << '.' << cih->chunkInfo.chunkId << '.' << cih->chunkInfo.chunkVersion;

    return os.str();
}

void
ChunkManager::MakeChunkInfoFromPathname(const string &pathname, off_t filesz, ChunkInfoHandle **result)
{
    string::size_type slash = pathname.rfind('/');
    ChunkInfoHandle *cih;

    if (slash == string::npos) {
        *result = NULL;
        return;
    }
    
    string chunkFn, dirname;
    vector<string> component;

    dirname.assign(pathname, 0, slash);
    chunkFn.assign(pathname, slash + 1, string::npos);
    split(component, chunkFn, '.');
    assert(component.size() == 3);

    chunkId_t chunkId = atoll(component[1].c_str());
    if (GetChunkInfoHandle(chunkId, &cih) == 0) {
        KFS_LOG_STREAM_INFO << "Duplicate chunk " << chunkId <<
            " with path: " << pathname << KFS_LOG_EOM;
        *result = NULL;
        return;
    }

    cih = new ChunkInfoHandle();    
    cih->chunkInfo.fileId = atoll(component[0].c_str());
    cih->chunkInfo.chunkId = atoll(component[1].c_str());
    cih->chunkInfo.chunkVersion = atoll(component[2].c_str());
    if (filesz >= (off_t) KFS_CHUNK_HEADER_SIZE)
        cih->chunkInfo.chunkSize = filesz - KFS_CHUNK_HEADER_SIZE;
    cih->chunkInfo.SetDirname(dirname);
    *result = cih;
    /*
    KFS_LOG_STREAM_DEBUG << "From " << chunkFn << " restored: " <<
        cih->chunkInfo.fileId << ", " << cih->chunkInfo.chunkId << ", " <<
        cih->chunkInfo.chunkVersion <<
    KFS_LOG_EOM;
    */
}

int
ChunkManager::OpenChunk(kfsChunkId_t chunkId, 
                        int openFlags)
{
    ChunkInfoHandle *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) {
        KFS_LOG_STREAM_DEBUG << "no such chunk: " << chunkId << KFS_LOG_EOM;
        return -EBADF;
    }
    cih = tableEntry->second;

    const string fn = MakeChunkPathname(cih);

    if (cih->IsFileOpen()) {
        return 0;
    }
    if (! cih->dataFH) {
        cih->dataFH.reset(new DiskIo::File());
    }
    string errMsg;
    const bool kReserveFileSpace = true;
    if (! cih->dataFH->Open(fn.c_str(),
            CHUNKSIZE + KFS_CHUNK_HEADER_SIZE,
            (openFlags & (O_WRONLY | O_RDWR)) == 0,
            kReserveFileSpace, cih->createFile, &errMsg)) {
        //
        // we are unable to open/create a file. notify the metaserver
        // of lost data so that it can re-replicate if needed.
        //
        NotifyMetaCorruptedChunk(chunkId);
        mChunkTable.erase(chunkId);        
        Delete(*cih);

        KFS_LOG_STREAM_ERROR <<
            "failed to open or create chunk file: " << fn << " :" << errMsg <<
        KFS_LOG_EOM;
        return -EBADF;
    }
    globals().ctrOpenDiskFds.Update(1);
    LruUpdate(*cih);

    // the checksums will be loaded async
    return 0;
}

void
ChunkManager::CloseChunk(kfsChunkId_t chunkId)
{
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) {
        return;
    }

    ChunkInfoHandle* const cih = tableEntry->second;
    // Close file if not in use.
    if (cih->IsFileOpen() && ! cih->IsFileInUse() && ! cih->SyncMeta()) {
        Release(*cih);
    }
}

int
ChunkManager::ChunkSize(kfsChunkId_t chunkId, kfsFileId_t &fid, off_t *chunkSize)
{
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(chunkId, &cih) < 0)
        return -EBADF;

    fid = cih->chunkInfo.fileId;
    *chunkSize = cih->chunkInfo.chunkSize;

    return 0;
}

int
ChunkManager::ReadChunk(ReadOp *op)
{
    ssize_t res;
    DiskIo *d;
    ChunkInfoHandle *cih;
    off_t offset;
    size_t numBytesIO;

    if (GetChunkInfoHandle(op->chunkId, &cih) < 0)
        return -EBADF;

    // provide the path to the client for telemetry
    op->driveName = cih->chunkInfo.dirname;

    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    if (op->chunkVersion != cih->chunkInfo.chunkVersion) {
        KFS_LOG_STREAM_INFO << "Version # mismatch (have=" <<
            cih->chunkInfo.chunkVersion << " vs asked=" << op->chunkVersion <<
            ")...failing a read" <<
        KFS_LOG_EOM;
        return -KFS::EBADVERS;
    }
    d = SetupDiskIo(op->chunkId, op);
    if (d == NULL)
        return -KFS::ESERVERBUSY;

    op->diskIo.reset(d);

    // schedule a read based on the chunk size
    if (op->offset >= cih->chunkInfo.chunkSize) {
        op->numBytesIO = 0;
    } else if ((off_t) (op->offset + op->numBytes) > cih->chunkInfo.chunkSize) {
        op->numBytesIO = cih->chunkInfo.chunkSize - op->offset;
    } else {
        op->numBytesIO = op->numBytes;
    }

    if (op->numBytesIO == 0)
        return -EIO;

    // for checksumming to work right, reads should be in terms of
    // checksum-blocks.
    offset = OffsetToChecksumBlockStart(op->offset);

    numBytesIO = OffsetToChecksumBlockEnd(op->offset + op->numBytesIO - 1) - offset;

    // Make sure we don't try to read past EOF; the checksumming will
    // do the necessary zero-padding. 
    if ((off_t) (offset + numBytesIO) > cih->chunkInfo.chunkSize)
        numBytesIO = cih->chunkInfo.chunkSize - offset;
    
    if ((res = op->diskIo->Read(offset + KFS_CHUNK_HEADER_SIZE, numBytesIO)) < 0)
        return -EIO;

    // read was successfully scheduled
    return 0;
}

int
ChunkManager::WriteChunk(WriteOp *op)
{
    ChunkInfoHandle *cih;
    int res;

    if (GetChunkInfoHandle(op->chunkId, &cih) < 0)
        return -EBADF;

    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();
    
    // schedule a write based on the chunk size.  Make sure that a
    // write doesn't overflow the size of a chunk.
    op->numBytesIO = min((size_t) (KFS::CHUNKSIZE - op->offset), op->numBytes);

    if (op->numBytesIO <= 0 || op->offset < 0)
        return -EINVAL;

    const int64_t addedBytes(op->offset + op->numBytesIO - cih->chunkInfo.chunkSize);
    if (addedBytes > 0 && mUsedSpace + addedBytes >= mTotalSpace) {
        KFS_LOG_STREAM_ERROR <<
            "out of disk space: " << mUsedSpace << " + " << addedBytes <<
            " = " << (mUsedSpace + addedBytes) << " >= " << mTotalSpace <<
        KFS_LOG_EOM;
	return -ENOSPC;
    }

    off_t   offset     = op->offset;
    ssize_t numBytesIO = op->numBytesIO;
    if ((OffsetToChecksumBlockStart(offset) == offset) &&
        ((size_t) numBytesIO >= (size_t) CHECKSUM_BLOCKSIZE)) {
        if (numBytesIO % CHECKSUM_BLOCKSIZE != 0) {
            assert(numBytesIO % CHECKSUM_BLOCKSIZE != 0);
            return -EINVAL;
        }
#if 0
        // checksum was computed when we got data from client..so, skip
        // Hopefully, common case: write covers an entire block and
        // so, we just compute checksum and get on with the write.
        op->checksums = ComputeChecksums(op->dataBuf, numBytesIO);
#endif
        if (!op->isFromReReplication &&
                op->checksums.size() == size_t(numBytesIO / CHECKSUM_BLOCKSIZE)) {
            assert(op->checksums[0] == op->wpop->checksum || op->checksums.size() > 1);
        } else {
            op->checksums = ComputeChecksums(op->dataBuf, numBytesIO);
        }
    } else {
        if ((size_t) numBytesIO >= (size_t) CHECKSUM_BLOCKSIZE) {
            assert((size_t) numBytesIO < (size_t) CHECKSUM_BLOCKSIZE);
            return -EINVAL;
        }

        op->checksums.clear();
        // The checksum block we are after is beyond the current
        // end-of-chunk.  So, treat that as a 0-block and splice in.
        if (OffsetToChecksumBlockStart(offset) >= cih->chunkInfo.chunkSize) {
            IOBuffer data;
            const int off(offset % CHECKSUM_BLOCKSIZE);
            data.ReplaceKeepBuffersFull(op->dataBuf, off, numBytesIO);
            data.ZeroFill(CHECKSUM_BLOCKSIZE - (off + numBytesIO));
            op->dataBuf->Move(&data);
        } else {
            // Need to read the data block over which the checksum is
            // computed. 
            if (op->rop == NULL) {
                // issue a read
                ReadOp *rop = new ReadOp(op, OffsetToChecksumBlockStart(offset),
                                         CHECKSUM_BLOCKSIZE);
                KFS_LOG_STREAM_DEBUG <<
                    "Write triggered a read for offset=" << offset <<
                KFS_LOG_EOM;
                op->rop = rop;

                rop->Execute();

                if (rop->status < 0) {
                    int res = rop->status;

                    op->rop = NULL;
                    rop->wop = NULL;
                    delete rop;
                    return res;
                }

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
            op->rop->dataBuf->ReplaceKeepBuffersFull(op->dataBuf,
                                     offset % CHECKSUM_BLOCKSIZE,
                                     numBytesIO);

            delete op->dataBuf;
            op->dataBuf = op->rop->dataBuf;
            op->rop->dataBuf = NULL;
            // If the buffer doesn't have a full CHECKSUM_BLOCKSIZE worth
            // of data, zero-pad the end.  We don't need to zero-pad the
            // front because the underlying filesystem will zero-fill when
            // we read a hole.
            ZeroPad(op->dataBuf);
        }

        assert(op->dataBuf->BytesConsumable() == (int) CHECKSUM_BLOCKSIZE);
        const uint32_t cksum = ComputeBlockChecksum(op->dataBuf, CHECKSUM_BLOCKSIZE);
        op->checksums.push_back(cksum);

        // Trim data at the buffer boundary from the beginning, to make write
        // offset close to where we were asked from.
        int off(offset % CHECKSUM_BLOCKSIZE);
        int numBytes(numBytesIO);
        offset -= off;
        op->dataBuf->TrimAtBufferBoundaryLeaveOnly(off, numBytes);
        offset += off;
        numBytesIO = numBytes;
    }

    DiskIo *d = SetupDiskIo(op->chunkId, op);
    if (d == NULL)
        return -KFS::ESERVERBUSY;

    op->diskIo.reset(d);

    /*
    KFS_LOG_STREAM_DEBUG <<
        "Checksum for chunk: " << op->chunkId << ", offset=" << op->offset <<
        ", bytes=" << op->numBytesIO << ", # of cksums=" << op->checksums.size() <<
    KFS_LOG_EOM;
    */

    res = op->diskIo->Write(offset + KFS_CHUNK_HEADER_SIZE, numBytesIO, op->dataBuf);
    if (res >= 0) {
        UpdateChecksums(cih, op);
        assert(res <= numBytesIO);
        res = std::min(res, int(op->numBytesIO));
        op->numBytesIO = numBytesIO;
    }
    return res;
}

void
ChunkManager::UpdateChecksums(ChunkInfoHandle *cih, WriteOp *op)
{
    mIsChunkTableDirty = true;

    off_t endOffset = op->offset + op->numBytesIO;

    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    for (vector<uint32_t>::size_type i = 0; i < op->checksums.size(); i++) {
        off_t offset = op->offset + i * CHECKSUM_BLOCKSIZE;
        uint32_t checksumBlock = OffsetToChecksumBlockNum(offset);

        cih->chunkInfo.chunkBlockChecksum[checksumBlock] = op->checksums[i];
    }

    if (cih->chunkInfo.chunkSize < endOffset) {

        UpdateDirSpace(cih, endOffset - cih->chunkInfo.chunkSize);

	mUsedSpace += endOffset - cih->chunkInfo.chunkSize;
        cih->chunkInfo.chunkSize = endOffset;

    }
    assert(0 <= mUsedSpace && mUsedSpace <= mTotalSpace);
}

void
ChunkManager::ReadChunkDone(ReadOp *op)
{
    ChunkInfoHandle *cih = NULL;
    
    if ((GetChunkInfoHandle(op->chunkId, &cih) < 0) ||
        (op->chunkVersion != cih->chunkInfo.chunkVersion)) {
        AdjustDataRead(op);
        if (cih) {
            KFS_LOG_STREAM_INFO << "Version # mismatch (have=" <<
                cih->chunkInfo.chunkVersion <<
                " vs asked=" << op->chunkVersion << ")" <<
            KFS_LOG_EOM;
        }
        op->status = -KFS::EBADVERS;
        return;
    }

    ZeroPad(op->dataBuf);

    assert(op->dataBuf->BytesConsumable() >= (int) CHECKSUM_BLOCKSIZE);

    // either nothing to verify or it better match

    bool mismatch = false;

    // figure out the block we are starting from and grab all the checksums
    vector<uint32_t>::size_type i, checksumBlock = OffsetToChecksumBlockNum(op->offset);
    op->checksum = ComputeChecksums(op->dataBuf, op->dataBuf->BytesConsumable());

    // the checksums should be loaded...
    if (!cih->chunkInfo.AreChecksumsLoaded()) {
        // the read took too long; the checksums got paged out.  ask the client to retry
        KFS_LOG_STREAM_INFO << "Checksums for chunk " <<
            cih->chunkInfo.chunkId  <<
            " got paged out; returning EAGAIN to client" <<
        KFS_LOG_EOM;
        op->status = -EAGAIN;
        return;
    }

    cih->chunkInfo.VerifyChecksumsLoaded();

    for (i = 0; i < op->checksum.size() &&
             checksumBlock < MAX_CHUNK_CHECKSUM_BLOCKS;
         checksumBlock++, i++) {
        if ((cih->chunkInfo.chunkBlockChecksum[checksumBlock] == 0) ||
            (op->checksum[i] == cih->chunkInfo.chunkBlockChecksum[checksumBlock])) {
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

    KFS_LOG_STREAM_ERROR <<
        "Checksum mismatch for chunk=" << op->chunkId <<
        " offset="    << op->offset <<
        " bytes="     << op->numBytesIO <<
        ": expect: "  << cih->chunkInfo.chunkBlockChecksum[checksumBlock] <<
        " computed: " << op->checksum[i] <<
    KFS_LOG_EOM;

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
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(chunkId, &cih) < 0) {
        KFS_LOG_STREAM_ERROR << 
            "Unable to notify metaserver of corrupt chunk: " << chunkId <<
        KFS_LOG_EOM;
        return;
    }

    KFS_LOG_STREAM_INFO <<
        "Notifying metaserver of lost/corrupt chunk (" <<  chunkId <<
        ") in file " << cih->chunkInfo.fileId <<
    KFS_LOG_EOM;

    // This op will get deleted when we get an ack from the metaserver
    CorruptChunkOp *ccop = new CorruptChunkOp(0, cih->chunkInfo.fileId, 
                                              chunkId);
    gMetaServerSM.EnqueueOp(ccop);
}

//
// directory with dirname is unaccessable; maybe drive failed.  so,
// notify metaserver of lost blocks.  the metaserver will then
// re-replicate.
//
void
ChunkManager::NotifyMetaChunksLost(const string &dirname)
{
    ChunkInfoHandle *cih;
    CMI iter = mChunkTable.begin();
    
    while (iter != mChunkTable.end()) {
        cih = iter->second;

        if (cih->chunkInfo.GetDirname() != dirname) {
            ++iter;
            continue;
        }
        KFS_LOG_STREAM_INFO <<
            "Notifying metaserver of lost chunk (" << cih->chunkInfo.chunkId <<
            ") in file " << cih->chunkInfo.fileId <<
            " in dir " << dirname <<
        KFS_LOG_EOM;
        // This op will get deleted when we get an ack from the metaserver
        CorruptChunkOp *ccop = new CorruptChunkOp(0, cih->chunkInfo.fileId, 
                                                  cih->chunkInfo.chunkId);
        gMetaServerSM.EnqueueOp(ccop);

        // get rid of chunkid from our list
        CMI prev = iter;
        ++iter;
        mChunkTable.erase(cih->chunkInfo.chunkId);
        Delete(*cih);
    }
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
    op->dataBuf->Consume(
        op->offset - OffsetToChecksumBlockStart(op->offset));
    op->dataBuf->Trim(op->numBytesIO);
}

uint32_t 
ChunkManager::GetChecksum(kfsChunkId_t chunkId, off_t offset)
{
    uint32_t checksumBlock = OffsetToChecksumBlockNum(offset);
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(chunkId, &cih) < 0)
        return 0;

    // the checksums should be loaded...
    cih->chunkInfo.VerifyChecksumsLoaded();

    assert(checksumBlock <= MAX_CHUNK_CHECKSUM_BLOCKS);

    return cih->chunkInfo.chunkBlockChecksum[checksumBlock];
}

DiskIo *
ChunkManager::SetupDiskIo(kfsChunkId_t chunkId, KfsOp *op)
{
    ChunkInfoHandle *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    if (tableEntry == mChunkTable.end()) {
        return NULL;
    }

    cih = tableEntry->second;
    if (!cih->IsFileOpen()) {
        CleanupInactiveFds();
        if (OpenChunk(chunkId, O_RDWR) < 0) 
            return NULL;
    }
 
    LruUpdate(*cih);
    return new DiskIo(cih->dataFH, op);
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
    CheckpointOp *cop;
    // on the macs, i can't declare CMI iter;
    CMI iter = mChunkTable.begin();

    mNextCheckpointTime = globals().netManager.Now() + mCheckpointIntervalSecs;

    if (!mIsChunkTableDirty)
        return;

    // KFS_LOG_STREAM_DEBUG << "Checkpointing state" << KFS_LOG_EOM;
    cop = new CheckpointOp(1);
    
#if 0
    // there are no more checkpoints on the chunkserver...this will all go
    // we are using this to rotate logs...

    ChunkInfoHandle *cih;

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

        cop->data << MAX_CHUNK_CHECKSUM_BLOCKS << ' ';
        for (uint32_t i = 0; i < MAX_CHUNK_CHECKSUM_BLOCKS; ++i) {
            cop->data << cih->chunkInfo.chunkBlockChecksum[i] << ' ';
        }
        cop->data << endl;
    }
#endif
    
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
        res = scandir(mChunkDirs[i].dirname.c_str(), &entries, 0, alphasort);
        if (res < 0) {
            KFS_LOG_STREAM_INFO <<
                "unable to open " << mChunkDirs[i].dirname <<
            KFS_LOG_EOM;
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

void
ChunkManager::GetChunkPathEntries(vector<string> &pathnames)
{
    uint32_t i;
    struct dirent **entries = NULL;
    int res;

    for (i = 0; i < mChunkDirs.size(); i++) {
        res = scandir(mChunkDirs[i].dirname.c_str(), &entries, 0, alphasort);
        if (res < 0) {
            KFS_LOG_STREAM_INFO <<
                "unable to open " << mChunkDirs[i].dirname <<
            KFS_LOG_EOM;
            mChunkDirs[i].availableSpace = -1;
            continue;
        }
        for (int j = 0; j < res; j++) {
            string s = mChunkDirs[i].dirname + "/" + entries[j]->d_name;
            pathnames.push_back(s);
            free(entries[j]);
        }
        free(entries);
    }
}

void
ChunkManager::Restart()
{
    int version;

    version = gLogger.GetVersionFromCkpt();
    if (version == gLogger.GetLoggerVersionNum()) {
        Restore();
    } else {
        std::cout << "Unsupported version...copy out the data and copy it back in...." << std::endl;
        exit(-1);
    }

    // Write out a new checkpoint file with just version and set it at 2
    gLogger.Checkpoint(NULL);
}

void
ChunkManager::Restore()
{
    // sort all the chunk names alphabetically in each of the
    // directories
    vector<string> chunkPathnames;
    struct stat buf;
    int res;
    uint32_t i, numChunkFiles;

    GetChunkPathEntries(chunkPathnames);

    numChunkFiles = chunkPathnames.size();
    // each chunk file is of the form: <fileid>.<chunkid>.<chunkversion>  
    // parse the filename to extract out the chunk info
    for (i = 0; i < numChunkFiles; ++i) {
        string s = chunkPathnames[i];
        ChunkInfoHandle *cih;
        res = stat(s.c_str(), &buf);
        if ((res < 0) || (!S_ISREG(buf.st_mode)))
            continue;
        MakeChunkInfoFromPathname(s, buf.st_size, &cih);
        if (cih != NULL)
            AddMapping(cih);
        else {
            KFS_LOG_STREAM_INFO <<
                "Deleting possibly duplicate file " << s <<
            KFS_LOG_EOM;
            unlink(s.c_str());
        }
    }
}

void
ChunkManager::AddMapping(ChunkInfoHandle *cih)
{
    mNumChunks++;
    mChunkTable[cih->chunkInfo.chunkId] = cih;
    mUsedSpace += cih->chunkInfo.chunkSize;
    UpdateDirSpace(cih, cih->chunkInfo.chunkSize);
}

void
ChunkManager::AddMapping(const ChunkInfo_t& ci)
{
    ChunkInfoHandle *cih = new ChunkInfoHandle();
    cih->chunkInfo = ci;
    mNumChunks++;
    mChunkTable[cih->chunkInfo.chunkId] = cih;
    mUsedSpace += cih->chunkInfo.chunkSize;
    UpdateDirSpace(cih, cih->chunkInfo.chunkSize);
}

void
ChunkManager::ReplayAllocChunk(kfsFileId_t fileId, kfsChunkId_t chunkId,
                               int64_t chunkVersion)
{
    ChunkInfoHandle *cih;

    mIsChunkTableDirty = true;

    if (GetChunkInfoHandle(chunkId, &cih) == 0) {
        // If the entry exists, just update the version
        cih->chunkInfo.chunkVersion = chunkVersion;
        mChunkTable[chunkId] = cih;
        return;
    }
    mNumChunks++;
    // after replay is done, when we verify entries in the table, we
    // stat the file and fix up the sizes then.  so, no need to do
    // anything here.
    cih = new ChunkInfoHandle();
    cih->chunkInfo.fileId = fileId;
    cih->chunkInfo.chunkId = chunkId;
    cih->chunkInfo.chunkVersion = chunkVersion;
    mChunkTable[chunkId] = cih;
}

void
ChunkManager::ReplayChangeChunkVers(kfsFileId_t fileId, kfsChunkId_t chunkId,
                                    int64_t chunkVersion)
{
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(chunkId, &cih) != 0) 
        return;

    KFS_LOG_STREAM_DEBUG << "Chunk " << chunkId <<
        " already exists; changing version # to " << chunkVersion <<
    KFS_LOG_EOM;    
    // Update the version #
    cih->chunkInfo.chunkVersion = chunkVersion;
    mChunkTable[chunkId] = cih;
    mIsChunkTableDirty = true;
}

void
ChunkManager::ReplayDeleteChunk(kfsChunkId_t chunkId)
{
    ChunkInfoHandle *cih;
    CMI tableEntry = mChunkTable.find(chunkId);

    mIsChunkTableDirty = true;

    if (tableEntry != mChunkTable.end()) {
        cih = tableEntry->second;
        mUsedSpace -= cih->chunkInfo.chunkSize;
        mChunkTable.erase(chunkId);
        Delete(*cih);

        mNumChunks--;
        assert(mNumChunks >= 0);
        if (mNumChunks < 0)
            mNumChunks = 0;

    }
}

void
ChunkManager::ReplayWriteDone(kfsChunkId_t chunkId, off_t chunkSize,
                              off_t offset, vector<uint32_t> checksums)
{
    ChunkInfoHandle *cih;
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
        size_t checksumBlock = OffsetToChecksumBlockNum(currOffset);
        
        cih->chunkInfo.chunkBlockChecksum[checksumBlock] = checksums[i];
    }
}

void
ChunkManager::ReplayTruncateDone(kfsChunkId_t chunkId, off_t chunkSize)
{
    ChunkInfoHandle *cih;
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

    cih->chunkInfo.chunkBlockChecksum[lastChecksumBlock] = 0;
}

void
ChunkManager::GetHostedChunks(vector<ChunkInfo_t> &result)
{
    ChunkInfoHandle *cih;

    // walk thru the table and pick up the chunk-ids
    for (CMI iter = mChunkTable.begin(); iter != mChunkTable.end(); ++iter) {
        cih = iter->second;
        result.push_back(cih->chunkInfo);
    }
}

int
ChunkManager::GetChunkInfoHandle(kfsChunkId_t chunkId, ChunkInfoHandle **cih)
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
ChunkManager::AllocateWriteId(WriteIdAllocOp *wi)
{
    WriteOp *op;
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(wi->chunkId, &cih) < 0)
        return -EBADF;

    if (wi->chunkVersion != cih->chunkInfo.chunkVersion) {
        KFS_LOG_STREAM_INFO << "Version # mismatch (have=" <<
            cih->chunkInfo.chunkVersion << " vs asked=" << wi->chunkVersion <<
            ")...failing a write" <<
        KFS_LOG_EOM;
        return -EINVAL;
    }

    mWriteId++;
    op = new WriteOp(wi->seq, wi->chunkId, wi->chunkVersion,
                     wi->offset, wi->numBytes, NULL, mWriteId);
    op->enqueueTime = globals().netManager.Now();
    wi->writeId = mWriteId;
    op->isWriteIdHolder = true;
    mPendingWrites.push_back(op);
    return 0;
}

int
ChunkManager::EnqueueWrite(WritePrepareOp *wp)
{
    WriteOp *op;
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(wp->chunkId, &cih) < 0)
        return -EBADF;

    if (wp->chunkVersion != cih->chunkInfo.chunkVersion) {
        KFS_LOG_STREAM_INFO << "Version # mismatch (have=" <<
            cih->chunkInfo.chunkVersion << " vs asked=" << wp->chunkVersion <<
            ")...failing a write" <<
        KFS_LOG_EOM;
        return -EINVAL;
    }
    op = mPendingWrites.FindAndMoveBack(wp->writeId);
    assert(op);
    if (op->dataBuf == NULL) {
        op->dataBuf = wp->dataBuf;
    } else {
        op->dataBuf->Append(wp->dataBuf);
        delete wp->dataBuf;
    }
    wp->dataBuf = NULL;
    return 0;
}

int64_t
ChunkManager::GetChunkVersion(kfsChunkId_t c)
{
    ChunkInfoHandle *cih;

    if (GetChunkInfoHandle(c, &cih) < 0)
        return -1;

    return cih->chunkInfo.chunkVersion;
}

WriteOp *
ChunkManager::CloneWriteOp(int64_t writeId)
{
    WriteOp *op, *other;

    other = mPendingWrites.find(writeId);
    if (! other || other->status < 0) {
        // if the write is "bad" already, don't add more data to it
        return NULL;
    }

    // Since we are cloning, "touch" the time
    other->enqueueTime = globals().netManager.Now();
    // offset/size/buffer are to be filled in
    op = new WriteOp(other->seq, other->chunkId, other->chunkVersion, 
                     0, 0, NULL, other->writeId);
    return op;
}

void
ChunkManager::SetWriteStatus(int64_t writeId, int status)
{
    WriteOp *op = mPendingWrites.find(writeId);
    if (! op)
        return;
    op->status = status;

    KFS_LOG_STREAM_INFO <<
        "Setting the status of writeid: " << writeId << " to " << status <<
    KFS_LOG_EOM;
}

int
ChunkManager::GetWriteStatus(int64_t writeId)
{
    WriteOp *op = mPendingWrites.find(writeId);
    if (! op)
        return -EINVAL;
    return op->status;
}


void
ChunkManager::Timeout()
{
    const time_t now = globals().netManager.Now();

#ifdef DEBUG
    verifyExecutingOnEventProcessor();
#endif

    if (now >= mNextCheckpointTime) {
        Checkpoint();
        // if any writes have been around for "too" long, remove them
        // and reclaim memory
        ScavengePendingWrites(now);
        // cleanup inactive fd's and thereby free up fd's
        CleanupInactiveFds(now);
    } else if (now > mNextPendingMetaSyncScanTime) {
        PendingMetaSyncQueue::Iterator it(mChunkInfoLists);
        int i = 0;
        ChunkInfoHandle* cih;
        while ((cih = it.Next()) &&
                cih->lastIOTime + mMetaSyncDelayTimeSecs < now) {
            KFS_LOG_STREAM_DEBUG << "[" << ++i <<
                "] starting sync for chunkId=" << cih->chunkInfo.chunkId <<
            KFS_LOG_EOM;
            cih->SyncMeta();
        }
        mNextPendingMetaSyncScanTime = now + (mMetaSyncDelayTimeSecs + 2) / 3;
    }
    if (now > mNextChunkDirsCheckTime) {
        // once in a while check that the drives hosting the chunks are good by doing disk IO.
        CheckChunkDirs();
        mNextChunkDirsCheckTime = now + mChunkDirsCheckIntervalSecs;
    }
}

void
ChunkManager::ScavengePendingWrites(time_t now)
{
    WriteOp *op;
    ChunkInfoHandle *cih;
    const time_t opExpireTime = now - mMaxPendingWriteLruSecs;

    while (! mPendingWrites.empty()) {
        op = mPendingWrites.front();
        // The list is sorted by enqueue time
        if (opExpireTime < op->enqueueTime) {
            break;
        }
        // if it exceeds 5 mins, retire the op
        KFS_LOG_STREAM_DEBUG <<
            "Retiring write with id=" << op->writeId <<
            " as it has been too long" <<
        KFS_LOG_EOM;
        mPendingWrites.pop_front();

        if (GetChunkInfoHandle(op->chunkId, &cih) == 0) {
            if (now - cih->lastIOTime >= mInactiveFdsCleanupIntervalSecs) {
                // close the chunk only if it is inactive
                CloseChunk(op->chunkId);
            }
            if (GetChunkInfoHandle(op->chunkId, &cih) == 0 &&
                    cih->IsFileOpen() && ! ChunkLru::IsInList(mChunkInfoLists, *cih)) {
                LruUpdate(*cih);
            }
        }
        delete op;
    }
}

int
ChunkManager::Sync(WriteOp *op)
{
    if (!op->diskIo) {
        return -1;
    }
    return op->diskIo->Sync(op->waitForSyncDone);
}

void
ChunkManager::CleanupInactiveFds(time_t now)
{
    const bool periodic = now > 0;
    // if we haven't cleaned up in 5 mins or if we too many fd's that
    // are open, clean up.
    if ((periodic ? now < mNextInactiveFdCleanupTime :
            (globals().ctrOpenDiskFds.GetValue() +
             globals().ctrOpenNetFds.GetValue()) <
             uint64_t(mMaxOpenChunkFiles))) {
        return;
    }

    const time_t cur = periodic ? now : globals().netManager.Now();
    // either we are periodic cleaning or we have too many FDs open
    // shorten the interval if we're out of fd.
    const time_t expireTime = cur - (periodic ?
        mInactiveFdsCleanupIntervalSecs :
        (mInactiveFdsCleanupIntervalSecs + 2) / 3);
    ChunkLru::Iterator it(mChunkInfoLists);
    ChunkInfoHandle* cih;
    while ((cih = it.Next()) && cih->lastIOTime < expireTime) {
        if (! cih->IsFileOpen() || cih->isBeingReplicated) {
            // Doesn't belong here, if / when io completes it will be added back.
            ChunkLru::Remove(mChunkInfoLists, *cih);
            continue;
        }
        bool inUse;
        bool hasLease = false;
        if ((inUse = cih->IsFileInUse()) ||
                (hasLease = gLeaseClerk.IsLeaseValid(cih->chunkInfo.chunkId)) ||
                IsWritePending(cih->chunkInfo.chunkId)) {
            KFS_LOG_STREAM_DEBUG << "cleanup: stale entry in chunk lru: "
                "fileid="    << cih->dataFH.get() <<
                " chunk="    << cih->chunkInfo.chunkId <<
                " last io= " << (now - cih->lastIOTime) << " sec. ago" <<
                (inUse ?    " file in use" : "") <<
                (hasLease ? " has lease"   : "") <<
            KFS_LOG_EOM;
            continue;
        }
        if (cih->SyncMeta()) {
            continue; // SyncMeta can delete cih
        }
        // we have a valid file-id and it has been over 5 mins since we last did I/O on it.
        KFS_LOG_STREAM_DEBUG << "cleanup: closing "
            "fileid="    << cih->dataFH.get() <<
            " chunk="    << cih->chunkInfo.chunkId <<
            " last io= " << (now - cih->lastIOTime) << " sec. ago" <<
        KFS_LOG_EOM;
        Release(*cih);
    }
    cih = ChunkLru::Front(mChunkInfoLists);
    mNextInactiveFdCleanupTime = mInactiveFdsCleanupIntervalSecs +
        ((cih && cih->lastIOTime > expireTime) ? cih->lastIOTime : cur);
}

string
GetStaleChunkPath(const string &partition)
{
    return partition + "/lost+found/";
}

#if defined(__APPLE__) || defined(__sun__) || (!defined(__i386__))
typedef struct statvfs StatVfs;
inline static int GetVfsStat(const char* path, StatVfs* stat) {
    return statvfs(path, stat);
}
#else
typedef struct statvfs64 StatVfs;
inline static int GetVfsStat(const char* path, StatVfs* stat) {
    return statvfs64(path, stat);
}
#endif

int64_t
ChunkManager::GetTotalSpace(
    bool startDiskIo) 
{
#if defined(__APPLE__)
    for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
        mChunkDirs[i].availableSpace = mTotalSpace;
    }
    return mTotalSpace;
#endif

    int64_t availableSpace = 0;
    set<dev_t> seenDrives;

    for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
        // report the space based on availability
        StatVfs result;
	struct stat statbuf;

        if (GetVfsStat(mChunkDirs[i].dirname.c_str(), &result) < 0 ||
	    stat(mChunkDirs[i].dirname.c_str(), &statbuf) < 0) {
            int err = errno;
            KFS_LOG_STREAM_INFO <<
                "statvfs (or stat) failed on " << mChunkDirs[i].dirname <<
                " with error: " << err << " " << strerror(err) <<
            KFS_LOG_EOM;
            mChunkDirs[i].availableSpace = 0;
            if ((err == EIO) || (err == ENOENT)) {
                // We can't stat the directory.
                // Notify metaserver that all blocks on this
                // drive are lost
                NotifyMetaChunksLost(mChunkDirs[i].dirname);
            }
            continue;
        }
        if (mChunkDirs[i].availableSpace < 0)
            // drive is flagged as being down; move on
            continue;

        string errMsg;
        if (startDiskIo &&
                ! DiskIo::StartIoQueue(mChunkDirs[i].dirname.c_str(),
                    result.f_fsid, mMaxOpenChunkFiles, &errMsg)) {
            KFS_LOG_STREAM_ERROR <<
                "Failed to start disk queue for: " << mChunkDirs[i].dirname <<
                " dev: << " << result.f_fsid << " :" << errMsg <<
            KFS_LOG_EOM;
            DiskIo::Shutdown();
            return -1;
        }
        if (seenDrives.find(statbuf.st_dev) != seenDrives.end()) {
            // if we have seen the drive where this directory is, then
            // we have already accounted for how much is free on the drive
            availableSpace += mChunkDirs[i].usedSpace;
            mChunkDirs[i].availableSpace = result.f_bavail * result.f_frsize + mChunkDirs[i].usedSpace;
        } else {
            // result.* is how much is available on disk; mUsedSpace is how
            // much we used up with chunks; so, the total storage available on
            // the drive is the sum of the two.  if we don't add mUsedSpace,
            // then all the chunks we write will get to use the space on disk and
            // won't get acounted for in terms of drive space.
            mChunkDirs[i].availableSpace = result.f_bavail * result.f_frsize + mChunkDirs[i].usedSpace;
            availableSpace += result.f_bavail * result.f_frsize + mChunkDirs[i].usedSpace;	    
            seenDrives.insert(statbuf.st_dev);
        }
        KFS_LOG_STREAM_INFO <<
            "Dir: " << mChunkDirs[i].dirname <<
            " has space " << mChunkDirs[i].availableSpace <<
        KFS_LOG_EOM;
    }
    if (startDiskIo) {
        mMaxIORequestSize = DiskIo::GetMaxRequestSize();
    }
    // we got all the info...so report true value
    return min(availableSpace, mTotalSpace);
}

void
ChunkManager::CheckChunkDirs()
{
    struct dirent **entries = NULL;
    KFS_LOG_STREAM_INFO << "Checking chunkdirs..." << KFS_LOG_EOM;
    for (uint32_t i = 0; i < mChunkDirs.size(); i++) {
        int nentries = scandir(mChunkDirs[i].dirname.c_str(), &entries, 0, alphasort);        
        if (nentries < 0) {
            KFS_LOG_STREAM_INFO <<
                "unable to open " << mChunkDirs[i].dirname <<
            KFS_LOG_EOM;
            NotifyMetaChunksLost(mChunkDirs[i].dirname);            
            mChunkDirs[i].availableSpace = -1;
            continue;
        }
        for (int j = 0; j < nentries; j++)
            free(entries[j]);
        free(entries);
    }
}

} // namespace KFS
