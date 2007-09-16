//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/05/26
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
// Code for parsing commands sent to the Chunkserver and generating
// responses that summarize the result of their execution.
//
// 
//----------------------------------------------------------------------------

#include "KfsOps.h"
#include "common/kfstypes.h"
#include "libkfsIO/Globals.h"
using namespace libkfsio;

#include "ChunkManager.h"
#include "ChunkServer.h"
#include "LeaseClerk.h"
#include "Replicator.h"

#include <algorithm>

using std::map;
using std::string;
using std::istringstream;
using std::ostringstream;
using std::for_each;

typedef int (*ParseHandler)(Properties &, KfsOp **);

/// command -> parsehandler map
typedef map<string, ParseHandler> ParseHandlerMap;
typedef map<string, ParseHandler>::iterator ParseHandlerMapIter;

// handlers for parsing
ParseHandlerMap	gParseHandlers;

// Counters for the various ops
typedef map<KfsOp_t, Counter *> OpCounterMap;
typedef map<KfsOp_t, Counter *>::iterator OpCounterMapIter;

OpCounterMap gCounters;
Counter gCtrWriteMaster("Write Master");

const char *KFS_VERSION_STR = "KFS/1.0";

// various parse handlers
int parseHandlerOpen(Properties &prop, KfsOp **c);
int parseHandlerClose(Properties &prop, KfsOp **c);
int parseHandlerRead(Properties &prop, KfsOp **c);
int parseHandlerWritePrepare(Properties &prop, KfsOp **c);
int parseHandlerWriteCommit(Properties &prop, KfsOp **c);
int parseHandlerWriteSync(Properties &prop, KfsOp **c);
int parseHandlerSize(Properties &prop, KfsOp **c);
int parseHandlerAllocChunk(Properties &prop, KfsOp **c);
int parseHandlerDeleteChunk(Properties &prop, KfsOp **c);
int parseHandlerTruncateChunk(Properties &prop, KfsOp **c);
int parseHandlerReplicateChunk(Properties &prop, KfsOp **c);
int parseHandlerHeartbeat(Properties &prop, KfsOp **c);
int parseHandlerChangeChunkVers(Properties &prop, KfsOp **c);
int parseHandlerStaleChunks(Properties &prop, KfsOp **c);
int parseHandlerPing(Properties &prop, KfsOp **c);
int parseHandlerStats(Properties &prop, KfsOp **c);

void
InitParseHandlers()
{
    gParseHandlers["OPEN"] = parseHandlerOpen;
    gParseHandlers["CLOSE"] = parseHandlerClose;
    gParseHandlers["READ"] = parseHandlerRead;
    gParseHandlers["WRITE_PREPARE"] = parseHandlerWritePrepare;
    gParseHandlers["WRITE_COMMIT"] = parseHandlerWriteCommit;
    gParseHandlers["WRITE_SYNC"] = parseHandlerWriteSync;
    gParseHandlers["SIZE"] = parseHandlerSize;
    gParseHandlers["ALLOCATE"] = parseHandlerAllocChunk;
    gParseHandlers["DELETE"] = parseHandlerDeleteChunk;
    gParseHandlers["TRUNCATE"] = parseHandlerTruncateChunk;
    gParseHandlers["REPLICATE"] = parseHandlerReplicateChunk;
    gParseHandlers["HEARTBEAT"] = parseHandlerHeartbeat;
    gParseHandlers["STALE_CHUNKS"] = parseHandlerStaleChunks;
    gParseHandlers["CHUNK_VERS_CHANGE"] = parseHandlerChangeChunkVers;
    gParseHandlers["PING"] = parseHandlerPing;
    gParseHandlers["STATS"] = parseHandlerStats;
}

static void
AddCounter(const char *name, KfsOp_t opName)
{
    Counter *c;
    
    c = new Counter(name);
    globals().counterManager.AddCounter(c);
    gCounters[opName] = c;
}

void
RegisterCounters()
{
    static int calledOnce = 0;
    if (calledOnce)
        return;

    calledOnce = 1;
    AddCounter("Open", CMD_OPEN);
    AddCounter("Read", CMD_READ);
    AddCounter("Write Prepare", CMD_WRITE_PREPARE);
    AddCounter("Write Commit", CMD_WRITE_COMMIT);
    AddCounter("Write Sync", CMD_WRITE_SYNC);
    AddCounter("Size", CMD_SIZE);
    AddCounter("Alloc", CMD_ALLOC_CHUNK);
    AddCounter("Delete", CMD_DELETE_CHUNK);
    AddCounter("Truncate", CMD_TRUNCATE_CHUNK);
    AddCounter("Replicate", CMD_REPLICATE_CHUNK);
    AddCounter("Heartbeat", CMD_HEARTBEAT);
    AddCounter("Change Chunk Vers", CMD_CHANGE_CHUNK_VERS);

    globals().counterManager.AddCounter(&gCtrWriteMaster);
}

static void
UpdateCounter(KfsOp_t opName)
{
    Counter *c;
    OpCounterMapIter iter;
    
    iter = gCounters.find(opName);
    if (iter == gCounters.end())
        return;
    c = iter->second;
    c->Update(1);
}

///
/// Given a command in a buffer, parse it out and build a "Command"
/// structure which can then be executed.  For parsing, we take the
/// string representation of a command and build a Properties object
/// out of it; we can then pull the various headers in whatever order
/// we choose.
/// Commands are of the form:
/// <COMMAND NAME> \r\n
/// {header: value \r\n}+\r\n
///
///  The general model in parsing the client command:
/// 1. Each command has its own parser
/// 2. Extract out the command name and find the parser for that
/// command
/// 3. Dump the header/value pairs into a properties object, so that we
/// can extract the header/value fields in any order.
/// 4. Finally, call the parser for the command sent by the client.
///
/// @param[in] cmdBuf: buffer containing the request sent by the client
/// @param[in] cmdLen: length of cmdBuf
/// @param[out] res: A piece of memory allocated by calling new that
/// contains the data for the request.  It is the caller's
/// responsibility to delete the memory returned in res.
/// @retval 0 on success;  -1 if there is an error
/// 
int
ParseCommand(char *cmdBuf, int cmdLen, KfsOp **res)
{
    const char *delims = " \r\n";
    // header/value pairs are separated by a :
    const char separator = ':';
    string cmdStr;
    string::size_type cmdEnd;
    Properties prop;
    istringstream ist(cmdBuf);
    ParseHandlerMapIter entry;
    ParseHandler handler;
    
    // get the first line and find the command name
    ist >> cmdStr;
    // trim the command
    cmdEnd = cmdStr.find_first_of(delims);
    if (cmdEnd != string::npos) {
        cmdStr.erase(cmdEnd);
    }
    
    // find the parse handler and parse the thing
    entry = gParseHandlers.find(cmdStr);
    if (entry == gParseHandlers.end())
        return -1;
    handler = entry->second;

    prop.loadProperties(ist, separator, false);

    return (*handler)(prop, res);
}

void
parseCommon(Properties &prop, kfsSeq_t &seq)
{
    seq = prop.getValue("Cseq", (kfsSeq_t) -1);
}

int
parseHandlerOpen(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    OpenOp *oc;
    string openMode;

    parseCommon(prop, seq);

    oc = new OpenOp(seq);
    oc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    openMode = prop.getValue("Intent", "");
    // XXX: need to do a string compare
    oc->openFlags = O_RDWR;
    *c = oc;

    return 0;
}

int
parseHandlerClose(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    CloseOp *cc;

    parseCommon(prop, seq);

    cc = new CloseOp(seq);
    cc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    *c = cc;

    return 0;
}

int
parseHandlerRead(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    ReadOp *rc;

    parseCommon(prop, seq);

    rc = new ReadOp(seq);
    rc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    rc->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    rc->offset = prop.getValue("Offset", (off_t) 0);
    rc->numBytes = prop.getValue("Num-bytes", (long long) 0);
    *c = rc;

    return 0;
}

int
parseHandlerWritePrepare(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    WritePrepareOp *wp;

    parseCommon(prop, seq);

    wp = new WritePrepareOp(seq);
    wp->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    wp->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    wp->offset = prop.getValue("Offset", (off_t) 0);
    wp->numBytes = prop.getValue("Num-bytes", (long long) 0);
    *c = wp;

    return 0;
}

int
parseHandlerWriteCommit(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    WriteCommitOp *wc;

    parseCommon(prop, seq);

    wc = new WriteCommitOp(seq);
    wc->writeId = prop.getValue("Write-id", (int64_t) -1);
    wc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    wc->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    *c = wc;

    return 0;
}

int
parseHandlerWriteSync(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    WriteSyncOp *ws;
    string writeInfo;
    int numServers;
    kfsChunkId_t cid;
    int64_t chunkVers;

    parseCommon(prop, seq);
    numServers = prop.getValue("Num-servers", 0);
    writeInfo = prop.getValue("Servers", "");
    cid = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    chunkVers = prop.getValue("Chunk-version", (int64_t) -1);
    
    ws = new WriteSyncOp(seq, cid, chunkVers, numServers, writeInfo);

    *c = ws;

    return 0;
}

int
parseHandlerSize(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    SizeOp *sc;

    parseCommon(prop, seq);

    sc = new SizeOp(seq);
    sc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    sc->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    *c = sc;

    return 0;
}

int
parseHandlerAllocChunk(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    AllocChunkOp *cc;

    parseCommon(prop, seq);

    cc = new AllocChunkOp(seq);
    cc->fileId = prop.getValue("File-handle", (kfsFileId_t) -1);
    cc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    cc->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    // if the leaseId is a positive value, then this server has the
    // lease on this chunk.
    cc->leaseId = prop.getValue("Lease-id", (int64_t) -1);
    *c = cc;

    return 0;
}

int
parseHandlerDeleteChunk(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    DeleteChunkOp *cc;

    parseCommon(prop, seq);

    cc = new DeleteChunkOp(seq);
    cc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    *c = cc;

    return 0;
}

int
parseHandlerTruncateChunk(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    TruncateChunkOp *tc;

    parseCommon(prop, seq);

    tc = new TruncateChunkOp(seq);
    tc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    tc->chunkSize = prop.getValue("Chunk-size", (long long) 0);

    *c = tc;

    return 0;
}

int
parseHandlerReplicateChunk(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    ReplicateChunkOp *rc;
    string s;

    parseCommon(prop, seq);

    rc = new ReplicateChunkOp(seq);
    rc->fid = prop.getValue("File-handle", (kfsFileId_t) -1);
    rc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    rc->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);
    s = prop.getValue("Chunk-location", "");
    if (s != "") {
        rc->location.FromString(s);
    }

    *c = rc;

    return 0;
}

int
parseHandlerChangeChunkVers(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    ChangeChunkVersOp *rc;

    parseCommon(prop, seq);

    rc = new ChangeChunkVersOp(seq);
    rc->fileId = prop.getValue("File-handle", (kfsFileId_t) -1);
    rc->chunkId = prop.getValue("Chunk-handle", (kfsChunkId_t) -1);
    rc->chunkVersion = prop.getValue("Chunk-version", (int64_t) -1);

    *c = rc;

    return 0;
}

int
parseHandlerHeartbeat(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    HeartbeatOp *hb;

    parseCommon(prop, seq);

    hb = new HeartbeatOp(seq);
    *c = hb;

    return 0;
}

int
parseHandlerStaleChunks(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    StaleChunksOp *sc;

    parseCommon(prop, seq);

    sc = new StaleChunksOp(seq);
    sc->contentLength = prop.getValue("Content-length", 0);
    sc->numStaleChunks = prop.getValue("Num-chunks", 0);
    *c = sc;
    return 0;
}

int
parseHandlerPing(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    PingOp *po;

    parseCommon(prop, seq);

    po = new PingOp(seq);
    *c = po;
    return 0;
}

int
parseHandlerStats(Properties &prop, KfsOp **c)
{
    kfsSeq_t seq;
    StatsOp *so;

    parseCommon(prop, seq);

    so = new StatsOp(seq);
    *c = so;
    return 0;
}

///
/// Generic event handler for tracking completion of an event
/// execution.  Call the client state machine back and notify event
/// completion.
///
int
KfsOp::HandleDone(int code, void *data)
{
    clnt->HandleEvent(EVENT_CMD_DONE, this);
    return 0;
}

///
/// A read op finished.  Set the status and the # of bytes read
/// alongwith the data and notify the client.
///
int
ReadOp::HandleDone(int code, void *data)
{
    IOBuffer *b;
    size_t chunkSize = 0;

    if (code == EVENT_DISK_ERROR)
        status = -1;
    else if (code == EVENT_DISK_READ) {
        if (dataBuf == NULL) {
            dataBuf = new IOBuffer();
        }
        b = (IOBuffer *) data;
        // Order matters...when we append b, we take the data from b
        // and put it into our buffer.
        dataBuf->Append(b);
        // verify checksum
        gChunkManager.ReadChunkDone(this);
        numBytesIO = dataBuf->BytesConsumable();
        if (status == 0)
            // checksum verified
            status = numBytesIO;

    }

    gChunkManager.ChunkSize(chunkId, &chunkSize);

    if (chunkSize > 0 &&
        offset + numBytesIO >= (off_t) chunkSize) {
        // If we have read the full chunk, close out the fd.  The
        // observation is that reads are sequential and when we
        // finished a chunk, the client will move to the next one.
        gChunkManager.CloseChunk(chunkId);
    }

    if (wop == NULL)
        clnt->HandleEvent(EVENT_CMD_DONE, this);
    else
        // resume execution of the write
        wop->Execute();
    return 0;
}

int
WriteOp::HandleWriteDone(int code, void *data)
{
    if (code == EVENT_DISK_ERROR) {
        // eat up everything that was sent
        dataBuf->Consume(numBytes);
        status = -1;
        clnt->HandleEvent(EVENT_CMD_DONE, this);
        return 0;
    }
    else if (code == EVENT_DISK_WROTE) {
        status = *(int *) data;
        numBytesIO = status;
        SET_HANDLER(this, &WriteOp::HandleSyncDone);
        if (gChunkManager.Sync(this) < 0) {
            COSMIX_LOG_DEBUG("Sync failed...");
            // eat up everything that was sent
            dataBuf->Consume(numBytes);
            // Sync failed
            status = -1;
            clnt->HandleEvent(EVENT_CMD_DONE, this);
        }
    }
    return 0;
}

///
/// A write op finished.  Set the status and the # of bytes written
/// and notify the client.
///
int
WriteOp::HandleSyncDone(int code, void *data)
{
    // eat up everything that was sent
    dataBuf->Consume(numBytes);

    if (code == EVENT_SYNC_DONE) {
        gChunkManager.WriteChunkDone(this);
    } else {
        status = -1;
    }

    if (offset + numBytesIO >= (off_t) KFS::CHUNKSIZE) {
        // If we have written till the end of the chunk, close out the
        // fd.  The observation is that writes are sequential and when
        // we finished a chunk, the client will move to the next one.
        gChunkManager.CloseChunk(chunkId);
    }

    if (status < 0) {
        clnt->HandleEvent(EVENT_CMD_DONE, this);
    } else {
        gChunkManager.ChunkSize(chunkId, &chunkSize);
        gLogger.Submit(this);
    }
    
    return 0;
}

int
WriteCommitOp::HandleDone(int code, void *data)
{
    KfsOp *op = static_cast<KfsOp *> (data);

    // For local writes, we are called with the WriteOp that just
    // finished; for remote writes, we are called with data's type
    // being WriteCommitOp.
    if (op->op == CMD_WRITE) {
        status = op->status;
        assert(op == writeOp);
    }

    clnt->HandleEvent(EVENT_CMD_DONE, this);
    return 0;
}

///
/// Handlers for ops that need logging.  This method is invoked by the
/// logger thread.  So, don't access any globals here---otherwise, we
/// need to add locking.
///
void
AllocChunkOp::Log(ofstream &ofs)
{
    ofs << "ALLOCATE " << chunkId << ' ' << fileId << ' ';
    ofs << chunkVersion << "\n";
    assert(!ofs.fail());
}

/// Resetting a chunk's version # is equivalent to doing an allocation
/// of an existing chunk.
void
ChangeChunkVersOp::Log(ofstream &ofs)
{
    ofs << "CHANGE_CHUNK_VERS " << chunkId << ' ' << fileId << ' ';
    ofs << chunkVersion << "\n";
    assert(!ofs.fail());
}

void
DeleteChunkOp::Log(ofstream &ofs)
{
    ofs << "DELETE " << chunkId << "\n";
    assert(!ofs.fail());
}

void
WriteOp::Log(ofstream &ofs)
{
    ofs << "WRITE " << chunkId << ' ' << chunkSize << ' ';
    ofs << offset << ' ';
    ofs << checksums.size();
    for (vector<uint32_t>::size_type i = 0; i < checksums.size(); i++) {
        ofs << ' ' << checksums[i];
    }
    ofs << "\n";
    assert(!ofs.fail());
}

void
TruncateChunkOp::Log(ofstream &ofs)
{
    ofs << "TRUNCATE " << chunkId << ' ' << chunkSize << "\n";
    assert(!ofs.fail());
}

// For replicating a chunk, we log nothing.  We don't write out info
// about the chunk in checkpoint files until replication is complete.
// This way, if we ever crash during chunk-replication, we'll simply
// nuke out the chunk on startup.
void
ReplicateChunkOp::Log(ofstream &ofs)
{

}

///
/// Handlers for executing the various ops.  If the op execution is
/// "in-line", that is the op doesn't block, then when the execution
/// is finished, the client is notified.  Otherwise, the op is queued
/// for execution and the client gets notified whenever the op
/// finishes execution.
///
void
OpenOp::Execute()
{
    status = gChunkManager.OpenChunk(chunkId, openFlags);

    UpdateCounter(CMD_OPEN);

    clnt->HandleEvent(EVENT_CMD_DONE, this);
}

void
CloseOp::Execute()
{
    UpdateCounter(CMD_CLOSE);

    gChunkManager.CloseChunk(chunkId);
    status = 0;
    clnt->HandleEvent(EVENT_CMD_DONE, this);
}

void
AllocChunkOp::Execute()
{
    UpdateCounter(CMD_ALLOC_CHUNK);

    status = gChunkManager.AllocChunk(fileId, chunkId, 
                                            chunkVersion);
    if (status < 0) {
        // failed; nothing to log
        clnt->HandleEvent(EVENT_CMD_DONE, this);
        return;
    }
    gLogger.Submit(this);
    if (leaseId >= 0) {
        gCtrWriteMaster.Update(1);
        gLeaseClerk.RegisterLease(chunkId, leaseId);
    }
}

void
DeleteChunkOp::Execute()
{
    UpdateCounter(CMD_DELETE_CHUNK);

    status = gChunkManager.DeleteChunk(chunkId);
    if (status < 0)
        // failed; nothing to log
        clnt->HandleEvent(EVENT_CMD_DONE, this);
    else
        gLogger.Submit(this);
}

void
TruncateChunkOp::Execute()
{
    UpdateCounter(CMD_TRUNCATE_CHUNK);

    status = gChunkManager.TruncateChunk(chunkId, chunkSize);
    if (status < 0)
        // failed; nothing to log
        clnt->HandleEvent(EVENT_CMD_DONE, this);
    else
        gLogger.Submit(this);
}

void
ReplicateChunkOp::Execute()
{
    UpdateCounter(CMD_REPLICATE_CHUNK);

#ifdef DEBUG
    string s = location.ToString();
    COSMIX_LOG_DEBUG("Replicating chunk: %ld from %s",
                     chunkId, s.c_str());
#endif

    replicator.reset(new Replicator(this));
    if (!replicator->Connect()) {
        status = -ECONNREFUSED;
        clnt->HandleEvent(EVENT_CMD_DONE, this);
        return;
    }
    // Get the animation going...
    SET_HANDLER(this, &ReplicateChunkOp::HandleDone);
    replicator->Start();
}

void
ChangeChunkVersOp::Execute()
{
    UpdateCounter(CMD_CHANGE_CHUNK_VERS);

    status = gChunkManager.ChangeChunkVers(fileId, chunkId, 
                                                 chunkVersion);
    if (status < 0)
        // failed; nothing to log
        clnt->HandleEvent(EVENT_CMD_DONE, this);
    else
        gLogger.Submit(this);
}

// This is the heartbeat sent by the meta server
void
HeartbeatOp::Execute()
{
    UpdateCounter(CMD_HEARTBEAT);

    totalSpace = gChunkManager.GetTotalSpace();
    usedSpace = gChunkManager.GetUsedSpace();
    status = 0;
    clnt->HandleEvent(EVENT_CMD_DONE, this);
}

void
StaleChunksOp::Execute()
{
    vector<kfsChunkId_t>::size_type i;

    for (i = 0; i < staleChunkIds.size(); ++i) {
        gChunkManager.DeleteChunk(staleChunkIds[i]);
    }
    status = 0;
    clnt->HandleEvent(EVENT_CMD_DONE, this);
}

void
ReadOp::Execute()
{
    UpdateCounter(CMD_READ);

    status = gChunkManager.ReadChunk(this);

    if ((status < 0) && (wop == NULL))
        clnt->HandleEvent(EVENT_CMD_DONE, this);

}

//
// Handling of writes is done in multiple steps:
// 1. The client allocates a chunk from the metaserver; the metaserver
// picks a set of hosting chunkservers and nominates one of the
// server's as the "master" for the transaction.
// 2. The client pushes data for a write via a WritePrepareOp to each
// of the hosting chunkservers (in any order).
// 3. The chunkserver in turn enqueues the write with the ChunkManager
// object.  The ChunkManager assigns an id to the write.   NOTE:
// nothing is written out to disk at this point.
// 4. After the client has pushed out data to replica chunk-servers
// and gotten write-id's, the client does a WriteSync to the master.
// 5. The master retrieves the write corresponding to the write-id and
// commits the write to disk.
// 6. The master then sends out a WriteCommit to each of the replica
// chunkservers asking them to commit the write; this commit message
// is sent concurrently to all the replicas.
// 7. After the replicas reply, the master replies to the client with
// status from individual servers and how much got written on each.
//
void 
WritePrepareOp::Execute()
{
    UpdateCounter(CMD_WRITE_PREPARE);

    status = gChunkManager.EnqueueWrite(this);

    clnt->HandleEvent(EVENT_CMD_DONE, this);
}

void 
WriteCommitOp::Execute()
{
    WriteOp *op = gChunkManager.GetWriteOp(writeId);

    UpdateCounter(CMD_WRITE_COMMIT);

    if ((op == NULL) || (op->chunkId != chunkId)) {
        if (op)
            delete op;
        status = -EINVAL;
        clnt->HandleEvent(EVENT_CMD_DONE, this);
        return;
    }

    int64_t v = gChunkManager.GetChunkVersion(chunkId);
    if (v != chunkVersion) {
        COSMIX_LOG_DEBUG("Version # mismatch(have=%ld vs asked=%ld...failing a commit",
                         v, chunkVersion);
        status = -KFS::EBADVERS;
        clnt->HandleEvent(EVENT_CMD_DONE, this);
        return;
    }

    writeOp = op;
    op->clnt = this;
    op->Execute();
}

void 
WriteOp::Execute()
{
    status = gChunkManager.WriteChunk(this);

    if (status < 0)
        clnt->HandleEvent(EVENT_CMD_DONE, this);
}

void
SizeOp::Execute()
{
    UpdateCounter(CMD_SIZE);

    status = gChunkManager.ChunkSize(chunkId, &size);
    clnt->HandleEvent(EVENT_CMD_DONE, this);
}

void
PingOp::Execute()
{
    totalSpace = gChunkManager.GetTotalSpace();
    usedSpace = gChunkManager.GetUsedSpace();
    status = 0;
    clnt->HandleEvent(EVENT_CMD_DONE, this);
}

void
StatsOp::Execute()
{
    ostringstream os;

    globals().counterManager.Show(os);
    stats = os.str();
    status = 0;
    clnt->HandleEvent(EVENT_CMD_DONE, this);
}

///
/// Generate response for an op based on the KFS protocol.
///
void
KfsOp::Response(ostringstream &os)
{
    os << "OK\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Status: " << status << "\r\n\r\n";
}

void
SizeOp::Response(ostringstream &os)
{
    os << "OK\r\n";
    os << "Cseq: " << seq << "\r\n";
    if (status < 0) {
        os << "Status: " << status << "\r\n\r\n";
        return;
    }
    os << "Status: " << status << "\r\n";    
    os << "Size: " << size << "\r\n\r\n";
}

void
ReadOp::Response(ostringstream &os)
{
    os << "OK\r\n";
    os << "Cseq: " << seq << "\r\n";
    if (status < 0) {
        os << "Status: " << status << "\r\n\r\n";
        return;
    }
    os << "Status: " << status << "\r\n";    
    os << "Content-length: " << numBytesIO << "\r\n\r\n";
}

void
WritePrepareOp::Response(ostringstream &os)
{
    os << "OK\r\n";
    os << "Cseq: " << seq << "\r\n";
    if (status < 0) {
        os << "Status: " << status << "\r\n\r\n";    
        return;
    }
    os << "Status: " << status << "\r\n";
    os << "Write-id: " << writeId << "\r\n\r\n";
}

void
SizeOp::Request(ostringstream &os)
{
    os << "SIZE \r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n\r\n";
}

void
ReadOp::Request(ostringstream &os)
{
    os << "READ \r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Chunk-handle: " << chunkId << "\r\n";
    os << "Chunk-version: " << chunkVersion << "\r\n";
    os << "Offset: " << offset << "\r\n";
    os << "Num-bytes: " << numBytes << "\r\n\r\n";
}

void
WriteCommitOp::Request(ostringstream &os)
{
    os << "WRITE_COMMIT\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Chunk-handle:" << chunkId << "\r\n";
    os << "Chunk-version:" << chunkVersion << "\r\n";
    os << "Write-id: " << writeId << "\r\n\r\n";
}

void
WriteCommitOp::Response(ostringstream &os)
{
    os << "OK\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Status: " << status << "\r\n\r\n";    
}

void
WriteSyncOp::Response(ostringstream &os)
{
    os << "OK\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Status: " << status << "\r\n\r\n";    
}

void
AllocChunkOp::Response(ostringstream &os)
{
    os << "OK\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Status: " << status << "\r\n\r\n";
}

void
HeartbeatOp::Response(ostringstream &os)
{
    os << "OK\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Status: " << status << "\r\n";
    os << "Total-space: " << totalSpace << "\r\n";
    os << "Used-space: " << usedSpace << "\r\n\r\n";
}

void
StaleChunksOp::Response(ostringstream &os)
{
    os << "OK\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Status: " << status << "\r\n\r\n";
}

void
ReplicateChunkOp::Response(ostringstream &os)
{
    os << "OK\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Status: " << status << "\r\n";
    if (status == 0) {
        os << "File-handle: " << fid << "\r\n";
        os << "Chunk-version: " << chunkVersion << "\r\n";
    }
    os << "\r\n";
}

void
PingOp::Response(ostringstream &os)
{
    ServerLocation loc = gMetaServerSM.GetLocation();

    os << "OK\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Status: " << status << "\r\n";
    os << "Meta-server-host: " << loc.hostname << "\r\n";
    os << "Meta-server-port: " << loc.port << "\r\n";
    os << "Total-space: " << totalSpace << "\r\n";
    os << "Used-space: " << usedSpace << "\r\n\r\n";
}

void
StatsOp::Response(ostringstream &os)
{
    os << "OK\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Status: " << status << "\r\n";
    os << stats << "\r\n";
}

////////////////////////////////////////////////
// Now the handle done's....
////////////////////////////////////////////////

int
ReplicateChunkOp::HandleDone(int code, void *data)
{
    if (status == 0) {
        gLogger.Submit(this);
        return 0;
    }
    clnt->HandleEvent(EVENT_CMD_DONE, this);
    return 0;
    
}

WriteOp::~WriteOp()
{
    if (dataBuf != NULL)
        delete dataBuf;
    if (rop != NULL) {
        rop->wop = NULL;
        assert(rop->dataBuf == NULL);
        delete rop;
    }
    if (diskConnection)
        diskConnection->Close();
}

WriteSyncOp::WriteSyncOp(kfsSeq_t s, kfsChunkId_t c, int64_t v,
                         int32_t n, const string &writeInfo)
    : KfsOp(CMD_WRITE_SYNC, s), chunkId(c), chunkVersion(v), numDone(0)
{
    istringstream ist(writeInfo);
    bool gotLocalWrite = false;

    status = 0;
    // Pre-allocate memory for the remote writes; otherwise, each time
    // we do a "push_back", STL may do reallocations.  If we don't
    // like "reserve", provide a copy constructor
    remoteWriteInfo.reserve(n);

    for (int i = 0; i < n; i++) {
        RemoteWriteInfo r;
        ist >> r.location.hostname;
        ist >> r.location.port;
        ist >> r.writeId;

        remoteWriteInfo.push_back(r);
        remoteWriteInfo[i].wc.reset(new WriteCommitOp(seq, chunkId, chunkVersion,
                                                      r.writeId, this));

        if (gChunkServer.IsLocalServer(r.location)) {
            if (gotLocalWrite) {
                // duplicate local write?
                status = -EINVAL;
                break;
            }
            // put the local write at index 0.
            if (i != 0) {
                RemoteWriteInfo temp;

                temp = remoteWriteInfo[i];
                remoteWriteInfo[i] = remoteWriteInfo[0];
                remoteWriteInfo[0] = temp;
            }

            gotLocalWrite = true;
        }
    }
    if (!gotLocalWrite) {
        status = -EINVAL;
    }

}

void
WriteSyncOp::Execute()
{
    UpdateCounter(CMD_WRITE_SYNC);

    if (!gLeaseClerk.IsLeaseValid(chunkId)) {
        COSMIX_LOG_DEBUG("Write sync failed...lease expired for %ld",
                         chunkId);
        status = -KFS::ELEASEEXPIRED;
    }

    if (status < 0) {
        clnt->HandleEvent(EVENT_CMD_DONE, this);
        return;
    }
    // Notify the lease clerk that we are doing write.  This is to
    // signal the lease clerk to renew the lease for the chunk when appropriate.
    gLeaseClerk.DoingWrite(chunkId);

    // commit the write locally and then we will do remote

    SET_HANDLER(this, &WriteSyncOp::HandleLocalDone);

    remoteWriteInfo[0].wc->Execute();
}

class RemoteWriteSync {
    WriteSyncOp *owner;

public:
    RemoteWriteSync(WriteSyncOp *o) : owner(o) { };
    void operator() (RemoteWriteInfo &p) {
        if (gChunkServer.IsLocalServer(p.location))
            return;

        RemoteSyncSMPtr peer = gChunkServer.FindServer(p.location);

        if (!peer) {
            WriteCommitOp op(owner->seq);

            op.status = -EINVAL;
            owner->HandleEvent(EVENT_CMD_DONE, &op);
            return;
        }
        p.wc.reset(new WriteCommitOp(peer->NextSeqnum(),
                                     owner->chunkId,
                                     owner->chunkVersion,
                                     p.writeId, owner));
        peer->Enqueue(p.wc.get());
    }
};

// Write has finished locally.  Send out writes to replicas if there any
int
WriteSyncOp::HandleLocalDone(int code, void *data)
{
    WriteCommitOp *op = static_cast<WriteCommitOp *> (data);

    if (op->status < 0) {
        status = op->status;
        clnt->HandleEvent(EVENT_CMD_DONE, this);
        return 0;
    }
    numDone = 1;

    status = 0;

    SET_HANDLER(this, &WriteSyncOp::HandleDone);    

    if (numDone == remoteWriteInfo.size()) {
        status = op->status;
        clnt->HandleEvent(EVENT_CMD_DONE, this);
        return 0;
    }
        
    // commit writes on remote servers
    for_each(remoteWriteInfo.begin(), remoteWriteInfo.end(),
             RemoteWriteSync(this));
    return 0;
}

// Write has finished locally as well as on replicas.  Send out status
int
WriteSyncOp::HandleDone(int code, void *data)
{
    WriteCommitOp *op = static_cast<WriteCommitOp *> (data);

    numDone++;
    if (op->status < 0) {
        status = op->status;
    }
    if (numDone == remoteWriteInfo.size()) {
        if (status == 0) {
            // yea...everywhere write succeeded; so, tell the client
            // how many bytes got written out.  get it out of the
            // local write.
            status = remoteWriteInfo[0].wc->status;
        }
        clnt->HandleEvent(EVENT_CMD_DONE, this);
    }
    return 0;
}

WriteCommitOp::~WriteCommitOp()
{
    delete writeOp;
}

void
LeaseRenewOp::Request(ostringstream &os)
{
    os << "LEASE_RENEW\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Chunk-handle:" << chunkId << "\r\n";
    os << "Lease-id: " << leaseId << "\r\n";
    os << "Lease-type: " << leaseType << "\r\n\r\n";
}

int
LeaseRenewOp::HandleDone(int code, void *data)
{
    KfsOp *op = (KfsOp *) data;

    assert(op == this);
    return op->clnt->HandleEvent(EVENT_CMD_DONE, data);
}

class PrintChunkInfo {
    ostringstream &os;
public:
    PrintChunkInfo(ostringstream &o) : os(o) { }
    void operator() (ChunkInfo_t &c) {
        os << c.fileId << ' ';
        os << c.chunkId << ' ';
        os << c.chunkVersion << ' ';
    }
};

void
HelloMetaOp::Request(ostringstream &os)
{
    ostringstream chunkInfo;

    os << "HELLO \r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n";
    os << "Chunk-server-name: " << myLocation.hostname << "\r\n";
    os << "Chunk-server-port: " << myLocation.port << "\r\n";
    os << "Total-space: " << totalSpace << "\r\n";
    os << "Used-space: " << usedSpace << "\r\n";

    // now put in the chunk information
    os << "Num-chunks: " << chunks.size() << "\r\n";
    
    // figure out the content-length first...
    for_each(chunks.begin(), chunks.end(), PrintChunkInfo(chunkInfo));

    os << "Content-length: " << chunkInfo.str().length() << "\r\n\r\n";
    os << chunkInfo.str().c_str();
}
