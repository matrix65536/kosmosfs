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
// Declarations for the various Chunkserver ops.
//
// 
//----------------------------------------------------------------------------

#ifndef _CHUNKSERVER_KFSOPS_H
#define _CHUNKSERVER_KFSOPS_H

#include <string>
#include <fstream>
#include <sstream>
#include <vector>
using std::vector;
using std::string;
using std::ostringstream;
using std::ofstream;

// forward declaration to get compiler happy
struct KfsOp;

#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/IOBuffer.h"
#include "libkfsIO/DiskConnection.h"

#include "libkfsIO/Chunk.h"

#include "common/properties.h"
#include "ClientSM.h"

enum KfsOp_t {
    CMD_UNKNOWN,
    // Meta server->Chunk server ops
    CMD_ALLOC_CHUNK,
    CMD_DELETE_CHUNK,
    CMD_TRUNCATE_CHUNK,
    CMD_REPLICATE_CHUNK,
    CMD_CHANGE_CHUNK_VERS,
    CMD_HEARTBEAT,
    CMD_STALE_CHUNKS,
    // Chunk server->Meta server ops
    CMD_META_HELLO,
    CMD_LEASE_RENEW,

    // Client -> Chunkserver ops
    CMD_SYNC,
    CMD_OPEN,
    CMD_CLOSE,
    CMD_READ,
    CMD_WRITE_PREPARE,
    CMD_WRITE_SYNC,
    CMD_SIZE,
    // Chunkserver -> Chunkserver ops
    CMD_WRITE_COMMIT,
    // Monitoring ops
    CMD_PING,
    CMD_STATS,
    // Internally generated ops
    CMD_CHECKPOINT,
    CMD_WRITE,
    CMD_NCMDS
};

struct KfsOp : public KfsCallbackObj {
    KfsOp_t op;
    kfsSeq_t   seq;
    int32_t   status;
    bool      cancelled;
    bool      done;
    KfsCallbackObj  *clnt;
    KfsOp (KfsOp_t o, kfsSeq_t s, KfsCallbackObj *c = NULL) :
        op(o), seq(s), status(0), cancelled(false), done(false),
        clnt(c)
    {
        SET_HANDLER(this, &KfsOp::HandleDone);
    }
    void Cancel() {
        cancelled = true;
    }
    // to allow dynamic-type-casting, make the destructor virtual
    virtual ~KfsOp() { };
    virtual void Request(ostringstream &os) {
        // fill this method if the op requires a message to be sent to a server.
        (void) os;
    };
    // After an op finishes execution, this method generates the
    // response that should be sent back to the client.  The response
    // string that is generated is based on the KFS protocol.
    virtual void Response(ostringstream &os);
    virtual void Execute() = 0;
    virtual void Log(ofstream &ofs) { };
    // Return info. about op for debugging
    virtual string Show() const = 0;
    // If the execution of an op suspends and then resumes and
    // finishes, this method should be invoked to signify completion.
    virtual int HandleDone(int code, void *data);
};

//
// Model used in all the c'tor's of the ops: we do minimal
// initialization and primarily init the fields that are used for
// output.  The fields that are "input" are set when they are parsed
// from the input stream.
//
struct AllocChunkOp : public KfsOp {
    kfsFileId_t fileId; // input
    kfsChunkId_t chunkId; // input
    int64_t chunkVersion; // input
    int64_t leaseId; // input
    AllocChunkOp(kfsSeq_t s) :
        KfsOp(CMD_ALLOC_CHUNK, s)
    {
        // All inputs will be parsed in
    }
    void Response(ostringstream &os);
    void Execute();
    void Log(ofstream &ofs);
    string Show() const {
        ostringstream os;

        os << "alloc-chunk: fileid = " << fileId << " chunkid = " << chunkId;
        os << " chunkvers = " << chunkVersion << " leaseid = " << leaseId;
        return os.str();
    }
};

struct ChangeChunkVersOp : public KfsOp {
    kfsFileId_t fileId; // input
    kfsChunkId_t chunkId; // input
    int64_t chunkVersion; // input
    ChangeChunkVersOp(kfsSeq_t s) :
        KfsOp(CMD_CHANGE_CHUNK_VERS, s)
    {

    }
    void Execute();
    void Log(ofstream &ofs);
    string Show() const {
        ostringstream os;

        os << "change-chunk-vers: fileid = " << fileId << " chunkid = " << chunkId;
        os << " chunkvers = " << chunkVersion;
        return os.str();
    }
};

struct DeleteChunkOp : public KfsOp {
    kfsChunkId_t chunkId; // input
    DeleteChunkOp(kfsSeq_t s) :
        KfsOp(CMD_DELETE_CHUNK, s)
    {

    }
    void Execute();
    void Log(ofstream &ofs);
    string Show() const {
        ostringstream os;

        os << "delete-chunk: " << " chunkid = " << chunkId;
        return os.str();
    }
};

struct TruncateChunkOp : public KfsOp {
    kfsChunkId_t chunkId;  // input
    size_t	 chunkSize; // size to which file should be truncated to
    TruncateChunkOp(kfsSeq_t s) :
        KfsOp(CMD_TRUNCATE_CHUNK, s)
    {

    }
    void Execute();
    void Log(ofstream &ofs);
    string Show() const {
        ostringstream os;

        os << "truncate-chunk: " << " chunkid = " << chunkId;
        os << " chunksize: " << chunkSize;
        return os.str();
    }
};

class Replicator;
typedef boost::shared_ptr<Replicator> ReplicatorPtr;

// Op for replicating the chunk.  The metaserver is asking this
// chunkserver to create a copy of a chunk.  We replicate the chunk
// and then notify the server upon completion.
//
struct ReplicateChunkOp : public KfsOp {
    kfsChunkId_t chunkId;  // input
    ServerLocation location; // input: where to get the chunk from
    kfsFileId_t fid; // output: we tell the metaserver what we replicated
    int64_t chunkVersion; // output: we tell the metaserver what we replicated
    ReplicatorPtr replicator;
    ReplicateChunkOp(kfsSeq_t s) :
        KfsOp(CMD_REPLICATE_CHUNK, s) { }
    void Execute();
    void Response(ostringstream &os);
    int HandleDone(int code, void *data);
    void Log(ofstream &ofs);
    string Show() const {
        ostringstream os;

        os << "replicate-chunk: " << " chunkid = " << chunkId;
        return os.str();
    }
};

struct HeartbeatOp : public KfsOp {
    ssize_t totalSpace;
    ssize_t usedSpace;
    HeartbeatOp(kfsSeq_t s) :
        KfsOp(CMD_HEARTBEAT, s)
    {
        // the fields will be filled in when we execute
    }
    void Execute();
    void Response(ostringstream &os);
    string Show() const {
        return "meta-server heartbeat";
    }
};

struct StaleChunksOp : public KfsOp {
    int contentLength; /* length of data that identifies the stale chunks */
    int numStaleChunks; /* what the server tells us */
    vector<kfsChunkId_t> staleChunkIds; /* data we parse out */
    StaleChunksOp(kfsSeq_t s) :
        KfsOp(CMD_STALE_CHUNKS, s)
    { 
    
    }
    void Execute();
    void Response(ostringstream &os);
    string Show() const {
        ostringstream os;
        
        os << "stale chunks: " << " # stale: " << numStaleChunks;
        return os.str();
    }
};

struct OpenOp : public KfsOp {
    kfsChunkId_t chunkId;  // input
    int openFlags;  // either O_RDONLY, O_WRONLY
    OpenOp(kfsSeq_t s) :
        KfsOp(CMD_OPEN, s)
    {

    }
    void Execute();
    string Show() const {
        ostringstream os;
        
        os << "open: chunkId = " << chunkId;
        return os.str();
    }
};

struct CloseOp : public KfsOp {
    kfsChunkId_t chunkId; // input
    CloseOp(kfsSeq_t s) :
        KfsOp(CMD_CLOSE, s)
    {

    }
    void Execute();
    string Show() const {
        ostringstream os;
        
        os << "close: chunkId = " << chunkId;
        return os.str();
    }
};

struct ReadOp;
struct WriteOp;

struct WritePrepareOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t	 chunkVersion;
    off_t 	 offset;   /* input */
    size_t 	 numBytes; /* input */
    int64_t      writeId; /* output */
    IOBuffer *dataBuf; /* buffer with the data to be written */
    WritePrepareOp(kfsSeq_t s) :
        KfsOp(CMD_WRITE_PREPARE, s), writeId(-1), dataBuf(NULL) 
    {
        SET_HANDLER(this, &WritePrepareOp::HandleDone);
    }
    ~WritePrepareOp() {
        // on a successful prepapre, dataBuf should be moved to a write op.
        assert((status != 0) || (dataBuf == NULL));

        if (dataBuf != NULL)
            delete dataBuf;
    }

    void Response(ostringstream &os);
    void Execute();
    string Show() const {
        ostringstream os;
        
        os << "write-prepare: chunkId = " << chunkId << " chunkversion = " << chunkVersion;
        os << " offset: " << offset << " numBytes: " << numBytes;
        return os.str();
    }
};

struct WriteCommitOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t	 chunkVersion;
    int64_t      writeId; /* input */
    WriteOp	*writeOp; /* write Op that maps to writeId */
    WriteCommitOp(kfsSeq_t s, kfsChunkId_t c = -1,
                  int64_t v = -1, int64_t w = -1, KfsCallbackObj *owner = NULL) :
        KfsOp(CMD_WRITE_COMMIT, s, owner), chunkId(c), chunkVersion(v),
        writeId(w), writeOp(NULL) 
    {  
        SET_HANDLER(this, &WriteCommitOp::HandleDone);
    }
    ~WriteCommitOp();

    void Request(ostringstream &os);
    void Response(ostringstream &os);
    void Execute();
    int HandleDone(int code, void *data);
    string Show() const {
        ostringstream os;
        
        os << "write-commit: chunkId = " << chunkId << " chunkversion = " << chunkVersion;
        return os.str();
    }
};

struct WriteOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t	 chunkVersion;
    off_t 	 offset;   /* input */
    size_t 	 numBytes; /* input */
    ssize_t	 numBytesIO; /* output: # of bytes actually written */
    DiskConnectionPtr diskConnection; /* disk connection used for writing data */
    IOBuffer *dataBuf; /* buffer with the data to be written */
    size_t	 chunkSize; /* store the chunk size for logging purposes */
    vector<uint32_t> checksums; /* store the checksum for logging purposes */
    /* 
     * for writes that are smaller than a checksum block, we need to
     * read the whole block in, compute the new checksum and then write
     * out data.  This buffer holds the data read in from disk.
    */
    ReadOp *rop;
    int64_t      writeId;
    // time at which the write was enqueued at the ChunkManager
    time_t	 enqueueTime;

    WriteOp(kfsChunkId_t c, int64_t v) :
        KfsOp(CMD_WRITE, 0), chunkId(c), chunkVersion(v),
        dataBuf(NULL), rop(NULL)
    {
        SET_HANDLER(this, &WriteOp::HandleWriteDone);
    }

    WriteOp(kfsSeq_t s, kfsChunkId_t c, int64_t v, off_t o, size_t n, 
            IOBuffer *b, int64_t id) :
        KfsOp(CMD_WRITE, s), chunkId(c), chunkVersion(v),
        offset(o), numBytes(n), numBytesIO(0),
        dataBuf(b), chunkSize(0), rop(NULL), writeId(id) 
    {
        SET_HANDLER(this, &WriteOp::HandleWriteDone);
    }
    ~WriteOp();

    void Reset() {
        status = numBytesIO = 0;
        SET_HANDLER(this, &WriteOp::HandleWriteDone);
    }
    void Response(ostringstream &os) { };
    void Execute();
    void Log(ofstream &ofs);
    int HandleWriteDone(int code, void *data);    
    int HandleSyncDone(int code, void *data);
    string Show() const {
        ostringstream os;
        
        os << "write: chunkId = " << chunkId << " chunkversion = " << chunkVersion;
        os << " offset: " << offset << " numBytes: " << numBytes;
        return os.str();
    }
};

struct RemoteWriteInfo {
    ServerLocation location;
    int64_t writeId;
    // Trying to write a copy constructor that'll work for
    // WriteCommitOp is too painful.  We need a copy constructor
    // if we simply had "WriteCommitOp *wc": all the
    // vector manipulations we do in WriteSyncOp need to copy the
    // object.  Since the default copy constructor does the "wrong"
    // thing, make write-commit-op a smart pointer.  Then, the default
    // copy constructor does the "right" thing.
    boost::shared_ptr<WriteCommitOp> wc;
    RemoteWriteInfo() : writeId(-1) { }
    string Show() const {
        ostringstream os;
        
        os << " location= " << location.ToString() << " writeId = " << writeId;
        return os.str();
    }
};

class ShowRemoteWriteInfo {
    ostringstream &os;
public:
    ShowRemoteWriteInfo(ostringstream &o) : os(o) { }
    void operator() (const RemoteWriteInfo &r) {
        os << r.Show() << ' ';
    }
};

// sent by the client to force data to disk
struct WriteSyncOp : public KfsOp {
    kfsChunkId_t chunkId;    
    int64_t chunkVersion;
    vector<RemoteWriteInfo> remoteWriteInfo;
    vector<RemoteWriteInfo>::size_type numDone;

    WriteSyncOp(kfsSeq_t s, kfsChunkId_t c, int64_t v, int32_t n, 
                const string &writeInfo);

    void Response(ostringstream &os);
    void Execute();
    // on a write sync, first do the write locally.  after that is
    // done, send out requests to remote servers to do the write.
    // after they finish, notify the client
    int HandleLocalDone(int code, void *data);
    int HandleDone(int code, void *data);    
    string Show() const {
        ostringstream os;
        
        os << "write-sync: chunkId = " << chunkId << " chunkversion = " << chunkVersion;
        for_each(remoteWriteInfo.begin(), remoteWriteInfo.end(),
                 ShowRemoteWriteInfo(os));
        return os.str();
    }
};

struct ReadOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t	 chunkVersion;
    off_t 	 offset;   /* input */
    size_t 	 numBytes; /* input */
    ssize_t	 numBytesIO; /* output: # of bytes actually read */
    DiskConnectionPtr diskConnection; /* disk connection used for reading data */
    IOBuffer *dataBuf; /* buffer with the data read */
    /*
     * for writes that require the associated checksum block to be
     * read in, store the pointer to the associated write op.
    */
    WriteOp *wop;
    ReadOp(kfsSeq_t s) :
        KfsOp(CMD_READ, s), numBytesIO(0), dataBuf(NULL), wop(NULL)
    {
        SET_HANDLER(this, &ReadOp::HandleDone);
    }
    ReadOp(WriteOp *w, off_t o, size_t n) :
        KfsOp(CMD_READ, w->seq), chunkId(w->chunkId),
        chunkVersion(w->chunkVersion), offset(o), numBytes(n),
        numBytesIO(0), dataBuf(NULL), wop(w)
    {
        clnt = w;
        SET_HANDLER(this, &ReadOp::HandleDone);
    }
    ~ReadOp() {
        assert(wop == NULL);
        if (dataBuf != NULL) {
            delete dataBuf;
        }
        if (diskConnection)
            diskConnection->Close();
    }

    void Request(ostringstream &os);
    void Response(ostringstream &os);
    void Execute();
    int HandleDone(int code, void *data);
    string Show() const {
        ostringstream os;
        
        os << "read: chunkId = " << chunkId << " chunkversion = " << chunkVersion;
        os << " offset: " << offset << " numBytes: " << numBytes;
        return os.str();
    }
};

// used for retrieving a chunk's size
struct SizeOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t	 chunkVersion;
    size_t     size; /* result */
    SizeOp(kfsSeq_t s) :
        KfsOp(CMD_SIZE, s) { }
    SizeOp(kfsSeq_t s, kfsChunkId_t c, int64_t v) :
        KfsOp(CMD_SIZE, s), chunkId(c), chunkVersion(v) { }

    void Request(ostringstream &os);
    void Response(ostringstream &os);
    void Execute();
    string Show() const {
        ostringstream os;
        
        os << "size: chunkId = " << chunkId << " chunkversion = " << chunkVersion;
        return os.str();
    }
};

// used for pinging the server and checking liveness
struct PingOp : public KfsOp {
    size_t totalSpace;
    size_t usedSpace;
    PingOp(kfsSeq_t s) :
        KfsOp(CMD_PING, s) { }
    void Response(ostringstream &os);
    void Execute();
    string Show() const {
        return "monitoring ping";
    }
};

// used to extract out all the counters we have
struct StatsOp : public KfsOp {
    string stats; // result
    StatsOp(kfsSeq_t s) :
        KfsOp(CMD_STATS, s) { }
    void Response(ostringstream &os);
    void Execute();
    string Show() const {
        return "monitoring stats";
    }
};

/// Checkpoint op is a means of communication between the main thread
/// and the logger thread.  The main thread sends this op to the logger
/// thread and the logger threads gets rid of it after taking a
/// checkpoint.
// XXX: We may want to allow users to submit checkpoint requests.  At
// that point code will need to come in for ops and such.

struct CheckpointOp : public KfsOp {
    ostringstream data; // the data that needs to be checkpointed
    CheckpointOp(kfsSeq_t s) :
        KfsOp(CMD_CHECKPOINT, s) { }
    void Response(ostringstream &os) { };
    void Execute() { };
    string Show() const {
        return "internal: checkpoint";
    }
};

struct LeaseRenewOp : public KfsOp {
    kfsChunkId_t chunkId;
    int64_t leaseId;
    string leaseType;
    LeaseRenewOp(kfsSeq_t s, kfsChunkId_t c, int64_t l, string t) :
        KfsOp(CMD_LEASE_RENEW, s), chunkId(c), leaseId(l), leaseType(t)
    {
        SET_HANDLER(this, &LeaseRenewOp::HandleDone);
    }
    void Request(ostringstream &os);
    // To be called whenever we get a reply from the server
    int HandleDone(int code, void *data);
    void Execute() { };
    string Show() const {
        ostringstream os;

        os << "lease-renew: " << " chunkid = " << chunkId;
        os << " leaseId: " << leaseId << " type: " << leaseType;
        return os.str();
    }
};

// This is just a helper op for building a hello request to the metaserver.
struct HelloMetaOp : public KfsOp {
    ServerLocation myLocation;
    size_t totalSpace;
    size_t usedSpace;
    vector<ChunkInfo_t> chunks;
    HelloMetaOp(kfsSeq_t s, ServerLocation &l) :
        KfsOp(CMD_META_HELLO, s), myLocation(l) {
    }
    void Execute() { }
    void Request(ostringstream &os);
    string Show() const {
        ostringstream os;

        os << "meta-hello: " << " mylocation = " << myLocation.ToString();
        return os.str();
    }
};

// Helper functor that matches ops based on seq #

class OpMatcher {
    kfsSeq_t seqNum;
public:
    OpMatcher(kfsSeq_t s) : seqNum(s) { };
    bool operator() (KfsOp *op) {
        return op->seq == seqNum;
    }
};

extern void InitParseHandlers();
extern void RegisterCounters();

extern int ParseCommand(char *cmdBuf, int cmdLen, KfsOp **res);

#endif // CHUNKSERVER_KFSOPS_H
