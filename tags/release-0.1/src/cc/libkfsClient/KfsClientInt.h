//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/04/18
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

#ifndef LIBKFSCLIENT_KFSCLIENTINT_H
#define LIBKFSCLIENT_KFSCLIENTINT_H

#include <string>
#include <vector>
#include "common/kfstypes.h"
#include "libkfsIO/TcpSocket.h"

#include "KfsAttr.h"

namespace KFS {

const size_t MIN_BYTES_PIPELINE_IO = 65536;

/// Push as much as we can per write...
const size_t MAX_BYTES_PER_WRITE = 1 << 20;

/// If an op fails because the server crashed, retry the op.  This
/// constant defines the # of retries before declaring failure.
const uint8_t NUM_RETRIES_PER_OP = 3;

/// Whenever an op fails, we need to give time for the server to
/// recover.  So, introduce a delay of 5 secs between retries.
const int RETRY_DELAY_SECS = 5;

/// Directory entries that we may have cached are valid for 30 secs;
/// after that force a revalidataion.
const int FILE_CACHE_ENTRY_VALID_TIME = 30;

///
/// A KfsClient maintains a file-table that stores information about
/// KFS files on that client.  Each file in the file-table is composed
/// of some number of chunks; the meta-information about each
/// chunk is stored in a chunk-table associated with that file.  Thus, given a
/// <file-id, offset>, we can map it to the appropriate <chunk-id,
/// offset within the chunk>; we can also find where that piece of
/// data is located and appropriately access it.
///

///
/// \brief Buffer for speeding up small reads and writes; holds
/// on to a piece of data from one chunk.
///
struct ChunkBuffer {
    // set the client buffer to be fairly big...for sequential reads,
    // we will hit the network few times and on each occasion, we read
    // a ton and thereby get decent performance; having a big buffer
    // obviates the need to do read-ahead :-)
    static const size_t ONE_MB = 1 << 20;
    // static const size_t BUF_SIZE =
    // KFS::CHUNKSIZE < 16 * ONE_MB ? KFS::CHUNKSIZE : 16 * ONE_MB;
    static const size_t BUF_SIZE = KFS::CHUNKSIZE;

    ChunkBuffer():chunkno(-1), start(0), length(0), dirty(false) { }
    void invalidate() { chunkno = -1; start = 0; length = 0; dirty = false; }
    int chunkno;		// which chunk
    off_t start;		// offset with chunk
    size_t length;	// length of valid data
    bool dirty;		// must flush to server if true
    char buf[BUF_SIZE];	// the data
};

struct ChunkServerConn {
    /// name/port of the chunk server to which this socket is
    /// connected.
    ServerLocation location;
    /// connected TCP socket.  If this object is copy constructed, we
    /// really can't afford this socket to close when the "original"
    /// is destructed. To protect such issues, make this a smart pointer.
    TcpSocketPtr   sock;

    ChunkServerConn(const ServerLocation &l) :
        location(l) { 
        sock.reset(new TcpSocket());
    }
    
    void Connect() {
        if (!sock->IsGood())
            sock->Connect(location);
    }
    bool operator == (const ServerLocation &other) const {
        return other == location;
    }
};

///
/// \brief Location of the file pointer in a file consists of two
/// parts: the offset in the file, which then translates to a chunk #
/// and an offset within the chunk.  Also, for performance, we do some
/// client-side buffering (for both reads and writes).  The buffer
/// stores data corresponding to the "current" chunk.
///
struct FilePosition {
    FilePosition() {
	fileOffset = chunkOffset = 0;
	chunkNum = 0;
        preferredServer = NULL;
    }
    ~FilePosition() {

    }
    void Reset() {

        KFS_LOG_DEBUG("Calling reset servers");

	fileOffset = chunkOffset = 0;
	chunkNum = 0;
        chunkServers.clear();
        preferredServer = NULL;
    }


    off_t	fileOffset; // offset within the file
    /// which chunk are we at: this is an index into fattr.chunkTable[]
    int32_t	chunkNum;
    /// offset within the chunk
    off_t	chunkOffset;
    
    /// For the purpose of write, we may have to connect to multiple servers
    std::vector<ChunkServerConn> chunkServers;

    /// For reads as well as meta requests about a chunk, this is the
    /// preferred server to goto.  This is a pointer to a socket in
    /// the vector<ChunkServerConn> structure. 
    TcpSocket *preferredServer;

    void ResetServers() {
        KFS_LOG_DEBUG("Calling reset servers");

        chunkServers.clear();
        preferredServer = NULL;
    }

    TcpSocket *GetChunkServerSocket(const ServerLocation &loc) {
        std::vector<ChunkServerConn>::iterator iter;

        iter = std::find(chunkServers.begin(), chunkServers.end(), loc);
        if (iter != chunkServers.end()) {
            iter->Connect();
            return (iter->sock.get());
        }

        // Bit of an issue here: The object that is being pushed is
        // copy constructed; when that object is destructed, the
        // socket it has will go.  To avoid that, we need the socket
        // to be a smart pointer.
        chunkServers.push_back(ChunkServerConn(loc));
        chunkServers[chunkServers.size()-1].Connect();

        return (chunkServers[chunkServers.size()-1].sock.get());
    }

    void SetPreferredServer(const ServerLocation &loc) {
        preferredServer = GetChunkServerSocket(loc);
    }

    TcpSocket *GetPreferredServer() {
        return preferredServer;
    }
};

///
/// \brief A table of entries that describe each open KFS file.
///
struct FileTableEntry {
    // the fid of the parent dir in which this entry "resides"
    kfsFileId_t parentFid;
    // stores the name of the file/directory.
    std::string	name;
    // one of O_RDONLY, O_WRONLY, O_RDWR
    int		openMode;
    FileAttr	fattr;
    std::map <int, ChunkAttr> cattr;
    // the position in the file at which the next read/write will occur
    FilePosition currPos;
    /// For the current chunk, do some amount of buffering on the
    /// client.  This helps absorb network latencies for small
    /// reads/writes.
    ChunkBuffer buffer;
    // for LRU reclamation of file table entries, track when this
    // entry was last accessed
    time_t	lastAccessTime;
    // directory entries are cached; ala NFS, keep the entries cached
    // for a max of 30 secs; after that revalidate
    time_t	validatedTime;

    FileTableEntry(kfsFileId_t p, const char *n):
	parentFid(p), name(n), lastAccessTime(0), validatedTime(0) { }
};

// Helper functions
extern int DoOpSend(KfsOp *op, TcpSocket *sock);
extern int DoOpSend(KfsOp *op, std::vector<TcpSocket *> &targets);
extern int DoOpResponse(KfsOp *op, TcpSocket *sock);
extern int DoOpCommon(KfsOp *op, TcpSocket *sock);

}

#endif // LIBKFSCLIENT_KFSCLIENTINT_H
