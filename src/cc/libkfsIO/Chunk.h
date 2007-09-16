//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/03/22
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

#ifndef _CHUNKSERVER_CHUNK_H
#define _CHUNKSERVER_CHUNK_H

#include <stdint.h>
#include <unistd.h>
#include <boost/shared_ptr.hpp>

#include <vector>
using std::vector;

#include "common/log.h"
#include "common/kfstypes.h"
using namespace KFS;

///
/// \file Chunk.h
/// \brief Declarations related to a Chunk in KFS.
///


/// 
/// \brief ChunkInfo_t
/// The "file system state" at a chunk server consists of all the
/// chunks the server has and their associated version #/checksums.
/// This state is periodically flushed out to disk.  
///
struct ChunkInfo_t {

    ChunkInfo_t() {
        fileId = 0;
        chunkId = 0;
        chunkSize = 0;
        chunkVersion = 0;
    }
    kfsFileId_t fileId;
    kfsChunkId_t chunkId;
    size_t  chunkSize;
    uint32_t chunkVersion;
    vector<uint32_t> chunkBlockChecksum;
};

///
/// \struct ChunkHandle_t
/// Individual chunks are stored as normal files in the underlying
/// filesystem (viz., xfs).  A ChunkHandle_t stores the 64-bit unique
/// id of the chunk and a file descriptor that corresponds to the
/// backing chunk file.
///
struct ChunkHandle_t {
    ChunkHandle_t() {
        mFileId = -1;
    }
    ~ChunkHandle_t() {
        if (mFileId != -1) {
            COSMIX_LOG_DEBUG("Closing fileid: %d", mFileId);
            close(mFileId);
        }
        mFileId = -1;
    }
    /// 64-bit unique chunk identifier.
    kfsChunkId_t	mChunkId;
    /// UNIX file descriptor corresponding to the chunk file
    int			mFileId;
};

///
/// \typedef ChunkHandlePtr
/// Chunk handles are encapsulated in a smart pointer, so that when
/// the last reference is released, the file descriptor will be closed.
///
typedef boost::shared_ptr<ChunkHandle_t> ChunkHandlePtr;

// For each file opened for writing, we need to associate all the
// streams of data with it in one place.
// struct FileObject_t; 


#endif // _CHUNKSERVER_CHUNK_H
