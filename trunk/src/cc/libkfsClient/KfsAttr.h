//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsClient/KfsAttr.h#3 $
//
// Created 2006/06/09
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
// \file KfsAttr.h
// \brief Kfs File/Chunk attributes.
//
//----------------------------------------------------------------------------

#ifndef LIBKFSCLIENT_KFSATTR_H
#define LIBKFSCLIENT_KFSATTR_H

extern "C" {
#include <sys/time.h>
}

#include <string>
#include <vector>
using std::string;
using std::vector;

#include "common/kfstypes.h"
#include "common/kfsdecls.h"

namespace KFS {

struct ChunkAttr {
    /// where is this chunk hosted.  There is a minor convention here:
    /// for writes, chunkServerLoc[0] holds the location of the
    /// "master" server that runs the transaction for writes.
    vector<ServerLocation> chunkServerLoc;

    /// what is the id of this chunk
    kfsChunkId_t chunkId;
    // what is the version # of this chunk
    int64_t  chunkVersion;

    /// did we request an allocation for this chunk?
    bool didAllocation;
    /// what is the size of this chunk
    size_t	chunkSize;
    ChunkAttr(): chunkId(-1), chunkVersion(-1), didAllocation(false),
                 chunkSize(0) { }
};

///
/// \brief Attributes downloaded from the metadata server for a file.
///
struct KfsServerAttr {
    /// the id of the file
    kfsFileId_t fileId;
    struct timeval mtime; /// modification time
    struct timeval ctime; /// attribute change time
    struct timeval crtime; /// creation time
    /// is this a directory?
    bool	isDirectory;
    /// how many chunks does it have
    long long	chunkCount;
};


///
/// \brief Server attributes + chunk attributes
///
struct FileAttr {
    /// the id of this file
    kfsFileId_t fileId;
    struct timeval mtime; /// modification time
    struct timeval ctime; /// attribute change time
    struct timeval crtime; /// creation time
    /// is this a directory?
    bool	isDirectory;
    /// the size of this file in bytes---computed value
    ssize_t	fileSize;
    
    /// how many chunks does it have
    long long	chunkCount;

    FileAttr() {
        fileId = (kfsFileId_t) -1;
        fileSize = 0;
        chunkCount = 0;
        isDirectory = false;
    }
    void Reset() {
        fileId = (kfsFileId_t) -1;
        fileSize = 0;
        chunkCount = 0;
        isDirectory = false;
    }
    void Init(bool isDir) {
        isDirectory = isDir;
        gettimeofday(&mtime, NULL);
        ctime = mtime;
        crtime = mtime;
    }
    FileAttr& operator= (const KfsServerAttr &other) {
        fileId = other.fileId;
        chunkCount = other.chunkCount;
        fileSize = 0; // need to compute this
        isDirectory = other.isDirectory;
        mtime = other.mtime;
        ctime = other.ctime;
        crtime = other.crtime;
        return *this;
    }
};

///
/// \brief File attributes as usable by applications.
///
/// 
struct KfsFileAttr {
    /// the name of this file
    string filename;
    /// the id of this file
    kfsFileId_t fileId;
    struct timeval mtime; /// modification time
    struct timeval ctime; /// attribute change time
    struct timeval crtime; /// creation time
    /// is this a directory?
    bool	isDirectory;
    /// the size of this file in bytes
    ssize_t	fileSize;

    KfsFileAttr& operator= (const KfsServerAttr &other) {
        fileId = other.fileId;
        fileSize = 0; // compute this from other.chunkCount
        isDirectory = other.isDirectory;
        mtime = other.mtime;
        ctime = other.ctime;
        crtime = other.crtime;
        return *this;
    }

    KfsFileAttr& operator= (const FileAttr &other) {
        fileId = other.fileId;
        fileSize = other.fileSize;
        isDirectory = other.isDirectory;
        mtime = other.mtime;
        ctime = other.ctime;
        crtime = other.crtime;
        return *this;
    }

};

}

#endif // LIBKFSCLIENT_KFSATTR_H
