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
// \file KfsClient.h
// \brief Kfs Client-library code.
//
//----------------------------------------------------------------------------

#ifndef LIBKFSCLIENT_KFSCLIENT_H
#define LIBKFSCLIENT_KFSCLIENT_H

#include <string>
#include <vector>
#include <sstream>
#include <algorithm>

#include <sys/stat.h>

#include "common/kfstypes.h"
#include "libkfsIO/TcpSocket.h"

#include "KfsAttr.h"
#include "KfsOps.h"
#include "LeaseClerk.h"

#include "concurrency.h"

#include "KfsClientInt.h"

namespace KFS {

/// Maximum length of a filename
const size_t MAX_FILENAME_LEN = 256;

///
/// \brief The KfsClient is the "bridge" between applications and the
/// KFS servers (either the metaserver or chunkserver): there can be
/// only one client per application; the client can interface with
/// only one metaserver.
///
class KfsClient {
    // Make the constructor private to get a Singleton.
    KfsClient();
    KfsClient(const KfsClient &other);
    const KfsClient & operator=(const KfsClient &other);

public:
    static KfsClient *Instance() {
        static KfsClient instance;
        return &instance;
    }
    ///
    /// @param[in] propFile that describes where the server is and
    /// other client configuration info.
    /// @retval 0 on success; -1 on failure
    ///
    int Init(const char *propFile);

    ///
    /// @param[in] metaServerHost  Machine on meta is running
    /// @param[in] metaServerPort  Port at which we should connect to
    /// @retval 0 on success; -1 on failure
    ///
    int Init(const std::string metaServerHost, int metaServerPort);

    bool IsInitialized() { return mIsInitialized; };

    ///
    /// Provide a "cwd" like facility for KFS.
    /// @param[in] pathname  The pathname to change the "cwd" to
    /// @retval 0 on sucess; -errno otherwise
    ///
    int Cd(const char *pathname);

    ///
    /// Make a directory hierarcy in KFS.  If the parent dirs are not
    /// present, they are also made.
    /// @param[in] pathname		The full pathname such as /.../dir
    /// @retval 0 if mkdir is successful; -errno otherwise
    int Mkdirs(const char *pathname);

    ///
    /// Make a directory in KFS.
    /// @param[in] pathname		The full pathname such as /.../dir
    /// @retval 0 if mkdir is successful; -errno otherwise
    int Mkdir(const char *pathname);

    ///
    /// Remove a directory in KFS.
    /// @param[in] pathname		The full pathname such as /.../dir
    /// @retval 0 if rmdir is successful; -errno otherwise
    int Rmdir(const char *pathname);

    ///
    /// Read a directory's contents
    /// @param[in] pathname	The full pathname such as /.../dir
    /// @param[out] result	The contents of the directory
    /// @retval 0 if readdir is successful; -errno otherwise
    int Readdir(const char *pathname, std::vector<std::string> &result);

    ///
    /// Read a directory's contents and retrieve the attributes
    /// @param[in] pathname	The full pathname such as /.../dir
    /// @param[out] result	The files in the directory and their attributes.
    /// @retval 0 if readdirplus is successful; -errno otherwise
    ///
    int ReaddirPlus(const char *pathname, std::vector<KfsFileAttr> &result);

    ///
    /// Stat a file and get its attributes.
    /// @param[in] pathname	The full pathname such as /.../foo
    /// @param[out] result	The attributes that we get back from server
    /// @param[in] computeFilesize  When set, for files, the size of
    /// file is computed and the value is returned in result.st_size
    /// @retval 0 if stat was successful; -errno otherwise
    ///
    int Stat(const char *pathname, struct stat &result, bool computeFilesize = true);

    ///
    /// Helper APIs to check for the existence of (1) a path, (2) a
    /// file, and (3) a directory.
    /// @param[in] pathname	The full pathname such as /.../foo
    /// @retval status: True if it exists; false otherwise
    ///
    bool Exists(const char *pathname);
    bool IsFile(const char *pathname);
    bool IsDirectory(const char *pathname);

    ///
    /// Create a file which is specified by a complete path.
    /// @param[in] pathname that has to be created
    /// @param[in] numReplicas the desired degree of replication for
    /// the file.
    /// @param[in] exclusive  create will fail if the exists (O_EXCL flag)
    /// @retval on success, fd corresponding to the created file;
    /// -errno on failure.
    ///
    int Create(const char *pathname, int numReplicas = 3, bool exclusive = false);

    ///
    /// Remove a file which is specified by a complete path.
    /// @param[in] pathname that has to be removed
    /// @retval status code
    ///
    int Remove(const char *pathname);

    ///
    /// Rename file/dir corresponding to oldpath to newpath
    /// @param[in] oldpath   path corresponding to the old name
    /// @param[in] newpath   path corresponding to the new name
    /// @param[in] overwrite  when set, overwrite the newpath if it
    /// exists; otherwise, the rename will fail if newpath exists
    /// @retval 0 on success; -1 on failure
    ///
    int Rename(const char *oldpath, const char *newpath, bool overwrite = true);

    ///
    /// Open a file
    /// @param[in] pathname that has to be opened
    /// @param[in] openFlags modeled after open().  The specific set
    /// of flags currently supported are:
    /// O_CREAT, O_CREAT|O_EXCL, O_RDWR, O_RDONLY, O_WRONLY, O_TRUNC, O_APPEND
    /// @param[in] numReplicas if O_CREAT is specified, then this the
    /// desired degree of replication for the file
    /// @retval fd corresponding to the opened file; -errno on failure
    ///
    int Open(const char *pathname, int openFlags, int numReplicas = 3);

    ///
    /// Return file descriptor for an open file
    /// @param[in] pathname of file
    /// @retval file descriptor if open, error code < 0 otherwise
    int Fileno(const char *pathname);

    ///
    /// Close a file
    /// @param[in] fd that corresponds to a previously opened file
    /// table entry.
    ///
    int Close(int fd);

    ///
    /// Read/write the desired # of bytes to the file, starting at the
    /// "current" position of the file.
    /// @param[in] fd that corresponds to a previously opened file
    /// table entry.
    /// @param buf For read, the buffer will be filled with data; for
    /// writes, this buffer supplies the data to be written out.
    /// @param[in] numBytes   The # of bytes of I/O to be done.
    /// @retval On success, return of bytes of I/O done (>= 0);
    /// on failure, return status code (< 0).
    ///
    ssize_t Read(int fd, char *buf, size_t numBytes);
    ssize_t Write(int fd, const char *buf, size_t numBytes);

    ///
    /// \brief Sync out data that has been written (to the "current" chunk).
    /// @param[in] fd that corresponds to a file that was previously
    /// opened for writing.
    ///
    int Sync(int fd);

    /// \brief Adjust the current position of the file pointer similar
    /// to the seek() system call.
    /// @param[in] fd that corresponds to a previously opened file
    /// @param[in] offset offset to which the pointer should be moved
    /// relative to whence.
    /// @param[in] whence one of SEEK_CUR, SEEK_SET, SEEK_END
    /// @retval On success, the offset to which the filer
    /// pointer was moved to; (off_t) -1 on failure.
    ///
    off_t Seek(int fd, off_t offset, int whence);
    /// In this version of seek, whence == SEEK_SET
    off_t Seek(int fd, off_t offset);

    /// Return the current position of the file pointer in the file.
    /// @param[in] fd that corresponds to a previously opened file
    /// @retval value returned is analogous to calling ftell()
    off_t Tell(int fd);

    ///
    /// Truncate a file to the specified offset.
    /// @param[in] fd that corresponds to a previously opened file
    /// @param[in] offset  the offset to which the file should be truncated
    /// @retval status code
    int Truncate(int fd, off_t offset);

    ///
    /// Given a starting offset/length, return the location of all the
    /// chunks that cover this region.  By location, we mean the name
    /// of the chunkserver that is hosting the chunk. This API can be
    /// used for job scheduling.
    ///
    /// @param[in] pathname	The full pathname of the file such as /../foo
    /// @param[in] start	The starting byte offset
    /// @param[in] len		The length in bytes that define the region
    /// @param[out] locations	The location(s) of various chunks
    /// @retval status: 0 on success; -errno otherwise
    ///
    int GetDataLocation(const char *pathname, off_t start, size_t len,
                        std::vector< std::vector <std::string> > &locations);

    ///
    /// Get the degree of replication for the pathname.
    /// @param[in] pathname	The full pathname of the file such as /../foo
    /// @retval count
    ///
    int16_t GetReplicationFactor(const char *pathname);

    ///
    /// Set the degree of replication for the pathname.
    /// @param[in] pathname	The full pathname of the file such as /../foo
    /// @param[in] numReplicas  The desired degree of replication.
    /// @retval -1 on failure; on success, the # of replicas that will be made.
    ///
    int16_t SetReplicationFactor(const char *pathname, int16_t numReplicas);

    // Next sequence number for operations.
    // This is called in a thread safe manner.
    kfsSeq_t nextSeq() { return mCmdSeqNum++; }

private:
     /// Maximum # of files a client can have open.
    static const int MAX_FILES = 4096;

    /// Primitive support for concurrent access in the KFS client: at
    /// each entry point from the public interfaces, grab the mutex
    /// before doing any work.  This ensures that all requests to the
    /// meta/chunk servers are serialized.
    pthread_mutex_t mMutex;

    bool	mIsInitialized;
    /// where is the meta server located
    ServerLocation mMetaServerLoc;

    LeaseClerk  mLeaseClerk;

    /// a tcp socket that holds the connection with the server
    TcpSocket	mMetaServerSock;
    /// seq # that we send in each command
    kfsSeq_t	mCmdSeqNum;

    /// The current working directory in KFS
    std::string	mCwd;

    std::string mHostname;

    /// keep a table of open files/directory handles.
    std::vector <FileTableEntry *> mFileTable;

    /// Check that fd is in range
    bool valid_fd(int fd) { return (fd >= 0 && fd < MAX_FILES); }

    /// Connect to the meta server and return status.
    /// @retval true if connect succeeds; false otherwise.
    bool ConnectToMetaServer();

    /// Lookup the attributes of a file given its parent file-id
    /// @param[in] parentFid  file-id of the parent directory
    /// @param[in] filename   filename whose attributes are being
    /// asked
    /// @param[out] result   the resultant attributes
    /// @param[in] computeFilesize  when set, for files, the size of
    /// the file is computed and returned in result.fileSize
    /// @retval 0 on success; -errno otherwise
    ///
    int LookupAttr(kfsFileId_t parentFid, const char *filename,
		   KfsFileAttr &result, bool computeFilesize);

    /// Helper functions that operate on individual chunks.

    /// Allocate the "current" chunk of fd.
    /// @param[in] fd  The index from mFileTable[] that corresponds to
    /// the file being accessed
    int AllocChunk(int fd);

    /// Open the "current" chunk of fd.  This involves setting up the
    /// socket to the chunkserver and determining the size of the chunk.
    /// @param[in] fd  The index from mFileTable[] that corresponds to
    /// the file being accessed
    int OpenChunk(int fd);

    bool IsChunkReadable(int fd);

    /// Given a chunkid, is our lease on that chunk "good"?  That is,
    /// if it is close to expiring, renew it; if it is expired, get a
    /// new one.
    /// @param[in] chunkId  The chunk for which we are trying to get a
    /// "good" lease.
    /// @retval true if our lease is good; false otherwise.
    bool IsChunkLeaseGood(kfsChunkId_t chunkId);


    /// Helper function that reads from the "current" chunk.
    /// @param[in] fd  The file from which data is to be read
    /// @param[out] buf  The buffer which will be filled with data
    /// @param[in] numBytes  The desired # of bytes to be read
    /// @retval  On success, # of bytes read; -1 on error
    ///
    ssize_t ReadChunk(int fd, char *buf, size_t numBytes);

    /// Helper function that reads from the "current" chunk from the
    /// chunk server.  For performance, depending on the # of bytes to
    /// be read, the read could be pipelined to overlap disk/network
    /// transfer from the chunkserver.
    ///
    /// @param[in] fd  The file from which data is to be read
    /// @param[out] buf  The buffer which will be filled with data
    /// @param[in] numBytes  The desired # of bytes to be read
    /// @retval  On success, # of bytes read; -1 on error
    ///
    ssize_t ReadFromServer(int fd, char *buf, size_t numBytes);

    /// Helper function that does a single read op to the chunkserver
    /// to get data back.
    /// @param[in] fd  The file from which data is to be read
    /// @param[out] buf  The buffer which will be filled with data
    /// @param[in] numBytes  The desired # of bytes to be read
    /// @retval  On success, # of bytes read; -1 on error
    ///
    ssize_t DoSmallReadFromServer(int fd, char *buf, size_t numBytes);

    /// Helper function that breaks up a single read into a bunch of
    /// small reads and pipelines the read to reduce latency.
    ssize_t DoLargeReadFromServer(int fd, char *buf, size_t numBytes);

    /// Helper function that copies out data from the chunk buffer
    /// corresponding to the "current" chunk.
    /// @param[in] fd  The file from which data is to be read
    /// @param[out] buf  The buffer which will be filled with data
    /// @param[in] numBytes  The desired # of bytes to be read
    /// @retval  # of bytes copied ( value >= 0).
    ///
    size_t CopyFromChunkBuf(int fd, char *buf, size_t numBytes);

    /// Helper function that zero-fills a buffer whenever there are
    /// holes in a file.
    /// @param[in] fd  The file from which data is to be read
    /// @param[out] buf  The buffer which will be filled with 0
    /// @param[in] numBytes  The desired # of bytes to be filled with 0
    /// @retval  # of bytes copied ( value >= 0).
    ///
    size_t ZeroFillBuf(int fd, char *buf, size_t numBytes);

    /// Given a chunk, find out which chunk-server is hosting it.  It
    /// is possible that no server is hosting the chunk---if there is
    /// a hole in the file.
    /// @retval status code: 0 on success; < 0 => failure
    int LocateChunk(int fd, int chunkNum);


    // Helper functions to deal with write and buffering at the client.

    /// Write the data to the chunk buffer and ack the application.
    /// This can be used for doing delayed write-backs.
    /// @param[in] fd  The file to which data is to be written
    /// @param[out] buf  The buffer with the data to be written out
    /// @param[in] numBytes  The desired # of bytes to be written
    /// @retval  # of bytes written; -1 on failure
    ssize_t WriteToBuffer(int fd, const char *buf, size_t numBytes);

    /// Flush the contents of the chunk buffer back to the chunk
    /// server.
    /// @param[in] fd  The file to which data is to be written
    /// @retval  # of bytes written; -1 on failure
    ssize_t FlushBuffer(int fd);

    /// Helper function that does the write RPC.
    /// @param[in] fd  The file to which data is to be written
    /// @param[in] offset  The offset in the chunk at which data has
    /// to be written out
    /// @param[out] buf  The buffer with the data to be written out
    /// @param[in] numBytes  The desired # of bytes to be written
    /// @retval  # of bytes written; -1 on failure
    ///
    ssize_t WriteToServer(int fd, off_t offset, const char *buf, size_t numBytes);

    /// Helper function that does a single write op to the server.
    /// @param[in] fd  The file to which data is to be written
    /// @param[in] offset  The offset in the chunk at which data has
    /// to be written out
    /// @param[out] buf  The buffer with the data to be written out
    /// @param[in] numBytes  The desired # of bytes to be written
    /// @retval  # of bytes written; -1 on failure
    ///
    ssize_t DoSmallWriteToServer(int fd, off_t offset, const char *buf, size_t numBytes);

    /// Helper function that does a pipelined write to server.
    /// Basically, break a write into smaller writes and pipeline them.
    ssize_t DoLargeWriteToServer(int fd, off_t offset, const char *buf, size_t numBytes);

    /// Request a chunk allocation with the metaserver if necessary.
    /// The 2nd argument "forces" an allocation with the server.
    int DoAllocation(int fd, bool force = false);

    // Return true if the attributes of the "current" chunk are
    // known, i.e., they were downloaded from meta server.
    bool IsCurrChunkAttrKnown(int fd);

    /// Get the size of the "current" chunk from the chunkserver.
    /// @param[in] fd  The index in mFileTable[] that corresponds to a
    /// previously opened file.
    int SizeChunk(int fd);

    /// Given a kfsfid with some # of chunks, compute the size of the
    /// file.  This involves looking up the size of the last chunk of
    /// the file and then adding with the size of the remaining (full) chunks.
    ssize_t ComputeFilesize(kfsFileId_t kfsfid);

    FileTableEntry *FdInfo(int fd) { return mFileTable[fd]; }
    FilePosition *FdPos(int fd) { return &FdInfo(fd)->currPos; }
    FileAttr *FdAttr(int fd) { return &FdInfo(fd)->fattr; }
    ChunkBuffer *FdBuffer(int fd) { return &FdInfo(fd)->buffer; }
    ChunkAttr *GetCurrChunk(int fd) {
	return &FdInfo(fd)->cattr[FdPos(fd)->chunkNum];
    }

    /// Do the work for an op with the metaserver; if the metaserver
    /// dies in the middle, retry the op a few times before giving up.
    int DoMetaOpWithRetry(KfsOp *op);

    /// Do the work for pipelined read: send a few
    /// requests to plumb the pipe and then whenever an op finishes,
    /// submit a new one.
    int DoPipelinedRead(std::vector<ReadOp *> &ops, TcpSocket *sock);
    int DoPipelinedWrite(int fd, std::vector<WritePrepareOp *> &ops);

    /// Helpers for pipelined write
    int PushDataForWrite(int fd, WritePrepareOp *op);
    int IssueWriteCommit(int fd, WritePrepareOp *op, WriteSyncOp **sop,
			 TcpSocket *masterSock);

    /// Get a response from the server, where, the response is
    /// terminated by "\r\n\r\n".
    int GetResponse(char *buf, int bufSize, int *delims, TcpSocket *sock);

    /// Given a path, get the parent fileid and the name following the
    /// trailing "/"
    int GetPathComponents(const char *pathname, kfsFileId_t *parentFid,
			  std::string &name);

    /// File table management utilities: find a free entry in the
    /// table, find the entry corresponding to a pathname, "mark" an
    /// entry in the table as in use, and "mark" an entry in the table
    /// as free.
    int FindFreeFileTableEntry();

    /// Wrapper function that calls LookupFileTableEntry with the parentFid
    int LookupFileTableEntry(const char *pathname);

    /// Return the file table entry corresponding to parentFid/name,
    /// where "name" is either a file or directory that resides in
    /// directory corresponding to parentFid.
    int LookupFileTableEntry(kfsFileId_t parentFid, const char *name);

    /// Given a parent fid and name, get the corresponding entry in
    /// the file table.  Note: if needed, attributes will be
    /// downloaded from the server.
    int Lookup(kfsFileId_t parentFid, const char *name);

    int ClaimFileTableEntry(kfsFileId_t parentFid, const char *name);
    int AllocFileTableEntry(kfsFileId_t parentFid, const char *name);
    void ReleaseFileTableEntry(int fte);

    /// Helper functions that interact with the leaseClerk to
    /// get/renew leases
    int GetLease(kfsChunkId_t chunkId);
    void RenewLease(kfsChunkId_t chunkId);

};

/// Given a error status code, return a string describing the error.
/// @param[in] status  The status code for an error.
/// @retval String that describes what the error is.
extern std::string ErrorCodeToStr(int status);

extern KfsClient *getKfsClient();

}

#endif // LIBKFSCLIENT_KFSCLIENT_H
