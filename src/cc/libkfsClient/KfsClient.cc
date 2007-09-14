//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsClient/KfsClient.cc#5 $
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
// \file KfsClient.cc
// \brief Kfs Client-library code.
//
//----------------------------------------------------------------------------

#include "KfsClient.h"

#include "common/config.h"
#include "common/properties.h"
#include "common/log.h"
#include "meta/kfstypes.h"
#include "libkfsIO/Checksum.h"
#include "Utils.h"

extern "C" {
#include <signal.h>
}
#include <cerrno>
#include <iostream>
#include <string>

using std::string;
using std::ostringstream;
using std::istringstream;
using std::min;
using std::max;
using std::map;
using std::vector;

using std::cout;
using std::endl;

using namespace KFS;

const int CMD_BUF_SIZE = 1024;

// Set the default timeout for server I/O's to be 5 mins for now.
// This is intentionally large so that we can do stuff in gdb and not
// have the client timeout in the midst of a debug session.
struct timeval gDefaultTimeout = {300, 0};

namespace {
    Properties & theProps()
    {
        static Properties p;
        return p;
    }
}   

KfsClient *
KFS::getKfsClient()
{
    return KfsClient::Instance();
}

KfsClient::KfsClient()
{
    pthread_mutexattr_t mutexAttr;
    int rval;
    const int hostnamelen = 256;
    char hostname[hostnamelen];

    if (gethostname(hostname, hostnamelen)) {
        perror("gethostname: ");
        exit(-1);
    }

    mHostname = hostname;

    // store the entry for "/"
    int UNUSED_ATTR rootfte = ClaimFileTableEntry(KFS::ROOTFID, "/");
    assert(rootfte == 0);
    mFileTable[0]->fattr.fileId = KFS::ROOTFID;
    mFileTable[0]->fattr.isDirectory = true;

    mCwd = "/";
    mIsInitialized = false;
    mCmdSeqNum = 0;

    // Setup the mutex to allow recursive locking calls.  This
    // simplifies things when a public method (eg., read) in KFS client calls
    // another public method (eg., seek) and both need to take the lock
    rval = pthread_mutexattr_init(&mutexAttr);
    assert(rval == 0);
    rval = pthread_mutexattr_settype(&mutexAttr, PTHREAD_MUTEX_RECURSIVE);
    assert(rval == 0);
    rval = pthread_mutex_init(&mMutex, &mutexAttr);
    assert(rval == 0);

    // whenever a socket goes kaput, don't crash the app
    signal(SIGPIPE, SIG_IGN);

    // for random # generation, seed it
    srand(getpid());
}

int
KfsClient::Init(const char *propFile)
{
    bool verbose = false;
#ifdef DEBUG
    verbose = true;
#endif

    if (mIsInitialized)
        return 0;

    if (theProps().loadProperties(propFile, '=', verbose) != 0) {
	mIsInitialized = false;
	return -1;
    }

    return Init(theProps().getValue("metaServer.name", ""),
                theProps().getValue("metaServer.port", -1));
    
}

int KfsClient::Init(const string metaServerHost, int metaServerPort)
{
    mMetaServerLoc.hostname = metaServerHost;
    mMetaServerLoc.port = metaServerPort;

    if (!mMetaServerLoc.IsValid()) {
	mIsInitialized = false;
	return -1;
    }

    if (!ConnectToMetaServer()) {
	mIsInitialized = false;
	return -1;
    }
    mIsInitialized = true;
    return 0;
}

bool
KfsClient::ConnectToMetaServer()
{
    return mMetaServerSock.Connect(mMetaServerLoc) >= 0;
}


/// A notion of "cwd" in KFS.
///
int
KfsClient::Cd(const char *pathname)
{
    MutexLock l(&mMutex);

    struct stat s;
    string path = build_path(mCwd, pathname);
    int status = Stat(path.c_str(), s);

    if (status < 0) {
	COSMIX_LOG_DEBUG("Non-existent path: %s", pathname);
	return -ENOENT;
    }

    if (!S_ISDIR(s.st_mode)) {
	COSMIX_LOG_DEBUG("Non-existent dir: %s", pathname);
	return -ENOTDIR;
    }

    mCwd = path;
    return 0;
}


///
/// Make a directory hierarchy in KFS.
///
int
KfsClient::Mkdirs(const char *pathname)
{
    MutexLock l(&mMutex);

    int res;
    string path = pathname;
    string component;
    const char slash = '/';
    string::size_type startPos = 1, endPos;
    bool done = false;

    //
    // Walk from the root down to the last part of the path making the
    // directory hierarchy along the way.  If any of the components of
    // the path is a file, error out.
    //
    while (!done) {
        endPos = path.find(slash, startPos);
        if (endPos == string::npos) {
            done = true;
            component = pathname;
        } else {
            component = path.substr(0, endPos);
            startPos = endPos + 1;
        }
	if (Exists(component.c_str())) {
	    if (IsFile(component.c_str()))
		return -ENOTDIR;
	    continue;
	}
	res = Mkdir(component.c_str());
	if (res < 0)
	    return res;
    }

    return 0;
}
 
///
/// Make a directory in KFS.
/// @param[in] pathname		The full pathname such as /.../dir
/// @retval 0 if mkdir is successful; -errno otherwise
int
KfsClient::Mkdir(const char *pathname)
{
    MutexLock l(&mMutex);

    kfsFileId_t parentFid;
    string dirname;
    int res = GetPathComponents(pathname, &parentFid, dirname);
    if (res < 0)
	return res;

    MkdirOp op(nextSeq(), parentFid, dirname.c_str());
    DoMetaOpWithRetry(&op);
    if (op.status < 0) {
	return op.status;
    }

    // Everything is good now...
    int fte = ClaimFileTableEntry(parentFid, dirname.c_str());
    if (fte < 0)	// Too many open files
	return -EMFILE;

    mFileTable[fte]->fattr.fileId = op.fileId;
    // setup the times and such
    mFileTable[fte]->fattr.Init(true);

    return 0;
}

///
/// Remove a directory in KFS.
/// @param[in] pathname		The full pathname such as /.../dir
/// @retval 0 if rmdir is successful; -errno otherwise
int
KfsClient::Rmdir(const char *pathname)
{
    MutexLock l(&mMutex);

    string dirname;
    kfsFileId_t parentFid;
    int res = GetPathComponents(pathname, &parentFid, dirname);
    if (res < 0)
	return res;

    int fte = LookupFileTableEntry(parentFid, dirname.c_str());
    if (fte > 0)
	ReleaseFileTableEntry(fte);

    RmdirOp op(nextSeq(), parentFid, dirname.c_str());
    (void)DoMetaOpWithRetry(&op);
    return op.status;
}

///
/// Read a directory's contents.  This is analogous to READDIR in
/// NFS---just reads the directory contents and returns the names;
/// you'll need to lookup the attributes next.
///
/// XXX NFS READDIR also returns the file ids, and we should do
/// the same here.
///
/// @param[in] pathname	The full pathname such as /.../dir
/// @param[out] result	The filenames in the directory
/// @retval 0 if readdir is successful; -errno otherwise
int
KfsClient::Readdir(const char *pathname, vector<string> &result)
{
    MutexLock l(&mMutex);

    int fte = LookupFileTableEntry(pathname);
    if (fte < 0) {
	// open the directory for reading
	fte = Open(pathname, O_RDONLY);
    }

    if (fte < 0)
	return fte;

    if (!mFileTable[fte]->fattr.isDirectory)
	return -ENOTDIR;

    kfsFileId_t dirFid = mFileTable[fte]->fattr.fileId;

    ReaddirOp op(nextSeq(), dirFid);
    DoMetaOpWithRetry(&op);
    int res = op.status;
    if (res < 0)
	return res;

    istringstream ist;
    char filename[MAX_FILENAME_LEN];
    assert(op.contentBuf != NULL);
    ist.str(op.contentBuf);
    result.resize(op.numEntries);
    for (int i = 0; i < op.numEntries; ++i) {
	// ist >> result[i];
	ist.getline(filename, MAX_FILENAME_LEN);
	result[i] = filename;
        COSMIX_LOG_DEBUG("Entry: %s", filename);
    }
    return res;
}

///
/// Read a directory's contents and get the attributes.  This is
/// analogous to READDIRPLUS in NFS.
///
/// @param[in] pathname	The full pathname such as /.../dir
/// @param[out] result	The filenames in the directory and their attributes
/// @retval 0 if readdir is successful; -errno otherwise
int
KfsClient::ReaddirPlus(const char *pathname, vector<KfsFileAttr> &result)
{
    MutexLock l(&mMutex);

    int fte = LookupFileTableEntry(pathname);
    if (fte < 0)	 // open the directory for reading
	fte = Open(pathname, O_RDONLY);
    if (fte < 0)
	   return fte;

    FileAttr *fa = FdAttr(fte);
    if (!fa->isDirectory)
	return -ENOTDIR;

    kfsFileId_t dirFid = fa->fileId;

    ReaddirOp op(nextSeq(), dirFid);
    (void)DoMetaOpWithRetry(&op);
    int res = op.status;
    if (res < 0) {
	return res;
    }

    istringstream ist;
    char filename[MAX_FILENAME_LEN];
    ist.str(op.contentBuf);
    result.resize(op.numEntries);
    for (int i = 0; i < op.numEntries; ++i) {
	ist.getline(filename, MAX_FILENAME_LEN);
	result[i].filename = filename;
        COSMIX_LOG_DEBUG("Entry: %s", filename);
        // get the file size for files
	LookupAttr(dirFid, result[i].filename.c_str(), result[i], true);
    }

    return res;
}

int
KfsClient::Stat(const char *pathname, struct stat &result, bool computeFilesize)
{
    MutexLock l(&mMutex);

    KfsFileAttr kfsattr;

    int fte = LookupFileTableEntry(pathname);
    if (fte >= 0) {
	kfsattr = mFileTable[fte]->fattr;
    } else {
	kfsFileId_t parentFid;
	string filename;
	int res = GetPathComponents(pathname, &parentFid, filename);
	if (res == 0)
	    res = LookupAttr(parentFid, filename.c_str(), kfsattr, computeFilesize);
	if (res < 0)
	    return res;
    }

    memset(&result, 0, sizeof (struct stat));
    result.st_mode = kfsattr.isDirectory ? S_IFDIR : S_IFREG;
    result.st_size = kfsattr.fileSize;
    result.st_atime = kfsattr.crtime.tv_sec;
    result.st_mtime = kfsattr.mtime.tv_sec;
    result.st_ctime = kfsattr.ctime.tv_sec;
    return 0;
}

bool
KfsClient::Exists(const char *pathname)
{
    MutexLock l(&mMutex);

    struct stat dummy;

    return Stat(pathname, dummy, false) == 0;
}

bool
KfsClient::IsFile(const char *pathname)
{
    MutexLock l(&mMutex);

    struct stat statInfo;

    if (Stat(pathname, statInfo, false) != 0)
	return false;
    
    return S_ISREG(statInfo.st_mode);
}

bool
KfsClient::IsDirectory(const char *pathname)
{
    MutexLock l(&mMutex);

    struct stat statInfo;

    if (Stat(pathname, statInfo, false) != 0)
	return false;
    
    return S_ISDIR(statInfo.st_mode);
}

int
KfsClient::LookupAttr(kfsFileId_t parentFid, const char *filename,
	              KfsFileAttr &result, bool computeFilesize)
{
    MutexLock l(&mMutex);

    if (parentFid < 0)
	return -EINVAL;

    LookupOp op(nextSeq(), parentFid, filename);
    (void)DoMetaOpWithRetry(&op);
    if (op.status < 0)
	return op.status;

    result = op.fattr;
    if ((!result.isDirectory) && computeFilesize)
	result.fileSize = ComputeFilesize(result.fileId);
    else
        result.fileSize = 0;

    return op.status;
}

int
KfsClient::Create(const char *pathname, int numReplicas)
{
    MutexLock l(&mMutex);

    kfsFileId_t parentFid;
    string filename;
    int res = GetPathComponents(pathname, &parentFid, filename);
    if (res < 0) {
	COSMIX_LOG_DEBUG("status %d for pathname %s", res, pathname);
	return res;
    }

    if (filename.size() >= MAX_FILENAME_LEN)
	return -ENAMETOOLONG;

    CreateOp op(nextSeq(), parentFid, filename.c_str(), numReplicas);
    (void)DoMetaOpWithRetry(&op);
    if (op.status < 0) {
	COSMIX_LOG_DEBUG("status %ld from create RPC", op.status);
	return op.status;
    }

    // Everything is good now...
    int fte = ClaimFileTableEntry(parentFid, filename.c_str());
    if (fte < 0) {	// XXX Too many open files
	COSMIX_LOG_DEBUG("status %d from ClaimFileTableEntry", fte);
	return fte;
    }

    FileAttr *fa = FdAttr(fte);
    fa->fileId = op.fileId;
    fa->Init(false);	// is an ordinary file

    FdInfo(fte)->openMode = O_RDWR;

    return fte;
}

int
KfsClient::Remove(const char *pathname)
{
    MutexLock l(&mMutex);

    kfsFileId_t parentFid;
    string filename;
    int res = GetPathComponents(pathname, &parentFid, filename);
    if (res < 0)
	return res;

    int fte = LookupFileTableEntry(parentFid, filename.c_str());
    if (fte > 0)
	ReleaseFileTableEntry(fte);

    RemoveOp op(nextSeq(), parentFid, filename.c_str());
    (void)DoMetaOpWithRetry(&op);
    return op.status;
}

int
KfsClient::Rename(const char *oldpath, const char *newpath, bool overwrite)
{
    MutexLock l(&mMutex);

    kfsFileId_t parentFid;
    string oldfilename;
    int res = GetPathComponents(oldpath, &parentFid, oldfilename);
    if (res < 0)
	return res;

    int fte = LookupFileTableEntry(parentFid, oldfilename.c_str());
    if (fte > 0)
	ReleaseFileTableEntry(fte);

    string absNewpath = build_path(mCwd, newpath);
    RenameOp op(nextSeq(), parentFid, oldfilename.c_str(),
		    absNewpath.c_str(), overwrite);
    (void)DoMetaOpWithRetry(&op);
    return op.status;
}

int
KfsClient::Fileno(const char *pathname)
{
    kfsFileId_t parentFid;
    string filename;
    int res = GetPathComponents(pathname, &parentFid, filename);
    if (res < 0)
	return res;

    return LookupFileTableEntry(parentFid, filename.c_str());
}

int
KfsClient::Open(const char *pathname, int openMode, int numReplicas)
{
    MutexLock l(&mMutex);

    kfsFileId_t parentFid;
    string filename;
    int res = GetPathComponents(pathname, &parentFid, filename);
    if (res < 0)
	return res;

    if (filename.size() >= MAX_FILENAME_LEN)
	return -ENAMETOOLONG;

    // avoid unnecesary lookups
    int fte = LookupFileTableEntry(parentFid, filename.c_str());
    if (fte >= 0)
	return fte;

    LookupOp op(nextSeq(), parentFid, filename.c_str());
    (void)DoMetaOpWithRetry(&op);
    if (op.status < 0) {
	if (openMode & O_CREAT) {
	    // file doesn't exist.  Create it
	    return Create(pathname, numReplicas);
	}
	return op.status;
    }

    fte = ClaimFileTableEntry(parentFid, filename.c_str());
    if (fte < 0)		// Too many open files
	return fte;

    if (openMode & O_RDWR)
	mFileTable[fte]->openMode = O_RDWR;
    else if (openMode & O_WRONLY)
	mFileTable[fte]->openMode = O_WRONLY;
    else
	mFileTable[fte]->openMode = O_RDONLY;

    // We got a path...get the fattr
    mFileTable[fte]->fattr = op.fattr;

    if (mFileTable[fte]->fattr.chunkCount > 0) {
	mFileTable[fte]->fattr.fileSize =
	    ComputeFilesize(op.fattr.fileId);
    }

    if (openMode & O_TRUNC)
	Truncate(fte, 0);

    if (openMode & O_APPEND)
	Seek(fte, 0, SEEK_END);

    return fte;
}

int
KfsClient::Close(int fd)
{
    MutexLock l(&mMutex);

    if (!valid_fd(fd))
	return -EBADF;

    if (mFileTable[fd]->buffer.dirty) {
	int status = FlushBuffer(fd);
	if (status < 0)
	    return status;
    }
    ReleaseFileTableEntry(fd);
    return 0;
}

int
KfsClient::Sync(int fd)
{
    MutexLock l(&mMutex);

    if (!valid_fd(fd))
	return -EBADF;

    if (mFileTable[fd]->buffer.dirty) {
       int status = FlushBuffer(fd);
       if (status < 0)
	   return status;
    }
    return 0;
}

int
KfsClient::Truncate(int fd, off_t offset)
{
    MutexLock l(&mMutex);

    if (!valid_fd(fd))
	return -EBADF;

    // for truncation, file should be opened for writing
    if (mFileTable[fd]->openMode == O_RDONLY)
	return -EBADF;

    ChunkBuffer *cb = FdBuffer(fd);
    if (cb->dirty) {
	int res = FlushBuffer(fd);
	if (res < 0)
	    return res;
    }

    // invalidate buffer in case it is past new EOF
    cb->invalidate();
    FilePosition *pos = FdPos(fd);
    pos->chunkServerSock = NULL;

    FileAttr *fa = FdAttr(fd);
    TruncateOp op(nextSeq(), fa->fileId, offset);
    (void)DoMetaOpWithRetry(&op);
    int res = op.status;

    if (res == 0) {
	fa->fileSize = offset;
	if (fa->fileSize == 0)
	    fa->chunkCount = 0;
	// else
	// chunkcount is off...but, that is ok; it is never exposed to
	// the end-client.

	gettimeofday(&fa->mtime, NULL);
	// force a re-lookup of locations
	FdInfo(fd)->cattr.clear();
    }
    return res;
}

int
KfsClient::GetDataLocation(const char *pathname, off_t start, size_t len,
                           vector< vector <string> > &locations)
{
    MutexLock l(&mMutex);

    int res, fd;
    bool didOpen = false;

    // Non-existent
    if (!IsFile(pathname)) 
        return -ENOENT;

    // load up the fte
    fd = LookupFileTableEntry(pathname);
    if (fd < 0) {
        // Open the file for reading...this'll get the attributes setup
        fd = Open(pathname, O_RDONLY);
        // we got too many open files?
        if (fd < 0)
            return fd;
        didOpen = true;
    }

    // locate each chunk and get the hosts that are storing the chunk.
    for (size_t pos = start; pos < start + len; pos += KFS::CHUNKSIZE) {
        ChunkAttr *chunkAttr;
        int chunkNum = pos / KFS::CHUNKSIZE;

        if ((res = LocateChunk(fd, chunkNum)) < 0) {
            if (didOpen)
                Close(fd);
            return res;
        }

        chunkAttr = &(mFileTable[fd]->cattr[chunkNum]);
        
        vector<string> hosts;
        for (vector<string>::size_type i = 0; i < chunkAttr->chunkServerLoc.size(); i++)
            hosts.push_back(chunkAttr->chunkServerLoc[i].hostname);

        locations.push_back(hosts);
    }

    if (didOpen)
        Close(fd);

    return 0;
}


off_t
KfsClient::Seek(int fd, off_t offset)
{
    return Seek(fd, offset, SEEK_SET);
}

off_t
KfsClient::Seek(int fd, off_t offset, int whence)
{
    MutexLock l(&mMutex);

    if (!valid_fd(fd) || mFileTable[fd]->fattr.isDirectory)
	return (off_t) -EBADF;

    FilePosition *pos = FdPos(fd);
    off_t newOff;
    switch (whence) {
    case SEEK_SET:
	newOff = offset;
	break;
    case SEEK_CUR:
	newOff = pos->fileOffset + offset;
	break;
    case SEEK_END:
	newOff = mFileTable[fd]->fattr.fileSize + offset;
	break;
    default:
	return (off_t) -EINVAL;
    }

    int32_t chunkNum = newOff / KFS::CHUNKSIZE;
    // If we are changing chunks, we need to reset the socket so that
    // it eventually points to the right place
    if (pos->chunkNum != chunkNum) {
	ChunkBuffer *cb = FdBuffer(fd);
	if (cb->dirty) {
	    FlushBuffer(fd);
	}
	assert(!cb->dirty);
	pos->chunkServerSock = NULL;
    }

    pos->fileOffset = newOff;
    pos->chunkNum = chunkNum;
    pos->chunkOffset = newOff % KFS::CHUNKSIZE;

    return newOff;
}

off_t KfsClient::Tell(int fd)
{
    MutexLock l(&mMutex);

    return mFileTable[fd]->currPos.fileOffset;
}

///
/// Send a request to the meta server to allocate a chunk.
/// @param[in] fd   The index for an entry in mFileTable[] for which
/// space should be allocated.
/// @param[in] numBytes  The # of bytes we will write to this file
/// @retval 0 if successful; -errno otherwise
///
int
KfsClient::AllocChunk(int fd)
{
    FileAttr *fa = FdAttr(fd);
    assert(valid_fd(fd) && !fa->isDirectory);

    AllocateOp op(nextSeq(), fa->fileId);
    FilePosition *pos = FdPos(fd);
    op.fileOffset = ((pos->fileOffset / KFS::CHUNKSIZE) * KFS::CHUNKSIZE);

    (void) DoMetaOpWithRetry(&op);
    if (op.status < 0) {
	COSMIX_LOG_DEBUG("AllocChunk(%ld)", op.status);
	return op.status;
    }
    ChunkAttr chunk;
    chunk.chunkId = op.chunkId;
    chunk.chunkVersion = op.chunkVersion;
    chunk.chunkServerLoc = op.chunkServers;
    FdInfo(fd)->cattr[pos->chunkNum] = chunk;

    COSMIX_LOG_DEBUG("Fileid: %ld, chunk : %ld, version: %ld, hosted on:",
                     fa->fileId, chunk.chunkId, chunk.chunkVersion);

    for (uint32_t i = 0; i < op.chunkServers.size(); i++) {
	COSMIX_LOG_DEBUG("%s", op.chunkServers[i].ToString().c_str());
    }

    return op.status;
}

///
/// Given a chunk of file, find out where the chunk is hosted.
/// @param[in] fd  The index for an entry in mFileTable[] for which
/// we are trying find out chunk location info.
///
/// @param[in] chunkNum  The index in
/// mFileTable[fd]->cattr[] corresponding to the chunk for
/// which we are trying to get location info.
///
///
int
KfsClient::LocateChunk(int fd, int chunkNum)
{
    assert(valid_fd(fd) && !mFileTable[fd]->fattr.isDirectory);

    if (chunkNum < 0)
	return -EINVAL;

    map <int, ChunkAttr>::iterator c;
    c = mFileTable[fd]->cattr.find(chunkNum);

    // Avoid unnecessary look ups.
    if (c != mFileTable[fd]->cattr.end() && c->second.chunkId > 0)
	return 0;

    GetAllocOp op(nextSeq(), mFileTable[fd]->fattr.fileId,
		  (off_t) chunkNum * KFS::CHUNKSIZE);
    (void)DoMetaOpWithRetry(&op);
    if (op.status < 0) {
	string errstr = ErrorCodeToStr(op.status);
	COSMIX_LOG_DEBUG("LocateChunk (%ld): %s", op.status, errstr.c_str());
	return op.status;
    }

    ChunkAttr chunk;
    chunk.chunkId = op.chunkId;
    chunk.chunkVersion = op.chunkVersion;
    chunk.chunkServerLoc = op.chunkServers;
    mFileTable[fd]->cattr[chunkNum] = chunk;

    if (op.chunkServers.size() > 0) {
	COSMIX_LOG_DEBUG("Fileid: %ld, chunk: %ld, hosted on (%s)",
	                 mFileTable[fd]->fattr.fileId,
	                 chunk.chunkId,
	                 op.chunkServers[0].ToString().c_str());
    }
    return 0;
}

bool
KfsClient::IsCurrChunkAttrKnown(int fd)
{
    map <int, ChunkAttr> *c = &FdInfo(fd)->cattr;
    return c->find(FdPos(fd)->chunkNum) != c->end();
}

///
/// Helper function that does the work for sending out an op to the
/// server.
///
/// @param[in] op the op to be sent out
/// @param[in] sock the socket on which we communicate with server
/// @retval 0 on success; -1 on failure
///
int
KFS::DoOpSend(KfsOp *op, TcpSocket *sock)
{
    std::ostringstream os;

    if ((sock == NULL ) || (!sock->IsGood())) {
	COSMIX_LOG_DEBUG("Trying to do I/O on a closed socket..failing it");
	op->status = -EHOSTUNREACH;
	return -1;
    }

    op->Request(os);
    int numIO = sock->DoSynchSend(os.str().c_str(), os.str().length());
    if (numIO <= 0) {
	sock->Close();
	COSMIX_LOG_DEBUG("Send failed...closing socket");
	op->status = -EHOSTUNREACH;
	return -1;
    }
    if (op->contentLength > 0) {
	numIO = sock->DoSynchSend(op->contentBuf, op->contentLength);
	if (numIO <= 0) {
	    sock->Close();
	    COSMIX_LOG_DEBUG("Send failed...closing socket");
	    op->status = -EHOSTUNREACH;
	    return -1;
	}
    }
    return 0;
}

/// Get a response from the server.  The response is assumed to
/// terminate with "\r\n\r\n".
/// @param[in/out] buf that should be filled with data from server
/// @param[in] bufSize size of the buffer
///
/// @param[out] delims the position in the buffer where "\r\n\r\n"
/// occurs; in particular, the length of the response string that ends
/// with last "\n" character.  If the buffer got full and we couldn't
/// find "\r\n\r\n", delims is set to -1.
///
/// @param[in] sock the socket from which data should be read
/// @retval # of bytes that were read; 0/-1 if there was an error
///
static int
GetResponse(char *buf, int bufSize, int *delims, TcpSocket *sock)
{
    *delims = -1;

    while (1) {
	struct timeval timeout = gDefaultTimeout;

	int nread = sock->DoSynchPeek(buf, bufSize, timeout);
	if (nread <= 0)
	    return nread;

	for (int i = 4; i <= nread; i++) {
	    if (i < 4)
		break;
	    if ((buf[i - 3] == '\r') &&
		(buf[i - 2] == '\n') &&
		(buf[i - 1] == '\r') &&
		(buf[i] == '\n')) {
		// valid stuff is from 0..i; so, length of resulting
		// string is i+1.
		memset(buf, '\0', bufSize);
		*delims = (i + 1);
		nread = sock->Recv(buf, *delims);
		return nread;
	    }
	}
    }
    return -ENOBUFS;
}

///
/// From a response, extract out seq # and content-length.
///
static void
GetSeqContentLen(const char *resp, int respLen,
	         kfsSeq_t *seq, int *contentLength)
{
    string respStr(resp, respLen);
    Properties prop;
    istringstream ist(respStr);
    const char separator = ':';

    prop.loadProperties(ist, separator, false);
    *seq = prop.getValue("Cseq", (kfsSeq_t) -1);
    *contentLength = prop.getValue("Content-length", 0);
}

///
/// Helper function that does the work of getting a response from the
/// server and parsing it out.
///
/// @param[in] op the op for which a response is to be gotten
/// @param[in] sock the socket on which we communicate with server
/// @retval 0 on success; -1 on failure
///
int
KFS::DoOpResponse(KfsOp *op, TcpSocket *sock)
{
    int numIO;
    char buf[CMD_BUF_SIZE];
    int nread = 0, len;
    ssize_t navail, nleft;
    kfsSeq_t resSeq;
    int contentLen;

    if ((sock == NULL) || (!sock->IsGood())) {
	op->status = -EHOSTUNREACH;
	COSMIX_LOG_DEBUG("Trying to do I/O on a closed socket..failing it");
	return -1;
    }

    while (1) {
	memset(buf, '\0', CMD_BUF_SIZE);

	numIO = GetResponse(buf, CMD_BUF_SIZE, &len, sock);

	assert(numIO != -ENOBUFS);

	if (numIO <= 0) {
	    if (numIO == -ENOBUFS) {
		op->status = -1;
	    } else if (numIO == -ETIMEDOUT) {
		op->status = -ETIMEDOUT;
		COSMIX_LOG_DEBUG("Get response recv timed out...");
	    } else {
		COSMIX_LOG_DEBUG("Get response failed...closing socket");
		sock->Close();
		op->status = -EHOSTUNREACH;
	    }

	    return -1;
	}

	assert(len > 0);

	GetSeqContentLen(buf, len, &resSeq, &contentLen);

	if (resSeq == op->seq) {
	    break;
	}
	COSMIX_LOG_DEBUG("Seq #'s dont match: Expect: %ld, got: %ld",
                         op->seq, resSeq);
        // assert(!"Seq # mismatch");

        if (contentLen > 0) {
            struct timeval timeout = gDefaultTimeout;
            int len = sock->DoSynchDiscard(contentLen, timeout);
            if (len != contentLen) {
                sock->Close();
                op->status = -EHOSTUNREACH;
                return -1;
            }
        }
    }

    contentLen = op->contentLength;

    op->ParseResponseHeader(buf, len);

    if (op->contentLength == 0) {
	// restore it back: when a write op is sent out and this
	// method is invoked with the same op to get the response, the
	// op's status should get filled in; we shouldn't be stomping
	// over content length.
	op->contentLength = contentLen;
	return numIO;
    }

    // This is the annoying part...we may have read some of the data
    // related to attributes already.  So, copy them out and then read
    // whatever else is left

    if (op->contentBufLen == 0) {
	op->contentBuf = new char[op->contentLength];
    }

    // len bytes belongs to the RPC reply.  Whatever is left after
    // stripping that data out is the data.
    navail = numIO - len;
    if (navail > 0) {
	assert(navail <= (ssize_t)op->contentLength);
	memcpy(op->contentBuf, buf + len, navail);
    }
    nleft = op->contentLength - navail;

    assert(nleft >= 0);

    if (nleft > 0) {
	struct timeval timeout = gDefaultTimeout;

	nread = sock->DoSynchRecv(op->contentBuf + navail, nleft, timeout);
	if (nread == -ETIMEDOUT) {
	    COSMIX_LOG_DEBUG("Recv timed out...");
	    op->status = -ETIMEDOUT;
	} else if (nread <= 0) {
	    COSMIX_LOG_DEBUG("Recv failed...closing socket");
	    op->status = -EHOSTUNREACH;
	    sock->Close();
	}

	if (nread <= 0) {
	    return 0;
	}
    }

    return nread + numIO;
}


///
/// Common work for each op: build a request; send it to server; get a
/// response; parse it.
///
/// @param[in] op the op to be done
/// @param[in] sock the socket on which we communicate with server
///
/// @retval # of bytes read from the server.
///
int
KFS::DoOpCommon(KfsOp *op, TcpSocket *sock)
{
    if (sock == NULL) {
	COSMIX_LOG_DEBUG("%s: send failed; no socket", op->Show().c_str());
	assert(sock);
	return -EHOSTUNREACH;
    }

    int res = DoOpSend(op, sock);
    if (res < 0) {
	COSMIX_LOG_DEBUG("%s: send failure code: %d", op->Show().c_str(), res);
	return res;
    }

    res = DoOpResponse(op, sock);

    if (res < 0) {
	COSMIX_LOG_DEBUG("%s: recv failure code: %d", op->Show().c_str(), res);
	return res;
    }

    if (op->status < 0) {
	string errstr = ErrorCodeToStr(op->status);
	COSMIX_LOG_DEBUG("%s failed with code: %s", op->Show().c_str(), errstr.c_str());
    }

    return res;
}

///
/// To compute the size of a file, determine what the last chunk in
/// the file happens to be (from the meta server); then, for the last
/// chunk, find its size and then add the size of remaining chunks
/// (all of which are assumed to be full).  The reason for asking the
/// meta server about the last chunk (and simply using chunkCount) is
/// that random writes with seeks affect where the "last" chunk of the
/// file happens to be: for instance, a file could have chunkCount = 1, but
/// that chunk could be the 10th chunk in the file---the first 9
/// chunks are just holes.
//
struct RespondingServer {
    KfsClient *client;
    const ChunkLayoutInfo &layout;
    int *status;
    ssize_t *size;
    RespondingServer(KfsClient *cli, const ChunkLayoutInfo &lay,
		     ssize_t *sz, int *st):
	    client(cli), layout(lay), status(st), size(sz) { }
    bool operator() (ServerLocation loc)
    {
	TcpSocket *sock = client->GetChunkServerSocket(loc);
	if (sock == NULL)
	    return false;

	SizeOp sop(client->nextSeq(), layout.chunkId, layout.chunkVersion);
	int numIO = DoOpCommon(&sop, sock);
	if (numIO < 0 && !sock->IsGood())
	    return false;

	*status = sop.status;
	if (*status >= 0)
		*size = sop.size;
	return true;
    }
};

ssize_t
KfsClient::ComputeFilesize(kfsFileId_t kfsfid)
{
    GetLayoutOp lop(nextSeq(), kfsfid);
    (void)DoMetaOpWithRetry(&lop);
    if (lop.status < 0) {
	// XXX: This can only during concurrent I/O when someone is
	// deleting a file and we are trying to compute the size of
	// this file.  For now, assert away.
	assert(lop.status != -ENOENT);
	return 0;
    }

    if (lop.ParseLayoutInfo()) {
	COSMIX_LOG_DEBUG("Unable to parse layout info");
	return -1;
    }
    COSMIX_LOG_DEBUG("Fileid: %ld, # of chunks: %lu", kfsfid, lop.chunks.size());
    if (lop.chunks.size() == 0)
	return 0;

    vector <ChunkLayoutInfo>::reverse_iterator last = lop.chunks.rbegin();
    ssize_t filesize = last->fileOffset;
    ssize_t endsize = 0;
    int rstatus = 0;
    RespondingServer responder(this, *last, &endsize, &rstatus);
    vector <ServerLocation>::iterator s =
	    find_if(last->chunkServers.begin(), last->chunkServers.end(),
			    responder);
    if (s != last->chunkServers.end()) {
	if (rstatus < 0) {
	    COSMIX_LOG_DEBUG("RespondingServer status %d", rstatus);
	    return 0;
	}
	filesize += endsize;
    }

    COSMIX_LOG_DEBUG("Size of kfsfid = %ld, size = %ld",
	             kfsfid, filesize);

    return filesize;
}

// A simple functor to match chunkserver by hostname
class ChunkserverMatcher {
    string myHostname;
public:
    ChunkserverMatcher(const string &l) :
        myHostname(l) { }
    bool operator()(const ServerLocation &loc) const {
        return loc.hostname == myHostname;
    }
};

int
KfsClient::OpenChunk(int fd)
{
    if (!IsCurrChunkAttrKnown(fd)) {
	// Nothing known about this chunk
	return -EINVAL;
    }

    ChunkAttr *chunk = GetCurrChunk(fd);
    if (chunk->chunkId == (kfsChunkId_t) -1) {
	chunk->chunkSize = 0;
	// don't send bogus chunk id's
	return -EINVAL;
    }

    TcpSocket **sock = &mFileTable[fd]->currPos.chunkServerSock;
    // try the local server first
    vector <ServerLocation>::iterator s =
        find_if(chunk->chunkServerLoc.begin(), chunk->chunkServerLoc.end(), 
                ChunkserverMatcher(mHostname));
    if (s != chunk->chunkServerLoc.end()) {
        *sock = GetChunkServerSocket(*s);
        if (*sock != NULL) {
            COSMIX_LOG_DEBUG("Picking local server: %s", s->ToString().c_str());
            return SizeChunk(fd);
        }
    }

    // else pick one at random
    vector<ServerLocation> loc = chunk->chunkServerLoc;
    vector<ServerLocation>::size_type i = rand() % loc.size();

    *sock = GetChunkServerSocket(loc[i]);

    COSMIX_LOG_DEBUG("Randomly chose: %s", loc[i].ToString().c_str());

    i = 0;
    while (*sock == NULL && i != loc.size()) {
   	*sock = GetChunkServerSocket(loc[i]);
    	++i;
    }

    return (*sock == NULL) ? -EHOSTUNREACH : SizeChunk(fd);
}

int
KfsClient::SizeChunk(int fd)
{
    ChunkAttr *chunk = GetCurrChunk(fd);

    SizeOp op(nextSeq(), chunk->chunkId, chunk->chunkVersion);
    (void)DoOpCommon(&op, mFileTable[fd]->currPos.chunkServerSock);
    chunk->chunkSize = op.size;

    COSMIX_LOG_DEBUG("Chunk: %ld, size = %zd",
	             chunk->chunkId, chunk->chunkSize);

    return op.status;
}

///
/// Wrapper for retrying ops with the metaserver.
///
int
KfsClient::DoMetaOpWithRetry(KfsOp *op)
{
    int res;

    if (!mMetaServerSock.IsGood())
	ConnectToMetaServer();

    for (int attempt = 0; attempt < NUM_RETRIES_PER_OP; attempt++) {
	res = DoOpCommon(op, &mMetaServerSock);
	if (op->status != -EHOSTUNREACH && op->status != -ETIMEDOUT)
	    break;
	Sleep(RETRY_DELAY_SECS);
	ConnectToMetaServer();
	// re-issue the op with a new sequence #
	op->seq = nextSeq();
    }
    return res;
}

static bool
null_fte(const FileTableEntry *ft)
{
    return (ft == NULL);
}

//
// Rank entries by access time, but putting all directories before files
//
static bool
fte_compare(const FileTableEntry *first, const FileTableEntry *second)
{
	bool dir1 = first->fattr.isDirectory;
	bool dir2 = second->fattr.isDirectory;

	if (dir1 == dir2)
		return first->lastAccessTime < second->lastAccessTime;
	else
		return dir1;
}

int
KfsClient::FindFreeFileTableEntry()
{
    vector <FileTableEntry *>::iterator b = mFileTable.begin();
    vector <FileTableEntry *>::iterator e = mFileTable.end();
    vector <FileTableEntry *>::iterator i = find_if(b, e, null_fte);
    if (i != e)
	return i - b;		// Use NULL entries first

    int last = mFileTable.size();
    if (last != MAX_FILES) {	// Grow vector up to max. size
	    mFileTable.push_back(NULL);
	    return last;
    }

    // recycle directory entries
    vector <FileTableEntry *>::iterator oldest = min_element(b, e, fte_compare);
    if ((*oldest)->fattr.isDirectory) {
	    ReleaseFileTableEntry(oldest - b);
	    return oldest - b;
    }

    return -EMFILE;		// No luck
}

class FTMatcher {
    kfsFileId_t parentFid;
    string myname;
public:
    FTMatcher(kfsFileId_t f, const char *n): parentFid(f), myname(n) { }
    bool operator () (FileTableEntry *ft) {
	return (ft != NULL &&
	        ft->parentFid == parentFid &&
	        ft->name == myname);
    }
};

int
KfsClient::LookupFileTableEntry(kfsFileId_t parentFid, const char *name)
{
    FTMatcher match(parentFid, name);
    vector <FileTableEntry *>::iterator i;
    i = find_if(mFileTable.begin(), mFileTable.end(), match);
    return (i == mFileTable.end()) ? -1 : i - mFileTable.begin();
}

int
KfsClient::LookupFileTableEntry(const char *pathname)
{
    kfsFileId_t parentFid;
    string name;
    int res = GetPathComponents(pathname, &parentFid, name);
    if (res < 0)
	return res;

    return LookupFileTableEntry(parentFid, name.c_str());
}

int
KfsClient::ClaimFileTableEntry(kfsFileId_t parentFid, const char *name)
{
    int fte = LookupFileTableEntry(parentFid, name);
    if (fte >= 0)
	return fte;

    fte = FindFreeFileTableEntry();
    if (fte >= 0) {
	mFileTable[fte] = new FileTableEntry(parentFid, name);
	mFileTable[fte]->lastAccessTime = time(NULL);
    }
    return fte;
}

void
KfsClient::ReleaseFileTableEntry(int fte)
{
    delete mFileTable[fte];
    mFileTable[fte] = NULL;
}

///
/// Given a parentFid and a file in that directory, return the
/// corresponding entry in the file table.  If such an entry has not
/// been seen before, download the file attributes from the server and
/// save it in the file table.
///
int
KfsClient::Lookup(kfsFileId_t parentFid, const char *name)
{
    int fte = LookupFileTableEntry(parentFid, name);
    if (fte >= 0)
	return fte;

    LookupOp op(nextSeq(), parentFid, name);
    (void) DoOpCommon(&op, &mMetaServerSock);
    if (op.status < 0) {
	return op.status;
    }
    // Everything is good now...
    fte = ClaimFileTableEntry(parentFid, name);
    if (fte < 0) // too many open files
	return -EMFILE;

    FileAttr *fa = FdAttr(fte);
    *fa = op.fattr;

    return fte;
}

///
/// Given a path, break it down into: parentFid and filename.  If the
/// path does not begin with "/", the current working directory is
/// inserted in front of it.
/// @param[in] path	The path string that needs to be extracted
/// @param[out] parentFid  The file-id corresponding to the parent dir
/// @param[out] name    The filename following the final "/".
/// @retval 0 on success; -errno on failure
///
int
KfsClient::GetPathComponents(const char *pathname, kfsFileId_t *parentFid,
	                     string &name)
{
    const char slash = '/';
    string pathstr = build_path(mCwd, pathname);

    string::size_type pathlen = pathstr.size();
    if (pathlen == 0 || pathstr[0] != slash)
	return -EINVAL;

    // find the trailing '/'
    string::size_type rslash = pathstr.rfind('/');
    if (rslash + 1 == pathlen) {
	// path looks like: /.../.../; so get rid of the last '/'
	pathstr.erase(rslash);
	pathlen = pathstr.size();
	rslash = pathstr.rfind('/');
    }

    if (pathlen == 0)
	name = "/";
    else {
	// the component of the name we want is between trailing slash
	// and the end of string
	name.assign(pathstr, rslash + 1, string::npos);
	// get rid of the last component of the path as we have copied
	// it out.
	pathstr.erase(rslash + 1, string::npos);
	pathlen = pathstr.size();
    }
    if (name.size() == 0)
	return -EINVAL;

    *parentFid = KFS::ROOTFID;
    if (pathlen == 0)
	return 0;

    // Verify that the all the components in pathname leading to
    // "name" are directories.
    string::size_type start = 1;
    while (start != string::npos) {
	string::size_type next = pathstr.find(slash, start);
	if (next == string::npos)
	    break;

	if (next == start)
	    return -EINVAL;		// don't allow "//" in path
	string component(pathstr, start, next - start);
	int fte = Lookup(*parentFid, component.c_str());
	if (fte < 0)
	    return fte;
	else if (!FdAttr(fte)->isDirectory)
	    return -ENOTDIR;
	else
	    *parentFid = FdAttr(fte)->fileId;
	start = next + 1; // next points to '/'
    }

    COSMIX_LOG_DEBUG("file-id for dir: %s (file = %s) is %ld",
	             pathstr.c_str(), name.c_str(), *parentFid);
    return 0;
}

TcpSocket *
KfsClient::GetChunkServerSocket(const ServerLocation &loc)
{
    int i;
    TcpSocket *sock;

    for (i = 0; i < MAX_CHUNKSERVERS; ++i) {
	if (mChunkServerSockTable[i].Matches(loc)) {
	    sock = &mChunkServerSockTable[i].chunkServerSock;
	    // if the socket isn't good for I/O, reconnect
	    if (sock->IsGood()) {
		return sock;
	    }
	    mChunkServerSockTable[i].entryInUse = false;
	}
    }

    for (i = 0; i < MAX_CHUNKSERVERS; ++i) {
	if (!mChunkServerSockTable[i].entryInUse) {
	    mChunkServerSockTable[i].Claim(loc);
	    sock = &mChunkServerSockTable[i].chunkServerSock;

	    if (sock->Connect(loc) < 0) {
		COSMIX_LOG_DEBUG("Unable to connect to: %s",
	                         loc.ToString().c_str());
		mChunkServerSockTable[i].Reset();
		return NULL;
	    }
	    return sock;
	}
    }
    assert(!"Too many chunk server connections");
    return NULL;
}

string
KFS::ErrorCodeToStr(int status)
{

    if (status == 0)
	return "";

    char buf[4096];
    char *errptr = NULL;

#if defined (__APPLE__)
    if (strerror_r(-status, buf, sizeof buf) == 0)
	errptr = buf;
    else
	errptr = "<unknown error>";
#else
    if ((errptr = strerror_r(-status, buf, sizeof buf)) == NULL)
	errptr = "<unknown error>";
#endif
    return string(errptr);

}

int
KfsClient::GetLease(kfsChunkId_t chunkId)
{
    int res;

    assert(chunkId >= 0);

    for (int i = 0; i < 3; i++) {		// XXX Evil constant
	LeaseAcquireOp op(nextSeq(), chunkId);
	res = DoOpCommon(&op, &mMetaServerSock);
	if (op.status == 0)
		mLeaseClerk.RegisterLease(op.chunkId, op.leaseId);
	if (op.status != -EBUSY) {
	    res = op.status;
	    break;
	}

	COSMIX_LOG_DEBUG("Server says lease is busy...waiting");
	// Server says the lease is busy...so wait
	Sleep(KFS::LEASE_INTERVAL_SECS);
    }
    return res;
}

void
KfsClient::RenewLease(kfsChunkId_t chunkId)
{
    int64_t leaseId;

    int res = mLeaseClerk.GetLeaseId(chunkId, leaseId);
    if (res < 0)
	return;

    LeaseRenewOp op(nextSeq(), chunkId, leaseId);
    res = DoOpCommon(&op, &mMetaServerSock);
    if (op.status == 0) {
	mLeaseClerk.LeaseRenewed(op.chunkId);
	return;
    }
    if (op.status == -EINVAL) {
	mLeaseClerk.UnRegisterLease(op.chunkId);
    }
}
