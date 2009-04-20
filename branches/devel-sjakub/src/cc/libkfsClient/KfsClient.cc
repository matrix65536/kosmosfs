//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/04/18
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
// \file KfsClient.cc
// \brief Kfs Client-library code.
//
//----------------------------------------------------------------------------

#include "KfsClient.h"
#include "KfsClientInt.h"

#include "common/config.h"
#include "common/properties.h"
#include "common/log.h"
#include "meta/kfstypes.h"
#include "libkfsIO/Checksum.h"
#include "Utils.h"

extern "C" {
#include <signal.h>
}

#include <cstdlib>

#include <cerrno>
#include <iostream>
#include <string>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <boost/scoped_array.hpp>

using std::string;
using std::ostringstream;
using std::istringstream;
using std::min;
using std::max;
using std::map;
using std::vector;
using std::sort;
using std::transform;
using std::random_shuffle;

using std::cout;
using std::endl;

using namespace KFS;

namespace {
    Properties & theProps()
    {
        static Properties p;
        return p;
    }
}   

KfsClientFactory *
KFS::getKfsClientFactory()
{
    return KfsClientFactory::Instance();
}

KfsClientPtr
KfsClientFactory::internalGetClient(const char *propFile)
{
    bool verbose = false;
#ifdef DEBUG
    verbose = true;
#endif
    if (theProps().loadProperties(propFile, '=', verbose) != 0) {
        KfsClientPtr clnt;
	return clnt;
    }

    return internalGetClient(theProps().getValue("metaServer.name", ""),
                     theProps().getValue("metaServer.port", -1));

}

KfsClientPtr
KfsClientFactory::internalGetClient(const std::string metaServerHost, int metaServerPort)
{
    vector<KfsClientPtr>::iterator iter;
    ServerLocation loc(metaServerHost, metaServerPort);

    // Check if off_t has expected size. Otherwise print an error and exit the whole program!
    if (sizeof(off_t) != 8)
    {
	KFS_LOG_VA_FATAL("Error! 'off_t' type needs to be 8 bytes long (instead of %zu). "
	    "You need to recompile KFS with: -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE "
	    "-D_LARGEFILE64_SOURCE -D_LARGE_FILES", sizeof(off_t));
	exit(-1);
    }

    iter = find_if(mClients.begin(), mClients.end(), MatchingServer(loc));
    if (iter != mClients.end())
        return *iter;

    KfsClientPtr clnt;

    clnt.reset(new KfsClient());

    clnt->Init(metaServerHost, metaServerPort);
    if (clnt->IsInitialized())
        mClients.push_back(clnt);
    else
        clnt.reset();

    return clnt;
}

void 
KfsClientFactory::checkClientOffSize(size_t size)
{
    // This should be called from code in .h file inherited by the client,
    // so we could test if program using KFS has correct size of off_t.
    // Check if off_t has expected size. Otherwise print an error and exit the whole program!
    if (size != 8)
    {
	KFS_LOG_VA_FATAL("Error! 'off_t' type needs to be 8 bytes long (instead of %zu). "
	    "You need to recompile your program with: -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE "
	    "-D_LARGEFILE64_SOURCE -D_LARGE_FILES", size);
	exit(-1);
    }
}

KfsClient::KfsClient()
{
    mImpl = new KfsClientImpl();
}

KfsClient::~KfsClient()
{
    delete mImpl;
}

void
KfsClient::SetLogLevel(string logLevel)
{
    if (logLevel == "DEBUG")
        MsgLogger::SetLevel(log4cpp::Priority::DEBUG);
    else if (logLevel == "INFO")
        MsgLogger::SetLevel(log4cpp::Priority::INFO);
}

int 
KfsClient::Init(const std::string metaServerHost, int metaServerPort)
{
    return mImpl->Init(metaServerHost, metaServerPort);
}

bool 
KfsClient::IsInitialized()
{
    return mImpl->IsInitialized();
}

int
KfsClient::Cd(const char *pathname)
{
    return mImpl->Cd(pathname);
}

string
KfsClient::GetCwd()
{
    return mImpl->GetCwd();
}

int
KfsClient::Mkdirs(const char *pathname)
{
    return mImpl->Mkdirs(pathname);
}

int 
KfsClient::Mkdir(const char *pathname)
{
    return mImpl->Mkdir(pathname);
}

int 
KfsClient::Rmdir(const char *pathname)
{
    return mImpl->Rmdir(pathname);
}

int 
KfsClient::Rmdirs(const char *pathname)
{
    return mImpl->Rmdirs(pathname);
}

int 
KfsClient::Readdir(const char *pathname, std::vector<std::string> &result)
{
    return mImpl->Readdir(pathname, result);
}

int 
KfsClient::ReaddirPlus(const char *pathname, std::vector<KfsFileAttr> &result)
{
    return mImpl->ReaddirPlus(pathname, result);
}

int 
KfsClient::GetDirSummary(const char *pathname, uint64_t &numFiles, uint64_t &numBytes)
{
    return mImpl->GetDirSummary(pathname, numFiles, numBytes);
}

int 
KfsClient::Stat(const char *pathname, struct stat &result, bool computeFilesize)
{
    return mImpl->Stat(pathname, result, computeFilesize);
}

bool 
KfsClient::Exists(const char *pathname)
{
    return mImpl->Exists(pathname);
}

bool 
KfsClient::IsFile(const char *pathname)
{
    return mImpl->IsFile(pathname);
}

bool 
KfsClient::IsDirectory(const char *pathname)
{
    return mImpl->IsDirectory(pathname);
}

int
KfsClient::EnumerateBlocks(const char *pathname)
{
    return mImpl->EnumerateBlocks(pathname);
}

bool
KfsClient::VerifyDataChecksums(const char *pathname, const vector<uint32_t> &checksums)
{
    return mImpl->VerifyDataChecksums(pathname, checksums);
}

bool
KfsClient::VerifyDataChecksums(int fd, off_t offset, const char *buf, off_t numBytes)
{
    return mImpl->VerifyDataChecksums(fd, offset, buf, numBytes);
}

int 
KfsClient::Create(const char *pathname, int numReplicas, bool exclusive)
{
    return mImpl->Create(pathname, numReplicas, exclusive);
}

int 
KfsClient::Remove(const char *pathname)
{
    return mImpl->Remove(pathname);
}

int 
KfsClient::Rename(const char *oldpath, const char *newpath, bool overwrite)
{
    return mImpl->Rename(oldpath, newpath, overwrite);
}

int 
KfsClient::Open(const char *pathname, int openFlags, int numReplicas)
{
    return mImpl->Open(pathname, openFlags, numReplicas);
}

int 
KfsClient::Fileno(const char *pathname)
{
    return mImpl->Fileno(pathname);
}

int 
KfsClient::Close(int fd)
{
    return mImpl->Close(fd);
}

ssize_t 
KfsClient::Read(int fd, char *buf, size_t numBytes)
{
    return mImpl->Read(fd, buf, numBytes);
}

ssize_t 
KfsClient::Write(int fd, const char *buf, size_t numBytes)
{
    return mImpl->Write(fd, buf, numBytes);
}

int 
KfsClient::Sync(int fd, bool flushOnlyIfHasFullChecksumBlock)
{
    return mImpl->Sync(fd, flushOnlyIfHasFullChecksumBlock);
}

off_t 
KfsClient::Seek(int fd, off_t offset, int whence)
{
    return mImpl->Seek(fd, offset, whence);
}

off_t 
KfsClient::Seek(int fd, off_t offset)
{
    return mImpl->Seek(fd, offset, SEEK_SET);
}

off_t 
KfsClient::Tell(int fd)
{
    return mImpl->Tell(fd);
}

int 
KfsClient::Truncate(int fd, off_t offset)
{
    return mImpl->Truncate(fd, offset);
}

int 
KfsClient::GetDataLocation(const char *pathname, off_t start, off_t len,
                           std::vector< std::vector <std::string> > &locations)
{
    return mImpl->GetDataLocation(pathname, start, len, locations);
}

int 
KfsClient::GetDataLocation(int fd, off_t start, off_t len,
                           std::vector< std::vector <std::string> > &locations)
{
    return mImpl->GetDataLocation(fd, start, len, locations);
}

int16_t 
KfsClient::GetReplicationFactor(const char *pathname)
{
    return mImpl->GetReplicationFactor(pathname);
}

int16_t 
KfsClient::SetReplicationFactor(const char *pathname, int16_t numReplicas)
{
    return mImpl->SetReplicationFactor(pathname, numReplicas);
}

ServerLocation
KfsClient::GetMetaserverLocation() const
{
    return mImpl->GetMetaserverLocation();
}

size_t
KfsClient::SetDefaultIoBufferSize(size_t size)
{
    return mImpl->SetDefaultIoBufferSize(size);
}

size_t
KfsClient::GetDefaultIoBufferSize() const
{
    return mImpl->GetDefaultIoBufferSize();
}

size_t
KfsClient::SetIoBufferSize(int fd, size_t size)
{
    return mImpl->SetIoBufferSize(fd, size);
}

size_t
KfsClient::GetIoBufferSize(int fd) const
{
    return mImpl->GetIoBufferSize(fd);
}

size_t
KfsClient::SetDefaultReadAheadSize(size_t size)
{
    return mImpl->SetDefaultReadAheadSize(size);
}

size_t
KfsClient::GetDefaultReadAheadSize() const
{
    return mImpl->GetDefaultReadAheadSize();
}

size_t
KfsClient::SetReadAheadSize(int fd, size_t size)
{
    return mImpl->SetReadAheadSize(fd, size);
}

size_t
KfsClient::GetReadAheadSize(int fd) const
{
    return mImpl->GetReadAheadSize(fd);
}
