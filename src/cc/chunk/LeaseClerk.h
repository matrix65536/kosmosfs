//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/LeaseClerk.h#3 $
//
// Created 2006/10/09
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
// \brief A lease clerk interacts with the metaserver for renewing
// leases.  There are two assumptions here:
// 1. The lease is for writes and only those need to be renewed
// 2. Prior to renewing a lease, the lease clerk checks with the
// ChunkManager to see if writes are outstanding on the chunk
// associated with the lease; only if writes are pending, is the lease
// renewed.
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_LEASECLERK_H
#define CHUNKSERVER_LEASECLERK_H

#include <tr1/unordered_map>

#include "libkfsIO/Chunk.h"
#include "libkfsIO/Event.h"
#include "common/kfstypes.h"

struct LeaseInfo_t {
    int64_t leaseId;
    time_t expires;
    time_t lastWriteTime;
    EventPtr timer;
};

// mapping from a chunk id to its lease
typedef std::tr1::unordered_map <kfsChunkId_t, LeaseInfo_t> LeaseMap;
typedef std::tr1::unordered_map <kfsChunkId_t, LeaseInfo_t>::iterator LeaseMapIter;

class LeaseClerk : public KfsCallbackObj {
public:
    /// Before the lease expires at the server, we submit we a renew
    /// request, so that the lease remains valid.  So, back-off a few
    /// secs before the leases and submit the renew
    static const int LEASE_RENEW_INTERVAL_SECS = KFS::LEASE_INTERVAL_SECS - 10;
    static const int LEASE_RENEW_INTERVAL_MSECS = LEASE_RENEW_INTERVAL_SECS * 1000;

    LeaseClerk();
    ~LeaseClerk(){ };
    /// Register a lease with the clerk.  The clerk will renew the
    /// lease with the server. 
    /// @param[in] chunkId The chunk associated with the lease.
    /// @param[in] leaseId  The lease id to be registered with the clerk
    void RegisterLease(kfsChunkId_t chunkId, int64_t leaseId);
    void UnRegisterLease(kfsChunkId_t chunkId);

    /// Record the occurence of a write.  This notifies the clerk to
    /// renew the lease prior to the end of the lease period.
    void DoingWrite(kfsChunkId_t chunkId);

    /// Check if lease is still valid.
    /// @param[in] leaseId  The lease id that we are checking for validity.
    bool IsLeaseValid(kfsChunkId_t chunkId);

    /// A handler for handling timeouts related to renewing leases.
    /// When a lease is registered with the clerk, the clerk sets up a
    /// timeout to renew the lease.  When the timeout occurs this
    /// method is called with data being the chunkid for which a lease
    /// renewal may be needed.
    int HandleEvent(int code, void *data);

private:
    /// All the leases registered with the clerk
    LeaseMap mLeases;

    void LeaseRenewed(kfsChunkId_t chunkId);
    void LeaseExpired(kfsChunkId_t chunkId);

    void CleanupExpiredLeases();
};

extern LeaseClerk gLeaseClerk;

#endif // CHUNKSERVER_LEASECLERK_H
