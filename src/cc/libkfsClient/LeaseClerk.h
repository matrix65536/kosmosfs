//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsClient/LeaseClerk.h#3 $
//
// Created 2006/10/12
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
// \brief A lease clerk interacts with the metaserver for renewing
// read leases. 
//----------------------------------------------------------------------------

#ifndef LIBKFSCLIENT_LEASECLERK_H
#define LIBKFSCLIENT_LEASECLERK_H

#include "common/kfstypes.h"
#include "libkfsIO/Chunk.h"
#include <tr1/unordered_map>

namespace KFS {

struct LeaseInfo_t {
    int64_t leaseId;
    time_t expires;
    time_t renewTime;
};

// mapping from a chunk id to its lease
typedef std::tr1::unordered_map <kfsChunkId_t, LeaseInfo_t> LeaseMap;
typedef std::tr1::unordered_map <kfsChunkId_t, LeaseInfo_t>::iterator LeaseMapIter;

class LeaseClerk {
public:
    /// Before the lease expires at the server, we submit we a renew
    /// request, so that the lease remains valid.  So, back-off a few
    /// secs before the leases and submit the renew
    static const int LEASE_RENEW_INTERVAL_SECS = KFS::LEASE_INTERVAL_SECS - 10;

    LeaseClerk() { };
    ~LeaseClerk(){ };
    /// Register a lease with the clerk.
    /// @param[in] chunkId The chunk associated with the lease.
    /// @param[in] leaseId  The lease id to be registered with the clerk
    void RegisterLease(kfsChunkId_t chunkId, int64_t leaseId);
    void UnRegisterLease(kfsChunkId_t chunkId);
    
    /// Check if lease is still valid.
    /// @param[in] chunkId  The chunk whose lease we are checking for validity.
    bool IsLeaseValid(kfsChunkId_t chunkId);

    bool ShouldRenewLease(kfsChunkId_t chunkId);

    /// Get the leaseId for a chunk.
    int GetLeaseId(kfsChunkId_t chunkId, int64_t &leaseId);

    void LeaseRenewed(kfsChunkId_t chunkId);

private:
    /// All the leases registered with the clerk
    LeaseMap mLeases;
};

}

#endif // LIBKFSCLIENT_LEASECLERK_H
