//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsClient/LeaseClerk.cc#3 $
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
// \brief Code for dealing with lease renewals.
//
//----------------------------------------------------------------------------

#include "LeaseClerk.h"
#include "common/log.h"

using namespace KFS;

void
LeaseClerk::RegisterLease(kfsChunkId_t chunkId, int64_t leaseId)
{
    time_t now = time(0);
    LeaseInfo_t lease;

    lease.leaseId = leaseId;
    lease.expires = now + KFS::LEASE_INTERVAL_SECS;
    lease.renewTime = now + LEASE_RENEW_INTERVAL_SECS;

    mLeases[chunkId] = lease;
    COSMIX_LOG_DEBUG("Registered lease: chunk=%ld, lease=%ld",
                     chunkId, leaseId);
}

void
LeaseClerk::UnRegisterLease(kfsChunkId_t chunkId)
{
    LeaseMapIter iter;

    iter = mLeases.find(chunkId);
    if (iter != mLeases.end()) {
        mLeases.erase(iter);
    }
    COSMIX_LOG_DEBUG("Lease for chunk = %ld unregistered",
                     chunkId);

}

bool
LeaseClerk::IsLeaseValid(kfsChunkId_t chunkId)
{
    LeaseMapIter iter;

    iter = mLeases.find(chunkId);
    if (iter == mLeases.end())
        return false;

    time_t now = time(NULL);
    LeaseInfo_t lease = iter->second;
    
    // now <= lease.expires ==> lease hasn't expired and is therefore
    // valid.
    return now <= lease.expires;
}


bool
LeaseClerk::ShouldRenewLease(kfsChunkId_t chunkId)
{
    LeaseMapIter iter;

    iter = mLeases.find(chunkId);
    assert(iter != mLeases.end());
    if (iter == mLeases.end()) {
        return true;
    }

    time_t now = time(NULL);
    LeaseInfo_t lease = iter->second;

    // now >= lease.renewTime ==> it is time to renew lease
    return now >= lease.renewTime;
}

int
LeaseClerk::GetLeaseId(kfsChunkId_t chunkId, int64_t &leaseId)
{
    LeaseMapIter iter;

    iter = mLeases.find(chunkId);
    if (iter == mLeases.end()) {
        return -1;
    }
    leaseId = iter->second.leaseId;

    return 0;
}

void
LeaseClerk::LeaseRenewed(kfsChunkId_t chunkId)
{
    LeaseMapIter iter;

    iter = mLeases.find(chunkId);
    if (iter == mLeases.end())
        return;

    time_t now = time(NULL);
    LeaseInfo_t lease = iter->second;

    COSMIX_LOG_DEBUG("Lease for chunk = %ld renewed",
                     chunkId);

    lease.expires = now + KFS::LEASE_INTERVAL_SECS;
    lease.renewTime = now + LEASE_RENEW_INTERVAL_SECS;
    mLeases[chunkId] = lease;
}
