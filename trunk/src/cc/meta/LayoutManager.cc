//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/06/06
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
// \file LayoutManager.cc
// \brief Handlers for chunk layout.
//
//----------------------------------------------------------------------------

#include <algorithm>
#include <functional>

#include "LayoutManager.h"
#include "kfstree.h"
#include "libkfsIO/Globals.h"

using std::for_each;
using std::find;
using std::ptr_fun;
using std::sort;
using std::remove_if;
using std::set;
using std::vector;
using std::map;
using std::min;

using namespace KFS;
using namespace KFS::libkfsio;

LayoutManager KFS::gLayoutManager;
const int MAX_CONCURRENT_REPLICATIONS = 10;

///
/// For disk space utilization balancing, we say that a server
/// is "under utilized" if is below 20% full; we say that a server
/// is "over utilized" if it is above 80% full.  For rebalancing, we
/// move data from servers that are over-utilized to servers that are
/// under-utilized.  These #'s are intentionally set conservatively; we
/// don't want the system to constantly move stuff between nodes when
/// there isn't much to be gained by it.
///
const float MIN_SERVER_SPACE_UTIL_THRESHOLD = 0.2;
const float MAX_SERVER_SPACE_UTIL_THRESHOLD = 0.8;


/// Helper functor that can be used to find a chunkid from a vector
/// of meta chunk info's.

class ChunkIdMatcher {
	chunkId_t myid;
public:
	ChunkIdMatcher(chunkId_t c) : myid(c) { }
	bool operator() (MetaChunkInfo *c) {
		return c->chunkId == myid;
	}
};

LayoutManager::LayoutManager() :
	mLeaseId(1), mNumOngoingReplications(0),
	mRecoveryStartTime(0)
{
	mOngoingReplicationStats = new Counter("Num Ongoing Replications");
	mTotalReplicationStats = new Counter("Total Num Replications");
	mFailedReplicationStats = new Counter("Num Failed Replications");
	globals().counterManager.AddCounter(mOngoingReplicationStats);
	globals().counterManager.AddCounter(mTotalReplicationStats);
	globals().counterManager.AddCounter(mFailedReplicationStats);
}


/// Add the newly joined server to the list of servers we have.  Also,
/// update our state to include the chunks hosted on this server.
void
LayoutManager::AddNewServer(MetaHello *r)
{
        ChunkServerPtr s;
        vector <chunkId_t> staleChunkIds;
        vector <ChunkInfo>::size_type i;
	vector <ChunkServer *>::size_type j;
	uint64_t allocSpace = r->chunks.size() * CHUNKSIZE;

	if (r->server->IsDown())
		return;

        s = r->server;
        s->SetServerLocation(r->location);
        s->SetSpace(r->totalSpace, r->usedSpace, allocSpace);

        // If a previously dead server reconnects, reuse the server's
        // position in the list of chunk servers.  This is because in
        // the chunk->server mapping table, we use the chunkserver's
        // position in the list of connected servers to find it.
        //
        for (j = 0; j < mChunkServers.size(); ++j) {
		if (mChunkServers[j]->MatchingServer(r->location)) {
			KFS_LOG_DEBUG("Duplicate server: %s, %d",
					 r->location.hostname.c_str(), r->location.port);
			return;
		}
        }

	mChunkServers.push_back(s);

	for (i = 0; i < r->chunks.size(); ++i) {
		vector<MetaChunkInfo *> v;
		vector<MetaChunkInfo *>::iterator chunk;
		int res = -1;

		metatree.getalloc(r->chunks[i].fileId, v);

		chunk = find_if(v.begin(), v.end(), ChunkIdMatcher(r->chunks[i].chunkId));
		if (chunk != v.end()) {
			MetaChunkInfo *mci = *chunk;
			if (mci->chunkVersion <= r->chunks[i].chunkVersion) {
				// This chunk is non-stale.  Verify that there are
				// sufficient copies; if there are too many, nuke some.
				ChangeChunkReplication(r->chunks[i].chunkId);

				res = UpdateChunkToServerMapping(r->chunks[i].chunkId, 
								s.get());
				assert(res >= 0);

				if (mci->chunkVersion < r->chunks[i].chunkVersion) {
					// version #'s differ.  have the chunkserver reset
					// to what the metaserver has.
					// XXX: This is all due to the issue with not logging
					// the version # that the metaserver is issuing.  What is going
					// on here is that, 
					//  -- client made a request
					//  -- metaserver bumped the version; notified the chunkservers
					//  -- the chunkservers write out the version bump on disk
					//  -- the metaserver gets ack; writes out the version bump on disk
					//  -- and then notifies the client
					// Now, if the metaserver crashes before it writes out the
					// version bump, it is possible that some chunkservers did the
					// bump, but not the metaserver.  So, fix up.  To avoid other whacky
					// scenarios, we increment the chunk version # by the incarnation stuff
					// to avoid reissuing the same version # multiple times.
					s->NotifyChunkVersChange(r->chunks[i].fileId,
							r->chunks[i].chunkId,
							mci->chunkVersion);

				}
			}
			else {
                        	KFS_LOG_DEBUG("Old version for chunk id = %lld => stale",
                                         r->chunks[i].chunkId);
			}
		}

                if (res < 0) {
                        /// stale chunk
                        KFS_LOG_DEBUG("Non-existent chunk id = %lld => stale",
                                         r->chunks[i].chunkId);
                        staleChunkIds.push_back(r->chunks[i].chunkId);
                }
	}

        if (staleChunkIds.size() > 0) {
                s->NotifyStaleChunks(staleChunkIds);
        }
}

class MapPurger {
	CSMap &cmap;
	CRCandidateSet &crset;
	const ChunkServer *target;
public:
	MapPurger(CSMap &m, CRCandidateSet &c, const ChunkServer *t):
		cmap(m), crset(c), target(t) { }
	void operator () (const map<chunkId_t, ChunkPlacementInfo >::value_type p) {
		ChunkPlacementInfo c = p.second;

		c.chunkServers.erase(remove_if(c.chunkServers.begin(), c.chunkServers.end(), 
					ChunkServerMatcher(target)), 
					c.chunkServers.end());
		cmap[p.first] = c;
		// we need to check the replication level of this chunk
		crset.insert(p.first);
	}
};

void
LayoutManager::ServerDown(ChunkServer *server)
{
        vector <ChunkServerPtr>::iterator i =
		find_if(mChunkServers.begin(), mChunkServers.end(), 
			ChunkServerMatcher(server));

	if (i == mChunkServers.end())
		return;
	
	/// Fail all the ops that were sent/waiting for response from
	/// this server.
	server->FailPendingOps();

	mChunkServers.erase(i);
	MapPurger purge(mChunkToServerMap, mChunkReplicationCandidates, server);
	for_each(mChunkToServerMap.begin(), mChunkToServerMap.end(), purge);
}

struct ServerSpace {
	uint32_t serverIdx;
	uint64_t availSpace;
	uint64_t usedSpace;

	// sort in decreasing order: Prefer the server with more free
	// space, or in the case of a tie, the one with less used space.

	bool operator < (const ServerSpace &other) const {
		if (availSpace != other.availSpace)
			return availSpace > other.availSpace;
		else
			return usedSpace < other.usedSpace;
	}
};

struct ServerSpaceUtil {
	uint32_t serverIdx;
	float utilization;

	// sort in increasing order of space utilization
	bool operator < (const ServerSpaceUtil &other) const {
		return utilization < other.utilization;
	}
};

void
LayoutManager::FindCandidateServers(vector<ChunkServerPtr> &result,
				const vector<ChunkServerPtr> &excludes)
{
	if (mChunkServers.size() < 1)
		return;

	FindCandidateServers(result, mChunkServers, excludes);
}

void
LayoutManager::FindCandidateServers(vector<ChunkServerPtr> &result,
				const vector<ChunkServerPtr> &sources,
				const vector<ChunkServerPtr> &excludes)
{
	if (sources.size() < 1)
		return;

	vector<ServerSpace> ss;
	ChunkServerPtr c;
	vector<ChunkServerPtr>::size_type i, j;
	vector<ChunkServerPtr>::const_iterator iter;

	ss.resize(sources.size());

	for (i = 0, j = 0; i < sources.size(); i++) {
		c = sources[i];
		if (c->GetAvailSpace() < CHUNKSIZE) {
			continue;
		}
		iter = find(excludes.begin(), excludes.end(), c);
		if (iter != excludes.end()) {
			continue;
		}
		ss[j].serverIdx = i;
		ss[j].availSpace = c->GetAvailSpace();
		ss[j].usedSpace = c->GetUsedSpace();
		j++;
	}
	ss.resize(j);

	sort(ss.begin(), ss.end());

	result.reserve(ss.size());
	for (i = 0; i < ss.size(); ++i) {
		result.push_back(sources[ss[i].serverIdx]);
	}
}

void
LayoutManager::SortServersByUtilization(vector<ChunkServerPtr> &servers)
{
	vector<ServerSpaceUtil> ss;
	vector<ChunkServerPtr> temp;

	ss.resize(servers.size());
	temp.resize(servers.size());

	for (vector<ChunkServerPtr>::size_type i = 0; i < servers.size(); i++) {
		ss[i].serverIdx = i;
		ss[i].utilization = servers[i]->GetSpaceUtilization();
		temp[i] = servers[i];
	}

	sort(ss.begin(), ss.end());
	for (vector<ChunkServerPtr>::size_type i = 0; i < servers.size(); i++) {
		servers[i] = temp[ss[i].serverIdx];
	}
}


/// 
/// The algorithm for picking a set of servers to hold a chunk is: (1) pick
/// the server with the most amount of free space, and (2) to break
/// ties, pick the one with the least amount of used space.  This
/// policy has the effect of doing round-robin allocations.  The
/// allocated space is something that we track.  Note: We rely on the
/// chunk servers to tell us how much space is used up on the server.
/// Since servers can respond at different rates, doing allocations
/// based on allocated space ensures equitable distribution;
/// otherwise, if we were to do allocations based on the amount of
/// used space, then a slow responding server will get pummelled with
/// lots of chunks (i.e., used space will be updated on the meta
/// server at a slow rate, causing the meta server to think that the
/// chunk server has lot of space available).
///
int
LayoutManager::AllocateChunk(MetaAllocate *r)
{
	vector<ChunkServerPtr> candidates, dummy;
	vector<ChunkServerPtr>::size_type i;

	FindCandidateServers(candidates, dummy);

	if (candidates.size() == 0) {
		return -ENOSPC;
	}

	r->servers.reserve(r->numReplicas);

	for (i = 0; r->servers.size() < (uint32_t) r->numReplicas && 
			i < mChunkServers.size(); i++) {
		r->servers.push_back(candidates[i]);
	}
        
	LeaseInfo l(WRITE_LEASE, mLeaseId, r->servers[0]);
	mLeaseId++;

	r->master = r->servers[0];
	r->servers[0]->AllocateChunk(r, l.leaseId);

	for (i = 1; i < r->servers.size(); i++) {        
		r->servers[i]->AllocateChunk(r, -1);
	}

	ChunkPlacementInfo v;

	v.fid = r->fid;
	v.chunkServers = r->servers;
	v.chunkLeases.push_back(l);

	mChunkToServerMap[r->chunkId] = v;

	return 0;
}

int
LayoutManager::GetChunkWriteLease(MetaAllocate *r, bool &isNewLease)
{
        CSMapIter iter;
	ChunkPlacementInfo v;
	vector<ChunkServerPtr>::size_type i;
	vector<LeaseInfo>::iterator l;

	// XXX: This is a little too conservative.  We should
	// check if any server has told us about a lease for this
	// file; if no one we know about has a lease, then deny
	// issuing the lease during recovery---because there could
	// be some server who has a lease and hasn't told us yet.
	if (InRecovery()) {
		KFS_LOG_DEBUG("GetChunkWriteLease: InRecovery() => EBUSY");
		return -EBUSY;
	}

	// if no allocation has been done, can't grab any lease
        iter = mChunkToServerMap.find(r->chunkId);
        if (iter == mChunkToServerMap.end())
                return -EINVAL;

	v = iter->second;
	if (v.chunkServers.size() == 0)
		// all the associated servers are dead...so, fail
		// the allocation request.
		return -KFS::EDATAUNAVAIL;

	l = find_if(v.chunkLeases.begin(), v.chunkLeases.end(),
			ptr_fun(LeaseInfo::IsValidWriteLease));
	if (l != v.chunkLeases.end()) {
		LeaseInfo lease = *l;
#ifdef DEBUG
		time_t now = time(0);
		assert(now <= lease.expires);
		KFS_LOG_DEBUG("write lease exists...no version bump");
#endif
		// valid write lease; so, tell the client where to go
		isNewLease = false;
		r->servers = v.chunkServers;
		r->master = lease.chunkServer;
		return 0;
	}
	// there is no valid write lease; to issue a new write lease, we
	// need to do a version # bump.  do that only if we haven't yet
	// handed out valid read leases
	l = find_if(v.chunkLeases.begin(), v.chunkLeases.end(),
			ptr_fun(LeaseInfo::IsValidLease));
	if (l != v.chunkLeases.end()) {
		KFS_LOG_DEBUG("GetChunkWriteLease: read lease => EBUSY");
		return -EBUSY;
	}
	// no one has a valid lease
	LeaseCleanup(r->chunkId, v);

	// Need space on the servers..otherwise, fail it
	r->servers = v.chunkServers;
	for (i = 0; i < r->servers.size(); i++) {        
		if (r->servers[i]->GetAvailSpace() < CHUNKSIZE)
			return -ENOSPC;
	}

	isNewLease = true;

	LeaseInfo lease(WRITE_LEASE, mLeaseId, r->servers[0]);
	mLeaseId++;

	v.chunkLeases.push_back(lease);
	mChunkToServerMap[r->chunkId] = v;

	// when issuing a new lease, bump the version # by the increment
	r->chunkVersion += chunkVersionInc;
	r->master = r->servers[0];
	r->master->AllocateChunk(r, lease.leaseId);

	for (i = 1; i < r->servers.size(); i++) {        
		r->servers[i]->AllocateChunk(r, -1);
	}
	return 0;
}

/*
 * \brief Process a reqeuest for a READ lease.
*/
int
LayoutManager::GetChunkReadLease(MetaLeaseAcquire *req)
{
        CSMapIter iter;
	ChunkPlacementInfo v;

	if (InRecovery()) {
		KFS_LOG_DEBUG("GetChunkReadLease: inRecovery() => EBUSY");
		return -EBUSY;
	}

        iter = mChunkToServerMap.find(req->chunkId);
        if (iter == mChunkToServerMap.end())
                return -EINVAL;

	// issue a read lease
	LeaseInfo lease(READ_LEASE, mLeaseId);
	mLeaseId++;

	v = iter->second;
	v.chunkLeases.push_back(lease);
	mChunkToServerMap[req->chunkId] = v;
	req->leaseId = lease.leaseId;

	return 0;
}

class ValidLeaseIssued {
	CSMap &chunkToServerMap;
public:
	ValidLeaseIssued(CSMap &m) : chunkToServerMap(m) { }
	bool operator() (MetaChunkInfo *c) {
		CSMapIter iter;
		ChunkPlacementInfo v;
		vector<LeaseInfo>::iterator l;

		iter = chunkToServerMap.find(c->chunkId);
		if (iter == chunkToServerMap.end())
			return false;
		v = iter->second;
		l = find_if(v.chunkLeases.begin(), v.chunkLeases.end(),
				ptr_fun(LeaseInfo::IsValidLease));
		return (l != v.chunkLeases.end());
	}
};

bool
LayoutManager::IsValidLeaseIssued(const vector <MetaChunkInfo *> &c)
{
	vector <MetaChunkInfo *>::const_iterator i;

	i = find_if(c.begin(), c.end(), ValidLeaseIssued(mChunkToServerMap));
	if (i == c.end())
		return false;
	KFS_LOG_DEBUG("Valid lease issued on chunk: %lld",
			(*i)->chunkId);
	return true;
}

class LeaseIdMatcher {
	int64_t myid;
public:
	LeaseIdMatcher(int64_t id) : myid(id) { }
	bool operator() (const LeaseInfo &l) {
		return l.leaseId == myid;
	}
};

int
LayoutManager::LeaseRenew(MetaLeaseRenew *req)
{
        CSMapIter iter;
	ChunkPlacementInfo v;
	vector<LeaseInfo>::iterator l;

        iter = mChunkToServerMap.find(req->chunkId);
        if (iter == mChunkToServerMap.end()) {
		if (InRecovery()) {
			// Allow lease renewals during recovery
			LeaseInfo lease(req->leaseType, req->leaseId);
			if (req->leaseId > mLeaseId)
				mLeaseId = req->leaseId + 1;
			v.chunkLeases.push_back(lease);
			mChunkToServerMap[req->chunkId] = v;
			return 0;
		}
                return -EINVAL;

	}
	v = iter->second;
	l = find_if(v.chunkLeases.begin(), v.chunkLeases.end(),
			LeaseIdMatcher(req->leaseId));
	if (l == v.chunkLeases.end())
		return -EINVAL;
	time_t now = time(0);
	if (now > l->expires) {
		// can't renew dead leases; get a new one
		v.chunkLeases.erase(l);
		return -ELEASEEXPIRED;
	}
	l->expires = now + LEASE_INTERVAL_SECS;
	mChunkToServerMap[req->chunkId] = v;
	return 0;
}

///
/// Handling a corrupted chunk involves removing the mapping
/// from chunk id->chunkserver that we know has it.
///
void
LayoutManager::ChunkCorrupt(MetaChunkCorrupt *r)
{
        CSMapIter iter;
	ChunkPlacementInfo v;

        iter = mChunkToServerMap.find(r->chunkId);
	if (iter == mChunkToServerMap.end())
		return;

	v = iter->second;
	if(v.fid != r->fid) {
		KFS_LOG_WARN("Server claims invalid chunk: <%lld, %lld> to be corrupt",
				r->fid, r->chunkId);
		return;
	}

	KFS_LOG_INFO("Server claims file/chunk: <%lld, %lld> to be corrupt",
			r->fid, r->chunkId);
	v.chunkServers.erase(remove_if(v.chunkServers.begin(), v.chunkServers.end(), 
			ChunkServerMatcher(r->server.get())), v.chunkServers.end());
	mChunkToServerMap[r->chunkId] = v;
	// check the replication state when the replicaiton checker gets to it
	ChangeChunkReplication(r->chunkId);
}

class ChunkDeletor {
    chunkId_t chunkId;
public:
    ChunkDeletor(chunkId_t c) : chunkId(c) { }
    void operator () (ChunkServerPtr &c) { c->DeleteChunk(chunkId); }
};

///
/// Deleting a chunk involves two things: (1) removing the
/// mapping from chunk id->chunk server that has it; (2) sending
/// an RPC to the associated chunk server to nuke out the chunk.
///
void
LayoutManager::DeleteChunk(chunkId_t chunkId)
{
	vector<ChunkServerPtr> c;

        // if we know anything about this chunk at all, then we
        // process the delete request.
	if (GetChunkToServerMapping(chunkId, c) != 0)
		return;

	// remove the mapping
	mChunkToServerMap.erase(chunkId);

	// submit an RPC request
	for_each(c.begin(), c.end(), ChunkDeletor(chunkId));
}


class Truncator {
    chunkId_t chunkId;
    size_t sz;
public:
    Truncator(chunkId_t c, size_t s) : chunkId(c), sz(s) { }
    void operator () (ChunkServerPtr &c) { c->TruncateChunk(chunkId, sz); }
};

///
/// To truncate a chunk, find the server that holds the chunk and
/// submit an RPC request to it.
///
void
LayoutManager::TruncateChunk(chunkId_t chunkId, size_t sz)
{
	vector<ChunkServerPtr> c;

        // if we know anything about this chunk at all, then we
        // process the truncate request.
	if (GetChunkToServerMapping(chunkId, c) != 0)
		return;

	// submit an RPC request
        Truncator doTruncate(chunkId, sz);
	for_each(c.begin(), c.end(), doTruncate);
}

void
LayoutManager::AddChunkToServerMapping(chunkId_t chunkId, fid_t fid, 
					ChunkServer *c)
{
	ChunkPlacementInfo v;

        if (c == NULL) {
		// Store an empty mapping to signify the presence of this
		// particular chunkId.
		v.fid = fid;
		mChunkToServerMap[chunkId] = v;
		return;
        }

	assert(ValidServer(c));

	KFS_LOG_DEBUG("Laying out chunk=%lld on server %s",
			 chunkId, c->GetServerName());

	if (UpdateChunkToServerMapping(chunkId, c) == 0)
            return;

	v.fid = fid;
        v.chunkServers.push_back(c->shared_from_this());
        mChunkToServerMap[chunkId] = v;
}

void
LayoutManager::RemoveChunkToServerMapping(chunkId_t chunkId)
{
        CSMapIter iter;

        iter = mChunkToServerMap.find(chunkId);
        if (iter == mChunkToServerMap.end())
                return;

        mChunkToServerMap.erase(iter);
}

int
LayoutManager::UpdateChunkToServerMapping(chunkId_t chunkId, ChunkServer *c)
{
        CSMapIter iter;

        // If the chunkid isn't present in the mapping table, it could be a
        // stale chunk
        iter = mChunkToServerMap.find(chunkId);
        if (iter == mChunkToServerMap.end())
                return -1;

	/*
	KFS_LOG_DEBUG("chunk=%lld was laid out on server %s",
			 chunkId, c->GetServerName());
	*/
        iter->second.chunkServers.push_back(c->shared_from_this());

        return 0;
}

int
LayoutManager::GetChunkToServerMapping(chunkId_t chunkId, vector<ChunkServerPtr> &c)
{
        CSMapConstIter iter;

        iter = mChunkToServerMap.find(chunkId);
        if ((iter == mChunkToServerMap.end()) || 
		(iter->second.chunkServers.size() == 0))
                return -1;

        c = iter->second.chunkServers;
        return 0;
}

/// Wrapper class due to silly template/smart-ptr madness
class Dispatcher {
public:
	Dispatcher() { }
	void operator() (ChunkServerPtr &c) { c->Dispatch(); }
};

void
LayoutManager::Dispatch()
{
	for_each(mChunkServers.begin(), mChunkServers.end(), Dispatcher());
}

bool
LayoutManager::ValidServer(ChunkServer *c)
{
	vector <ChunkServerPtr>::const_iterator i;
	
	i = find_if(mChunkServers.begin(), mChunkServers.end(), 
		ChunkServerMatcher(c));
	return (i != mChunkServers.end());
}

class Pinger {
	string &result;
public:
	Pinger(string &r):result(r) { }
	void operator () (ChunkServerPtr &c) { c->Ping(result); }
};

void
LayoutManager::Ping(string &result)
{
	Pinger doPing(result);
	for_each(mChunkServers.begin(), mChunkServers.end(), doPing);
}

/// functor to tell if a lease has expired
class LeaseExpired {
	time_t now;
public:
	LeaseExpired(time_t n): now(n) { }
	bool operator () (const LeaseInfo &l) { return now >= l.expires; }

};

class ChunkWriteDecrementor {
public:
	void operator() (ChunkServerPtr &c) { c->UpdateNumChunkWrites(-1); }
};

/// If the write lease on a chunk is expired, then decrement the # of writes
/// on the servers that are involved in the write.
class DecChunkWriteCount {
	chunkId_t c;
public:
	DecChunkWriteCount(chunkId_t id) : c(id) { }
	void operator() (const LeaseInfo &l) {
		if (l.leaseType != WRITE_LEASE)
			return;
		vector<ChunkServerPtr> servers;
		gLayoutManager.GetChunkToServerMapping(c, servers);
		for_each(servers.begin(), servers.end(), ChunkWriteDecrementor());
	}

};

/// functor to that expires out leases
class LeaseExpirer {
	CSMap &cmap;
	time_t now;
public:
	LeaseExpirer(CSMap &m, time_t n): cmap(m), now(n) { }
	void operator () (const map<chunkId_t, ChunkPlacementInfo >::value_type p)
	{
		ChunkPlacementInfo c = p.second;
		chunkId_t chunkId = p.first;
		vector<LeaseInfo>::iterator i;

		i = remove_if(c.chunkLeases.begin(), c.chunkLeases.end(), 
			LeaseExpired(now));

		for_each(i, c.chunkLeases.end(), DecChunkWriteCount(chunkId));
		// trim the list
		c.chunkLeases.erase(i, c.chunkLeases.end());
		cmap[p.first] = c;
	}
};

void
LayoutManager::LeaseCleanup()
{
	time_t now = time(0);

	for_each(mChunkToServerMap.begin(), mChunkToServerMap.end(),
		LeaseExpirer(mChunkToServerMap, now));

}

// Cleanup the leases for a particular chunk
void
LayoutManager::LeaseCleanup(chunkId_t chunkId, ChunkPlacementInfo &v)
{
	for_each(v.chunkLeases.begin(), v.chunkLeases.end(), 
		DecChunkWriteCount(chunkId));
	v.chunkLeases.clear();
}

void
LayoutManager::ReplicateChunk(chunkId_t chunkId, const ChunkPlacementInfo &clli,
				int extraReplicas)
{
	// find a place
	vector<ChunkServerPtr> candidates;
	ChunkServerPtr c;

	FindCandidateServers(candidates, clli.chunkServers);

	if (candidates.size() == 0)
		return;

	vector<MetaChunkInfo *> v;
	vector<MetaChunkInfo *>::iterator chunk;
	fid_t fid = clli.fid;

	metatree.getalloc(fid, v);
	chunk = find_if(v.begin(), v.end(), ChunkIdMatcher(chunkId));
	if (chunk == v.end()) {
		// Need to nuke the copy
		panic("missing chunk", true);
	}

	mTotalReplicationStats->Update(1);
	mOngoingReplicationStats->Update(1);

	MetaChunkInfo *mci = *chunk;
	uint32_t numCopies = min((size_t) extraReplicas, candidates.size());

	for (uint32_t i = 0; i < numCopies; i++) {
		c = candidates[i];
#ifdef DEBUG
		// verify that we got good candidates
		vector<ChunkServerPtr>::const_iterator iter;
		iter = find(clli.chunkServers.begin(), clli.chunkServers.end(), c);
		if (iter != clli.chunkServers.end()) {
			assert(!"Not possible...");
		}
#endif
		c->ReplicateChunk(fid, chunkId, mci->chunkVersion,
				clli.chunkServers[0]->GetServerLocation());
	}
}


bool
LayoutManager::CanReplicateChunkNow(chunkId_t chunkId, 
				ChunkPlacementInfo &c,
				int &extraReplicas)
{
	vector<LeaseInfo>::iterator l;

	// Don't replicate chunks for which a write lease
	// has been issued.
	l = find_if(c.chunkLeases.begin(), c.chunkLeases.end(),
		ptr_fun(LeaseInfo::IsValidWriteLease));

	if (l != c.chunkLeases.end())
		return false;
	
	extraReplicas = 0;
	// Can't re-replicate a chunk if we don't have a copy! so,
	// take out this chunk from the candidate set.
	if (c.chunkServers.size() == 0)
		return true;

	MetaFattr *fa = metatree.getFattr(c.fid);
	if (fa == NULL)
		// No file attr.  So, take out this chunk
		// from the candidate set.
		return true;

	// check if the chunk still exists
	vector<MetaChunkInfo *> v;
	vector<MetaChunkInfo *>::iterator chunk;

	metatree.getalloc(c.fid, v);
	chunk = find_if(v.begin(), v.end(), ChunkIdMatcher(chunkId));
	if (chunk == v.end()) {
		// This chunk doesn't exist in this file anymore.  
		// So, take out this chunk from the candidate set.
		return true;
	}

	// May need to re-replicate this chunk: 
	//    - extraReplicas > 0 means make extra copiles; 
	//    - extraReplicas == 0, take out this chunkid from the candidate set
	//    - extraReplicas < 0, means we got too many copies; delete some
	extraReplicas = fa->numReplicas - c.chunkServers.size();
	return true;
}

void
LayoutManager::ChunkReplicationChecker()
{
	if ((mNumOngoingReplications > MAX_CONCURRENT_REPLICATIONS) ||
		(InRecovery())) {
		return;
	}

	// There is a set of chunks that are affected: their server went down
	// or there is a change in their degree of replication.  in either
	// case, walk this set of chunkid's and work on their replication amount.
	
	CSMapIter iter;
	chunkId_t chunkId;
	CRCandidateSet delset;
	int extraReplicas;

	for (CRCandidateSetIter citer = mChunkReplicationCandidates.begin(); 
		mNumOngoingReplications <= MAX_CONCURRENT_REPLICATIONS &&
		citer != mChunkReplicationCandidates.end(); ++citer) {
		chunkId = *citer;

        	iter = mChunkToServerMap.find(chunkId);
        	if (iter == mChunkToServerMap.end()) {
			delset.insert(chunkId);
			continue;
		}
		if (iter->second.ongoingReplications > 0)
			continue;

		KFS_LOG_DEBUG("Checking replication level for chunk: %lld", chunkId);

		if (!CanReplicateChunkNow(iter->first, iter->second, extraReplicas))
			continue;

		if (extraReplicas > 0) {
			ReplicateChunk(iter->first, iter->second, extraReplicas);
			iter->second.ongoingReplications += extraReplicas;
			mNumOngoingReplications++;
		} else if (extraReplicas == 0) {
			delset.insert(chunkId);
		} else {
			DeleteAddlChunkReplicas(iter->first, iter->second, -extraReplicas);
			delset.insert(chunkId);
		}
	}
	
	if (delset.size() > 0) {
		for (CRCandidateSetIter citer = delset.begin(); 
			citer != delset.end(); ++citer) {
			mChunkReplicationCandidates.erase(*citer);
		}
	}

	// If some servers are more loaded (space-wise) than others, move stuff around
	RebalanceServers();
}

void
LayoutManager::ChunkReplicationDone(const MetaChunkReplicate *req)
{
	CSMapIter iter;

	mOngoingReplicationStats->Update(-1);

	// Book-keeping....
	iter = mChunkToServerMap.find(req->chunkId);
	if (iter != mChunkToServerMap.end()) {
		iter->second.ongoingReplications--;
		if (iter->second.ongoingReplications < 0)
			// sanity...
			iter->second.ongoingReplications = 0;
	}

	// if all the replications for this chunk are done,
	// then update the global counter.
	if (iter->second.ongoingReplications == 0)
		mNumOngoingReplications--;

	if (mNumOngoingReplications < 0)
		mNumOngoingReplications = 0;

	req->server->ReplicateChunkDone();

	if (req->status != 0) {
		// Replication failed...we will try again later
		mFailedReplicationStats->Update(1);
		return;
	}

	// replication succeeded: book-keeping
	// validate that the server got the latest copy of the chunk
	vector<MetaChunkInfo *> v;
	vector<MetaChunkInfo *>::iterator chunk;

	metatree.getalloc(req->fid, v);
	chunk = find_if(v.begin(), v.end(), ChunkIdMatcher(req->chunkId));
	if (chunk == v.end()) {
		// Chunk disappeared -> stale; this chunk will get nuked
		KFS_LOG_DEBUG("Re-replicate: chunk (%lld) disappeared => so, stale",
				req->chunkId);
		mFailedReplicationStats->Update(1);
		req->server->NotifyStaleChunk(req->chunkId);
		return;
	}
	MetaChunkInfo *mci = *chunk;
	if (mci->chunkVersion != req->chunkVersion) {
		// Version that we replicated has changed...so, stale
		KFS_LOG_DEBUG("Re-replicate: chunk (%lld) version changed (was=%lld, now=%lld) => so, stale",
				req->chunkId, req->chunkVersion, mci->chunkVersion);
		mFailedReplicationStats->Update(1);
		req->server->NotifyStaleChunk(req->chunkId);
		return;
	}

	// Yaeee...all good...
	UpdateChunkToServerMapping(req->chunkId, req->server.get());

}

//
// To delete additional copies of a chunk, find the servers that have the least
// amount of space and delete the chunk from there.
//
void
LayoutManager::DeleteAddlChunkReplicas(chunkId_t chunkId, ChunkPlacementInfo &clli,
				uint32_t extraReplicas)
{
	vector<ChunkServerPtr> servers = clli.chunkServers;
	uint32_t numReplicas = servers.size() - extraReplicas;

	// We get servers sorted by increasing amount of space utilization; so the candidates
	// we want to delete are at the end
	SortServersByUtilization(servers);

	clli.chunkServers = servers;
	clli.chunkServers.resize(numReplicas);
	mChunkToServerMap[chunkId] = clli;

	KFS_LOG_INFO("Deleting extra replicas (%d) of chunk: %lld", extraReplicas, chunkId);

	// The first N are what we want to keep; the rest should go.
	for_each(servers.begin() + numReplicas, servers.end(), ChunkDeletor(chunkId));
}

void
LayoutManager::ChangeChunkReplication(chunkId_t chunkId)
{
	mChunkReplicationCandidates.insert(chunkId);
}

//
// Check if the server is part of the set of the servers hosting the chunk
//
bool
LayoutManager::IsChunkHostedOnServer(const vector<ChunkServerPtr> &hosters,
					const ChunkServerPtr &server)
{
	vector<ChunkServerPtr>::const_iterator iter;
	iter = find(hosters.begin(), hosters.end(), server);
	return iter != hosters.end();
}

//
// Periodically, if we find that some chunkservers have LOT (> 80% free) of space
// and if others are loaded (i.e., < 30% free space), move chunks around.  This
// helps with keeping better disk space utilization (and maybe load).
//
void
LayoutManager::RebalanceServers()
{
	if ((mNumOngoingReplications > MAX_CONCURRENT_REPLICATIONS) ||
		(InRecovery())) {
		return;
	}

	vector<ChunkServerPtr> servers = mChunkServers;
	vector<ChunkServerPtr> loadedServers, nonloadedServers;
	int extraReplicas;

	// We get servers sorted by increasing amount of space utilization
	SortServersByUtilization(servers);

	// If things are somewhat utilized, don't mess...
	if ((servers.back()->GetSpaceUtilization() < MAX_SERVER_SPACE_UTIL_THRESHOLD) ||
		(servers.front()->GetSpaceUtilization() > MIN_SERVER_SPACE_UTIL_THRESHOLD))
		return;

	for (uint32_t i = 0; i < servers.size(); i++) {
		if (servers[i]->GetSpaceUtilization() < MIN_SERVER_SPACE_UTIL_THRESHOLD)
			nonloadedServers.push_back(servers[i]);
		else if (servers[i]->GetSpaceUtilization() > MAX_SERVER_SPACE_UTIL_THRESHOLD)
			loadedServers.push_back(servers[i]);
	}

	for (CSMapIter iter = mChunkToServerMap.begin();
		iter != mChunkToServerMap.end(); iter++) {
		chunkId_t chunkId = iter->first;
		ChunkPlacementInfo &clli = iter->second;

		if (mNumOngoingReplications > MAX_CONCURRENT_REPLICATIONS)
			break;

		// If this chunk is already being replicated or it is busy, skip
		if ((clli.ongoingReplications > 0) ||
			(!CanReplicateChunkNow(chunkId, clli, extraReplicas)))
				continue;

		// if we got too many copies of this chunk, don't bother
		if (extraReplicas < 0)
			continue;

		// chunk could be moved around if it is hosted on a loaded server
		bool canMoveChunk = false;
		for (uint32_t i = 0; i < loadedServers.size(); i++) {
			if (IsChunkHostedOnServer(clli.chunkServers, loadedServers[i])) {
				canMoveChunk = true;
				break;
			}
		}

		if (!canMoveChunk)
			continue;

		// find candidates
		FindCandidateServers(servers, nonloadedServers, clli.chunkServers);

		if (servers.size() == 0)
			// no candidates :-(
			continue;
		// get the chunk version
		vector<MetaChunkInfo *> v;
		vector<MetaChunkInfo *>::iterator chunk;

		metatree.getalloc(clli.fid, v);
		chunk = find_if(v.begin(), v.end(), ChunkIdMatcher(chunkId));
		if (chunk == v.end())
			continue;
		MetaChunkInfo *mci = *chunk;

		clli.ongoingReplications++;
		mNumOngoingReplications++;

		// add this chunk to the target set of chunkIds that we are tracking
		// for replication status change
		ChangeChunkReplication(chunkId);
		uint32_t numCopies = min(servers.size(), clli.chunkServers.size());

		for (uint32_t i = 0; i < numCopies; i++) {
			assert(!IsChunkHostedOnServer(clli.chunkServers, servers[i]));
			if (IsChunkHostedOnServer(clli.chunkServers, servers[i])) {
				string s = servers[i]->GetServerLocation().ToString();
				KFS_LOG_DEBUG("ERROR Chunk (%lld) can't be moved as it is already hosted on %s",
						chunkId, s.c_str());
				continue;
			}

			string s = clli.chunkServers[i]->GetServerLocation().ToString();
			string d = servers[i]->GetServerLocation().ToString();

			KFS_LOG_INFO("Trying to move chunk(%lld): from %s to %s", chunkId,
				s.c_str(), d.c_str());

			servers[i]->ReplicateChunk(clli.fid, chunkId, mci->chunkVersion,
					clli.chunkServers[i]->GetServerLocation());
		}
	}
}
