//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/06/06
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
// \file LayoutManager.h
// \brief Layout manager is responsible for laying out chunks on chunk
// servers.  Model is that, when a chunkserver connects to the meta
// server, the layout manager gets notified; the layout manager then
// uses the chunk server for data placement.
//
//----------------------------------------------------------------------------

#ifndef META_LAYOUTMANAGER_H
#define META_LAYOUTMANAGER_H

#include <map>
#include <vector>
#include <set>

#include "kfstypes.h"
#include "meta.h"
#include "queue.h"
#include "ChunkServer.h"
#include "LeaseCleaner.h"
#include "ChunkReplicator.h"

#include "libkfsIO/Counter.h"

namespace KFS
{
	/// Model for leases: metaserver assigns write leases to chunkservers;
	/// clients/chunkservers can grab read lease on a chunk at any time.
	/// The server will typically renew unexpired leases whenever asked.
	/// As long as the lease is valid, server promises not to chnage
	/// the lease's version # (also, chunk won't disappear as long as
	/// lease is valid).
	struct LeaseInfo {
		LeaseInfo(LeaseType t, int64_t i): leaseType(t), leaseId(i)
		{
			time(&expires);
			// default lease time of 1 min
			expires += LEASE_INTERVAL_SECS;
		}
		LeaseInfo(LeaseType t, int64_t i, ChunkServerPtr &c):
			leaseType(t), leaseId(i), chunkServer(c)
		{
			time(&expires);
			// default lease time of 1 min
			expires += LEASE_INTERVAL_SECS;
		}
		static bool IsValidLease(const LeaseInfo &l)
		{
			time_t now = time(0);
			return now <= l.expires;
		}
		static bool IsValidWriteLease(const LeaseInfo &l)
		{
			return (l.leaseType == WRITE_LEASE) &&
				IsValidLease(l);
		}

		LeaseType leaseType;
		int64_t leaseId;
		// set for a write lease
		ChunkServerPtr chunkServer;
		time_t expires;
	};

	// Given a chunk-id, where is stored and who has the lease(s)
	struct ChunkPlacementInfo {
		ChunkPlacementInfo() : 
			fid(-1), ongoingReplications(0) { }
		// For cross-validation, we store the fid here.  This
		// is also useful during re-replication: given a chunk, we
		// can get its fid and from all the attributes of the file
		fid_t fid;
		/// is this chunk being (re) replicated now?  if so, how many
		int ongoingReplications;
		std::vector<ChunkServerPtr> chunkServers;
		std::vector<LeaseInfo> chunkLeases;
	};

	// To support rack-aware placement, we need an estimate of how much
	// space is available on each given rack.  Once the set of candidate
	// racks are ordered, we walk down the sorted list to pick the
	// desired # of servers.  For ordering purposes, we track how much space
	// each machine on the rack exports and how much space we have parceled
	// out; this gives us an estimate of availble space (we are
	// over-counting because the space we parcel out may not be fully used).
	class RackInfo {
		uint32_t mRackId;
		uint64_t mTotalSpace;
		uint64_t mAllocSpace;
		// set of servers on this rack
		std::vector<ChunkServerPtr> mServers;
	public:
		RackInfo(int id) : mRackId(id), mTotalSpace(0), mAllocSpace(0) { }

		inline uint32_t id() const {
			return mRackId;
		}
		void clear() {
			mTotalSpace = mAllocSpace = 0;
		}
		void addServer(ChunkServerPtr &s) {
			mTotalSpace += s->GetTotalSpace();
			mAllocSpace += s->GetUsedSpace();
			mServers.push_back(s);
		}
		void removeServer(ChunkServer *server) {
			std::vector <ChunkServerPtr>::iterator iter;
			
			iter = find_if(mServers.begin(), mServers.end(),
					ChunkServerMatcher(server));
			if (iter == mServers.end())
				return;

			mTotalSpace -= server->GetTotalSpace();
			mAllocSpace -= server->GetUsedSpace();
			mServers.erase(iter);
		}
		void computeSpace() {
			clear();
			for(std::vector<ChunkServerPtr>::iterator iter = mServers.begin();
				iter != mServers.end(); iter++) {
				ChunkServerPtr s = *iter;
				mTotalSpace += s->GetTotalSpace();
				mAllocSpace += s->GetUsedSpace();
			}
		}
		const std::vector<ChunkServerPtr> &getServers() {
			return mServers;
		}

		uint64_t availableSpace() const {
			if (mTotalSpace < mAllocSpace)
				// paranoia...
				return 0;
			return mTotalSpace - mAllocSpace;
		}
		// want to sort in decreasing order so that racks with more
		// space are at the head of list (and so, a node from them will
		// get chosen).
		bool operator < (const RackInfo &other) const {
			uint64_t mine;

			mine = availableSpace();
			if (mine == 0)
				return false;
			return mine < other.availableSpace();
		}
	};

	// Functor to enable matching of a rack-id with a RackInfo
	class RackMatcher {
		uint32_t id;
	public:
		RackMatcher(uint32_t rackId) : id(rackId) { }
		bool operator()(const RackInfo &rack) const {
			return rack.id() == id;
		}
	};

	// chunkid to server(s) map
	typedef std::map <chunkId_t, ChunkPlacementInfo > CSMap;
	typedef std::map <chunkId_t, ChunkPlacementInfo >::const_iterator CSMapConstIter;
	typedef std::map <chunkId_t, ChunkPlacementInfo >::iterator CSMapIter;
	
	// candidate set of chunks whose replication needs checking
	typedef std::set <chunkId_t> CRCandidateSet;
	typedef std::set <chunkId_t>::iterator CRCandidateSetIter;

        ///
        /// LayoutManager is responsible for write allocation:
        /// it determines where to place a chunk based on metrics such as,
        /// which server has the most space, etc.  Will eventually be
        /// extend this model to include replication.
        ///
        /// Allocating space for a chunk is a 3-way communication:
        ///  1. Client sends a request to the meta server for
        /// allocation
        ///  2. Meta server picks a chunkserver to hold the chunk and
        /// then sends an RPC to that chunkserver to create a chunk.
        ///  3. The chunkserver creates a chunk and replies to the
        /// meta server's RPC.
        ///  4. Finally, the metaserver logs the allocation request
        /// and then replies to the client.
        ///
        /// In this model, the layout manager picks the chunkserver
        /// location and queues the RPC to the chunkserver.  All the
        /// communication is handled by a thread in NetDispatcher
        /// which picks up the RPC request and sends it on its merry way.
        ///
	class LayoutManager {
	public:
		LayoutManager();

		virtual ~LayoutManager() { }

                /// A new chunk server has joined and sent a HELLO message.
                /// Use it to configure information about that server
                /// @param[in] r  The MetaHello request sent by the
                /// new chunk server.
		void AddNewServer(MetaHello *r);

                /// Our connection to a chunkserver went down.  So,
                /// for all chunks hosted on this server, update the
                /// mapping table to indicate that we can't
                /// get to the data.
                /// @param[in] server  The server that is down
		void ServerDown(ChunkServer *server);

		int RetireServer(const ServerLocation &loc);

                /// Allocate space to hold a chunk on some
                /// chunkserver.
                /// @param[in] r The request associated with the
                /// write-allocation call.
                /// @retval 0 on success; -1 on failure
		int AllocateChunk(MetaAllocate *r);

		/// A chunkid has been previously allocated.  The caller
		/// is trying to grab the write lease on the chunk. If a valid
		/// lease exists, we return it; otherwise, we assign a new lease,
		/// bump the version # for the chunk and notify the caller.
		///
                /// @param[in] r The request associated with the
                /// write-allocation call.
		/// @param[out] isNewLease  True if a new lease has been
		/// issued, which tells the caller that a version # bump
		/// for the chunk has been done.
                /// @retval status code
		int GetChunkWriteLease(MetaAllocate *r, bool &isNewLease);

                /// Delete a chunk on the server that holds it.
                /// @param[in] chunkId The id of the chunk being deleted
		void DeleteChunk(chunkId_t chunkId);

                /// A chunkserver is notifying us that a chunk it has is
		/// corrupt; so update our tables to reflect that the chunk isn't
		/// hosted on that chunkserver any more; re-replication will take
		/// care of recovering that chunk.
                /// @param[in] r  The request that describes the corrupted chunk
		void ChunkCorrupt(MetaChunkCorrupt *r);

                /// Truncate a chunk to the desired size on the server that holds it.
                /// @param[in] chunkId The id of the chunk being
                /// truncated
		/// @param[in] sz    The size to which the should be
                /// truncated to.
		void TruncateChunk(chunkId_t chunkId, off_t sz);

		/// Handlers to acquire and renew leases.  Unexpired leases
		/// will typically be renewed.
		int GetChunkReadLease(MetaLeaseAcquire *r);
		int LeaseRenew(MetaLeaseRenew *r);

		/// Is a valid lease issued on any of the chunks in the
		/// vector of MetaChunkInfo's?
		bool IsValidLeaseIssued(const std::vector <MetaChunkInfo *> &c);

                /// Add a mapping from chunkId -> server.
                /// @param[in] chunkId  chunkId that has been stored
                /// on server c
                /// @param[in] fid  fileId associated with this chunk.
                /// @param[in] c   server that stores chunk chunkId.
                ///   If c == NULL, then, we update the table to
                /// reflect chunk allocation; whenever chunk servers
                /// start up and tell us what chunks they have, we
                /// line things up and see which chunk is stored where.
		void AddChunkToServerMapping(chunkId_t chunkId, fid_t fid, ChunkServer *c);

		/// Remove the mappings for a chunk.
                /// @param[in] chunkId  chunkId for which mapping needs to be nuked.
		void RemoveChunkToServerMapping(chunkId_t chunkId);

                /// Update the mapping from chunkId -> server.
		/// @param[in] chunkId  chunkId that has been stored
		/// on server c
		/// @param[in] c   server that stores chunk chunkId.
		/// @retval  0 if update is successful; -1 otherwise
		/// Update will fail if chunkId is not present in the
		/// chunkId -> server mapping table.
		int UpdateChunkToServerMapping(chunkId_t chunkId, ChunkServer *c);

                /// Get the mapping from chunkId -> server.
                /// @param[in] chunkId  chunkId that has been stored
                /// on some server(s)
                /// @param[out] c   server(s) that stores chunk chunkId
                /// @retval 0 if a mapping was found; -1 otherwise
                ///
		int GetChunkToServerMapping(chunkId_t chunkId, std::vector<ChunkServerPtr> &c);

                /// Ask each of the chunkserver's to dispatch pending RPCs
		void Dispatch();

		/// For monitoring purposes, dump out state of all the
		/// connected chunk servers.
		/// @param[out] result  The string containing the
		/// state of the chunk servers.
		void Ping(string &result);

		/// Periodically, walk the table of chunk -> [location, lease]
		/// and remove out dead leases.
		void LeaseCleanup();

		/// Cleanup the lease for a particular chunk
		/// @param[in] chunkId  the chunk for which leases need to be cleaned up
		/// @param[in] v   the placement/lease info for the chunk
		void LeaseCleanup(chunkId_t chunkId, ChunkPlacementInfo &v);

		/// Handler that loops thru the chunk->location map and determines
		/// if there are sufficient copies of each chunk.  Those chunks with
		/// fewer copies are (re) replicated.
		void ChunkReplicationChecker();

		/// A chunk replication operation finished.  If the op was successful,
		/// then, we update the chunk->location map to record the presence
		/// of a new replica.
		/// @param[in] req  The op that we sent to a chunk server asking
		/// it to do the replication.
		void ChunkReplicationDone(MetaChunkReplicate *req);

		/// Degree of replication for chunk has changed.  When the replication
		/// checker runs, have it check the status for this chunk.
		/// @param[in] chunkId  chunk whose replication level needs checking
		///
		void ChangeChunkReplication(chunkId_t chunkId);

		/// Get all the fid's for which there is an open lease (read/write).
		/// This is useful for reporting purposes.
		/// @param[out] openForRead, openForWrite: the pathnames of files
		/// that are open for reading/writing respectively
		void GetOpenFiles(std::string &openForRead, std::string &openForWrite);

		void InitRecoveryStartTime()
		{
			mRecoveryStartTime = time(0);
		}

		void SetMinChunkserversToExitRecovery(uint32_t n) {
			mMinChunkserversToExitRecovery = n;
		}

		void ToggleRebalancing(bool v) {
			mIsRebalancingEnabled = v;
		}

        private:
		/// A rolling counter for tracking leases that are issued to
		/// to clients/chunkservers for reading/writing chunks
		int64_t mLeaseId;

		/// A counter to track the # of ongoing chunk replications
		int mNumOngoingReplications;


		/// A switch to toggle rebalancing: if the system is under load,
		/// we'd like to turn off rebalancing.  We can enable it a
		/// suitable time.
		bool mIsRebalancingEnabled;

		/// On each iteration, we try to rebalance some # of blocks;
		/// this counter tracks the last chunk we checked
		kfsChunkId_t mLastChunkRebalanced;

		/// When a server goes down or needs retiring, we start
		/// replicating blocks.  Whenever a replication finishes, we
		/// find the next candidate.  We need to track "where" we left off
		/// on a previous iteration, so that we can start from there and
		/// run with it.
		kfsChunkId_t mLastChunkReplicated;

		/// After a crash, track the recovery start time.  For a timer
		/// period that equals the length of lease interval, we only grant
		/// lease renews and new leases to new chunks.  We however,
		/// disallow granting new leases to existing chunks.  This is
		/// because during the time period that corresponds to a lease interval,
		/// we may learn about leases that we had handed out before crashing.
		time_t mRecoveryStartTime;

		/// Periodically clean out dead leases
		LeaseCleaner mLeaseCleaner;

		/// Similar to the lease cleaner: periodically check if there are
		/// sufficient copies of each chunk.
		ChunkReplicator mChunkReplicator;

		uint32_t mMinChunkserversToExitRecovery;

                /// List of connected chunk servers.
                std::vector <ChunkServerPtr> mChunkServers;

		/// State about how each rack (such as, servers/space etc)
		std::vector<RackInfo> mRacks;

                /// Mapping from a chunk to its location(s).
                CSMap mChunkToServerMap;

                /// Candidate set of chunks whose replication needs checking
                CRCandidateSet mChunkReplicationCandidates;

		/// Counters to track chunk replications
		Counter *mOngoingReplicationStats;
		Counter *mTotalReplicationStats;
		/// Track the # of replication ops that failed
		Counter *mFailedReplicationStats;
		/// Track the # of stale chunks we have seen so far
		Counter *mStaleChunkCount;

		/// Find a set of racks to place a chunk on; the racks are
		/// ordered by space.
		void FindCandidateRacks(std::vector<int> &result);

		/// Find a set of racks to place a chunk on; the racks are
		/// ordered by space.  The set excludes defines the set of racks
		/// that should be excluded from consideration.
		void FindCandidateRacks(std::vector<int> &result, const std::set<int> &excludes);

		/// Helper function to generate candidate servers
		/// for hosting a chunk.  The list of servers returned is
		/// ordered in decreasing space availability.
		/// @param[out] result  The set of available servers
		/// @param[in] excludes  The set of servers to exclude from
		///    candidate generation.
		/// @param[in] rackId   The rack to restrict the candidate
		/// selection to; if rackId = -1, then all servers are fair game
		void FindCandidateServers(std::vector<ChunkServerPtr> &result,
					const std::vector<ChunkServerPtr> &excludes, 
					int rackId = -1);

		/// Helper function to generate candidate servers from
		/// the specified set of sources for hosting a chunk.  
		/// The list of servers returned is
		/// ordered in decreasing space availability.
		/// @param[out] result  The set of available servers
		/// @param[in] sources  The set of possible source servers 
		/// @param[in] excludes  The set of servers to exclude from
		/// @param[in] rackId   The rack to restrict the candidate
		/// selection to; if rackId = -1, then all servers are fair game
		void FindCandidateServers(std::vector<ChunkServerPtr> &result,
					const std::vector<ChunkServerPtr> &sources,
					const std::vector<ChunkServerPtr> &excludes,
					int rackId = -1);

		/// Helper function that takes a set of servers and sorts
		/// them by space utilization.  The list of servers returned is
		/// ordered on increasing space utilization (i.e., decreasing 
		/// space availability).
		/// @param[in/out] servers  The set of servers we want sorted
		void SortServersByUtilization(vector<ChunkServerPtr> &servers);

		/// Check the # of copies for the chunk and return true if the
		/// # of copies is less than targeted amount.  We also don't replicate a chunk
		/// if it is currently being written to (i.e., if a write lease
		/// has been issued).
		/// @param[in] chunkId   The id of the chunk which we are checking
		/// @param[in] clli  The lease/location information about the chunk.
		/// @param[out] extraReplicas  The target # of additional replicas for the chunk
		/// @retval true if the chunk is to be replicated; false otherwise
		bool CanReplicateChunkNow(chunkId_t chunkId, 
				ChunkPlacementInfo &clli,
				int &extraReplicas);

		/// Replicate a chunk.  This involves finding a new location for
		/// the chunk that is different from the existing set of replicas
		/// and asking the chunkserver to get a copy.
		/// @param[in] chunkId   The id of the chunk which we are checking
		/// @param[in] clli  The lease/location information about the chunk.
		/// @param[in] extraReplicas  The target # of additional replicas for the chunk
		/// @param[in] candidates   The set of servers on which the additional replicas 
		/// 				should be stored
		/// @retval  The # of actual replications triggered
		int ReplicateChunk(chunkId_t chunkId, const ChunkPlacementInfo &clli,
				uint32_t extraReplicas);
		int ReplicateChunk(chunkId_t chunkId, const ChunkPlacementInfo &clli,
				uint32_t extraReplicas, const std::vector<ChunkServerPtr> &candidates);

		/// The server has finished re-replicating a chunk.  If there is more
		/// re-replication to be done, send it the server's way.
		/// @param[in] server  The server to which re-replication work should be sent
		void FindReplicationWorkForServer(ChunkServerPtr &server);

		/// There are more replicas of a chunk than the requested amount.  So,
		/// delete the extra replicas and reclaim space.  When deleting the addtional
		/// copies, find the servers that are low on space and delete from there.
		/// As part of deletion, we update our mapping of where the chunk is stored.
		/// @param[in] chunkId   The id of the chunk which we are checking
		/// @param[in] clli  The lease/location information about the chunk.
		/// @param[in] extraReplicas  The # of replicas that need to be deleted
		void DeleteAddlChunkReplicas(chunkId_t chunkId, ChunkPlacementInfo &clli,
				uint32_t extraReplicas);

		/// Helper function to check set membership.
		/// @param[in] hosters  Set of servers hosting a chunk
		/// @param[in] server   The server we want to check for membership in hosters.
		/// @retval true if server is a member of the set of hosters; 
		///         false otherwise
		bool IsChunkHostedOnServer(const vector<ChunkServerPtr> &hosters,
						const ChunkServerPtr &server);

		/// Periodically, update our estimate of how much space is
		/// used/available in each rack.
		void UpdateRackSpaceUsageCounts();

		/// Periodically, rebalance servers by moving chunks around from
		/// "over utilized" servers to "under utilized" servers.
		void RebalanceServers();
		void FindIntraRackRebalanceCandidates(vector<ChunkServerPtr> &candidates,
				const vector<ChunkServerPtr> &nonloadedServers,
				const ChunkPlacementInfo &clli);

		void FindInterRackRebalanceCandidate(ChunkServerPtr &candidate,
				const vector<ChunkServerPtr> &nonloadedServers,
				const ChunkPlacementInfo &clli);


		/// Return true if c is a server in mChunkServers[].
		bool ValidServer(ChunkServer *c);

		/// For a time period that corresponds to the length of a lease interval,
		/// we are in recovery after a restart.
		/// Also, if the # of chunkservers that are connected to us is
		/// less than some threshold, we are in recovery mode.
		bool InRecovery()
		{
			if (mChunkServers.size() < mMinChunkserversToExitRecovery)
				return true;
			time_t now = time(0);
			return now - mRecoveryStartTime <= 
				KFS::LEASE_INTERVAL_SECS;
		}

        };

        extern LayoutManager gLayoutManager;
}

#endif // META_LAYOUTMANAGER_H
