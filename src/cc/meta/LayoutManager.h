//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/LayoutManager.h#3 $
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
using std::vector;
using std::map;

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
			fid(-1), isBeingReplicated(false) { }
		// For cross-validation, we store the fid here.  This
		// is also useful during re-replication: given a chunk, we
		// can get its fid and from all the attributes of the file
		fid_t fid;
		/// is this chunk being (re) replicated now?
		bool isBeingReplicated;
		vector<ChunkServerPtr> chunkServers;
		vector<LeaseInfo> chunkLeases;
	};

	// chunkid to server(s) map
	typedef map <chunkId_t, ChunkPlacementInfo > CSMap;
	typedef map <chunkId_t, ChunkPlacementInfo >::const_iterator CSMapConstIter;
	typedef map <chunkId_t, ChunkPlacementInfo >::iterator CSMapIter;

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

                /// Truncate a chunk to the desired size on the server that holds it.
                /// @param[in] chunkId The id of the chunk being
                /// truncated
		/// @param[in] sz    The size to which the should be
                /// truncated to.
		void TruncateChunk(chunkId_t chunkId, size_t sz);

		/// Handlers to acquire and renew leases.  Unexpired leases
		/// will typically be renewed.
		int GetChunkReadLease(MetaLeaseAcquire *r);
		int LeaseRenew(MetaLeaseRenew *r);

		/// Is a valid lease issued on any of the chunks in the
		/// vector of MetaChunkInfo's?
		bool IsValidLeaseIssued(const vector <MetaChunkInfo *> &c);

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
		int GetChunkToServerMapping(chunkId_t chunkId, vector<ChunkServerPtr> &c);

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
		void ChunkReplicationDone(const MetaChunkReplicate *req);

		void InitRecoveryStartTime()
		{
			mRecoveryStartTime = time(0);
		}

        private:
		/// A rolling counter for tracking leases that are issued to
		/// to clients/chunkservers for reading/writing chunks
		int64_t mLeaseId;

		/// A counter to track the # of ongoing chunk replications
		int mNumOngoingReplications;

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

                /// List of connected chunk servers.
                vector <ChunkServerPtr> mChunkServers;

                /// Mapping from a chunk to its location(s).
                CSMap mChunkToServerMap;

		/// Counters to track chunk replications
		Counter *mOngoingReplicationStats;
		Counter *mTotalReplicationStats;
		/// Track the # of replication ops that failed
		Counter *mFailedReplicationStats;

		/// Helper function to generate candidate servers
		/// for hosting a chunk.  The list of servers returned is
		/// ordered in decreasing space availability.
		/// @param[out] result  The set of available servers
		/// @param[in] excludes  The set of servers to exclude from
		///    candidate generation.
		void FindCandidateServers(vector<ChunkServerPtr> &result,
					const vector<ChunkServerPtr> &excludes);

		/// Check the # of copies for the chunk and return true if the
		/// # of copies is less than 3.  We also don't replicate a chunk
		/// if it is currently being written to (i.e., if a write lease
		/// has been issued).
		/// @param[in] chunkId   The id of the chunk which we are checking
		/// @param[in] clli  The lease/location information about the chunk.
		/// @retval true if the chunk is to be replicated; false otherwise
		bool ChunkNeedsReplication(chunkId_t chunkId, 
				ChunkPlacementInfo &clli);

		/// Replicate a chunk.  This involves finding a new location for
		/// the chunk that is different from the existing set of replicas
		/// and asking the chunkserver to get a copy.
		/// @param[in] chunkId   The id of the chunk which we are checking
		/// @param[in] clli  The lease/location information about the chunk.
		void ReplicateChunk(chunkId_t chunkId, 
				const ChunkPlacementInfo &clli);

		/// Return true if c is a server in mChunkServers[].
		bool ValidServer(ChunkServer *c);

		/// For a time period that corresponds to the length of a lease interval,
		/// we are in recovery after a restart.
		bool InRecovery()
		{
			time_t now = time(0);
			return now - mRecoveryStartTime <= 
				KFS::LEASE_INTERVAL_SECS;
		}

        };

        extern LayoutManager gLayoutManager;
}

#endif // META_LAYOUTMANAGER_H
