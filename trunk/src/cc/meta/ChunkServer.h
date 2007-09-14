//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/ChunkServer.h#4 $
//
// Created 2006/06/05
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
// \file ChunkServer.h
// \brief Object that handles the communication with an individual
// chunk server. Model here is the following:
//  - For write-allocation, layout manager asks the ChunkServer object
// to send an RPC to the chunk server.
//  - The ChunkServer object sends the RPC and holds on to the request
// that triggered the RPC.
//  - Eventually, when the RPC reply is received, the request is
// re-activated (alongwith the response) and is sent back down the pike.
//
//----------------------------------------------------------------------------

#ifndef META_CHUNKSERVER_H
#define META_CHUNKSERVER_H

#include <string>
#include <sstream>
using std::string;
using std::ostringstream;

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/ITimeout.h"
#include "libkfsIO/NetConnection.h"
#include "request.h"
#include "queue.h"

#include "common/properties.h"


namespace KFS
{
        /// Chunk server connects to the meta server, sends a HELLO
        /// message to configure its state with the meta server,  and
        /// from then onwards, the meta server then drives the RPCs.
        /// Types of messages:
        ///   Meta server --> Chunk server: Allocate, Free, Heartbeat
        ///

        /// Something to trigger timeouts so that we send heartbeat to
        /// the chunk server.
        class ChunkServerTimeoutImpl;

        class ChunkServer : public KfsCallbackObj,
		public boost::enable_shared_from_this<ChunkServer> {
        public:
                ///
                /// Sequence:
                ///  Chunk server connects.
                ///   - A new chunkserver sm is born
                ///   - chunkserver sends a HELLO with config info
                ///   - send/recv messages with that chunkserver.
                ///
                ChunkServer(NetConnectionPtr &conn);
                ~ChunkServer();

                /// Handler to handle the HELLO message.  This method
                /// gets from the net manager when it sees some data
                /// is available on the socket.
                int HandleHello(int code, void *data);

                /// Generic event handler to handle network
                /// events. This method gets from the net manager when
                /// it sees some data is available on the socket.
                int HandleRequest(int code, void *data);

                /// Send an RPC to allocate a chunk on this server.
                /// An RPC request is enqueued and the call returns.
                /// When the server replies to the RPC, the request
                /// processing resumes.
                /// @param[in] r the request associated with the RPC call.
		/// @param[in] leaseId the id associated with the write lease.
                /// @retval 0 on success; -1 on failure
                ///
                int AllocateChunk(MetaAllocate *r, int64_t leaseId);

                /// Send an RPC to delete a chunk on this server.
                /// An RPC request is enqueued and the call returns.
                /// When the server replies to the RPC, the request
                /// processing resumes.
                /// @param[in] chunkId name of the chunk that is being
                ///  deleted.
                /// @retval 0 on success; -1 on failure
                ///
                int DeleteChunk(chunkId_t chunkId);

                /// Send an RPC to truncate a chunk.
                /// An RPC request is enqueued and the call returns.
                /// When the server replies to the RPC, the request
                /// processing resumes.
                /// @param[in] chunkId name of the chunk that is being
                ///  truncated
                /// @param[in] s   size to which chunk is being truncated to.
                /// @retval 0 on success; -1 on failure
                ///
		int TruncateChunk(chunkId_t chunkId, size_t s);

		/// Methods to handle (re) replication of a chunk.  If there are
		/// insufficient copies of a chunk, we replicate it.
		int ReplicateChunk(fid_t fid, chunkId_t chunkId, seq_t chunkVersion,
                                const ServerLocation &loc);

		/// Replication of a chunk finished.  Update statistics
		void ReplicateChunkDone() {
			mNumChunkReplications--;
			assert(mNumChunkReplications >= 0);
			if (mNumChunkReplications < 0)
				mNumChunkReplications = 0;
		}

		/// Accessor method to get # of replications that are being
		/// handled by this server.
		int GetNumChunkReplications() const {
			return mNumChunkReplications;
		}

                /// Periodically, send a heartbeat message to the
                /// chunk server.  The message is enqueued to the list
                /// of RPCs that need to be dispatched; whenever the
                /// dispatcher sends them out, the message goes.
                void Heartbeat();

                /// Whenever the layout manager determines that this
                /// server has stale chunks, it queues an RPC to
                /// notify the chunk server of the stale data.
                void NotifyStaleChunks(const vector<chunkId_t> &staleChunks);
                void NotifyStaleChunk(chunkId_t staleChunk);

                /// There is a difference between the version # as stored
		/// at the chunkserver and what is on the metaserver.  By sending
		/// this message, the metaserver is asking the chunkserver to change
		/// the version # to what is passed in.
                void NotifyChunkVersChange(fid_t fid, chunkId_t chunkId, seq_t chunkVers);

		/// Dispatch all the pending RPCs to the chunk server.
		void Dispatch();

		/// An op has been dispatched.  Stash a pointer to that op
		/// in the list of dispatched ops.
		/// @param[in] op  The op that was just dispatched
		void Dispatched(MetaRequest *r) {
			mDispatchedReqs.push_back(r);
		}

                ///
                /// We sent a request; we got a reply.  Take the op
                /// which has the response values filled in and resume
                /// processing for that op.
                /// @param[in] op  The op for which we got a reply
                /// from a chunkserver.
                ///
                void ResumeOp(MetaRequest *op);

                /// Accessor method to get the host name/port
                ServerLocation GetServerLocation() const {
			return mLocation;
                }

		string ServerID()
		{
			return mLocation.ToString();
		}

		/// Check if the hostname/port matches what is passed in
		/// @param[in] name  name to match
		/// @param[in] port  port # to match
		/// @retval true  if a match occurs; false otherwise
		bool MatchingServer(const ServerLocation &loc) const {
			return mLocation == loc;
		}

                /// Setter method to set the host name/port
                void SetServerLocation(const ServerLocation &loc) {
			mLocation = loc;
                }

                /// Setter method to set space
                void SetSpace(uint64_t total, uint64_t used, uint64_t alloc) {
			mTotalSpace = total;
			mUsedSpace = used;
			mAllocSpace = alloc; 
                }

                const char *GetServerName() {
                        return mLocation.hostname.c_str();
                }

		/// Available space is defined as the difference
		/// between the total storage space available
		/// on the server and the amount of space that
		/// has been parceled out for outstanding writes 
		/// by the meta server.  THat is, alloc space is tied
		/// to the chunks that have been write-leased.  This
		/// has the effect of keeping alloc space tied closely
		/// to used space.
                uint64_t GetAvailSpace() {
 			mAllocSpace = mUsedSpace + 
				(mNumChunkReplications + mNumChunkWrites) * 
					CHUNKSIZE;
			if (mAllocSpace >= mTotalSpace)
				return 0;
			else
                        	return mTotalSpace - mAllocSpace;
                }

		/// An estimate of the # of writes that are currently
		/// happening at this server.
		inline void UpdateNumChunkWrites(int amount) {
			mNumChunkWrites += amount;
			if (mNumChunkWrites < 0)
				mNumChunkWrites = 0;

		}

                uint64_t GetUsedSpace() {
                        return mUsedSpace;
                }

		bool IsDown() {
			return mDown;
		}

                ///
                /// The chunk server went down.  So, fail all the
                /// outstanding ops. 
                ///
                void FailPendingOps();

		/// For monitoring purposes, dump out state as a string.
		/// @param [out] result   The state of this server
		///
		void Ping(string &result);

		seq_t NextSeq() { return mSeqNo++; }

        private:
                /// A sequence # associated with each RPC we send to
                /// chunk server.  This variable tracks the seq # that
                /// we should use in the next RPC.
                seq_t mSeqNo;
                /// A handle to the network connection
                NetConnectionPtr mNetConnection;

                /// Periodically heartbeat the chunk server
                ChunkServerTimeoutImpl *mTimer;

                /// Are we thru with processing HELLO message
                bool mHelloDone;

		/// Boolean that tracks whether this server is down
		bool mDown;

                /// Is there a heartbeat message for which we haven't
		/// recieved a reply yet?  If yes, dont' send one more
                bool mHeartbeatSent;

                /// Location of the server at which clients can
                /// connect to
		ServerLocation mLocation;

                /// total space available on this server
                uint64_t mTotalSpace;
                /// space that has been used by chunks on this server
                uint64_t mUsedSpace;

                /// space that has been allocated for chunks: this
                /// corresponds to the allocations that have been
                /// made, but not all of the allocated space is used.
                /// For instance, when we have partially filled
                /// chunks, there is space is allocated for a chunk
                /// but that space hasn't been fully used up.
                uint64_t mAllocSpace;

		/// An estimate of the # of writes that are being handled
		/// by this server.  We use this value to update mAllocSpace
		/// The problem we have is that, we can end up with lots of
		/// partial chunks and over time such drift can significantly
		/// reduce the available space on the server (space is held
		/// down for by the partial chunks that may never be written to).
		/// Since writes can occur only when someone gets a valid write lease,
		/// we track the # of write leases that are issued and where the
		/// writes are occurring.  So, whenever we get a heartbeat, we
		/// can update alloc space as a sum of the used space and the # of
		/// writes that are currently being handled by this server.
		int mNumChunkWrites;


		/// Track the # of chunk replications that are going on this server
		int mNumChunkReplications;

                /// list of RPCs that need to be sent to this chunk
                /// server.  This list is shared between the main
                /// event processing loop and the network thread.
                MetaQueue <MetaRequest> mPendingReqs;
                /// list of RPCs that we have sent to this chunk
                /// server.  This list is operated by the network
                /// thread.
                list <MetaRequest *> mDispatchedReqs;

                ///
                /// We have received a message from the chunk
                /// server. Do something with it.
                /// @param[in] iobuf  An IO buffer stream with message
                /// received from the chunk server.
                /// @param[in] msgLen  Length in bytes of the message.
                /// @retval 0 if message was processed successfully;
                /// -1 if there was an error
                ///
                int HandleMsg(IOBuffer *iobuf, int msgLen);

		/// Handlers for the 3 types of messages we could get:
		/// 1. Hello message from a chunkserver
		/// 2. An RPC from a chunkserver
		/// 3. A reply to an RPC that we have sent previously.

		int HandleHelloMsg(IOBuffer *iobuf, int msgLen);
		int HandleCmd(IOBuffer *iobuf, int msgLen);
		int HandleReply(IOBuffer *iobuf, int msgLen);

		/// Send a response message to the MetaRequest we got.
		void SendResponse(MetaRequest *op);

                ///
                /// Given a response from a chunkserver, find the
                /// associated request that we previously sent.
                /// Request/responses are matched based on sequence
                /// numbers in the messages.
                ///
                /// @param[in] cseq The sequence # of the op we are
                /// looking for.
                /// @retval The matching request if one exists; NULL
                /// otherwise
                ///
                MetaRequest *FindMatchingRequest(seq_t cseq);

                ///
                /// The response sent by a chunkserver is of the form:
                /// OK \r\n
                /// Cseq: <seq #>\r\n
                /// Status: <status> \r\n\r\n
                /// Extract out Cseq, Status
                ///
                /// @param[in] buf Buffer containing the response
                /// @param[in] bufLen length of buf
                /// @param[out] prop  Properties object with the response header/values
                ///
                void ParseResponse(char *buf, int bufLen, Properties &prop);

                ///
                /// The chunk server went down.  So, stop the network timer event;
		/// also, fail all the dispatched ops.
                ///
		void StopTimer();
		void FailDispatchedOps();
        };

        class ChunkServerTimeoutImpl: public ITimeout {
        public:
                ChunkServerTimeoutImpl(ChunkServer *c) {
                        mChunkServer = c;
                        // send heartbeat once every min
                        SetTimeoutInterval(60 * 1000);
                };
                ~ChunkServerTimeoutImpl() {
			mChunkServer = NULL;
		};
                // On a timeout send a heartbeat RPC
                void Timeout() {
                        mChunkServer->Heartbeat();
                };
        private:
                ChunkServer *mChunkServer; //!< pointer to the owner (chunk server)
        };

	class ChunkServerMatcher {
		const ChunkServer *target;

	public:
		ChunkServerMatcher(const ChunkServer *t): target(t) { };
		bool operator() (ChunkServerPtr &c) {
			return c.get() == target;
		}
	};
}

#endif // META_CHUNKSERVER_H
