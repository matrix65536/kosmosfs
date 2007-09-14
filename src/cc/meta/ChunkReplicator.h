//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/ChunkReplicator.h#4 $
//
// Created 2007/01/18
// Author: Sriram Rao (Kosmix Corp.)
//
// Copyright 2007 Kosmix Corp.
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
// \brief A chunk replicator is used to replicate chunks when necessary. For
// instance, if a chunkserver dies, the degree of replication for that
// chunk may be less than the desired amount.  In such cases,
// replicate that chunk.  This process works by sending a
// "MetaChunkReplicate" op to do the work.
//
//----------------------------------------------------------------------------

#ifndef META_CHUNKREPLICATOR_H
#define META_CHUNKREPLICATOR_H

#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/Event.h"
#include "request.h"

namespace KFS
{

class ChunkReplicator : public KfsCallbackObj {
public:
	/// The interval with which we check if chunks are sufficiently replicated
	static const int REPLICATION_CHECK_INTERVAL_SECS = 300;
	static const int REPLICATION_CHECK_INTERVAL_MSECS = 
				REPLICATION_CHECK_INTERVAL_SECS * 1000;

	ChunkReplicator();
	int HandleEvent(int code, void *data);

private:
	/// If a replication op is in progress, skip a send
	bool mInProgress;
	/// The event registered with the Event Manager to signify timeout events
	EventPtr mEvent;
	/// The op for checking
	MetaChunkReplicationCheck mOp;
};

}

#endif // META_CHUNKREPLICATOR_H
