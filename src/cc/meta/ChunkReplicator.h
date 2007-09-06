//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/ChunkReplicator.h#4 $
//
// Created 2007/01/18
// Author: Sriram Rao (Kosmix Corp.)
//
// Copyright (C) 2007 Kosmix Corp.
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
