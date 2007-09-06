//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/ChunkReplicator.cc#3 $
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
// 
//----------------------------------------------------------------------------

#include "ChunkReplicator.h"
using namespace KFS;

#include "libkfsIO/Globals.h"
#include <cassert>

ChunkReplicator::ChunkReplicator() :
	mInProgress(false), mOp(1, this) 
{ 
	SET_HANDLER(this, &ChunkReplicator::HandleEvent);
	/// setup a periodic event to do the cleanup
	mEvent.reset(new Event(this, NULL, REPLICATION_CHECK_INTERVAL_MSECS, true));
	libkfsio::globals().eventManager.Schedule(mEvent, REPLICATION_CHECK_INTERVAL_MSECS);
}

/// Use the main loop to process the request.
int
ChunkReplicator::HandleEvent(int code, void *data)
{
	static seq_t seqNum = 1;
	switch (code) {
	case EVENT_CMD_DONE:
		mInProgress = false;
		return 0;
	case EVENT_TIMEOUT:
		if (mInProgress)
			return 0;
		mOp.opSeqno = seqNum;
		++seqNum;
		mInProgress = true;
		submit_request(&mOp);
		return 0;
	default:
		assert(!"Unknown event");
		break;
	}
	return 0;
}
