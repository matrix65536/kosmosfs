//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/LeaseCleaner.cc#3 $
//
// Created 2006/10/16
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
//----------------------------------------------------------------------------


#include "LeaseCleaner.h"
using namespace KFS;

#include "libkfsIO/Globals.h"
#include <cassert>

LeaseCleaner::LeaseCleaner() :
	mInProgress(false), mOp(1, this) 
{ 
	SET_HANDLER(this, &LeaseCleaner::HandleEvent);
	/// setup a periodic event to do the cleanup
	mEvent.reset(new Event(this, NULL, CLEANUP_INTERVAL_MSECS, true));
	libkfsio::globals().eventManager.Schedule(mEvent, CLEANUP_INTERVAL_MSECS);
}

/// Use the main looop to submit the cleanup request.
int
LeaseCleaner::HandleEvent(int code, void *data)
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
