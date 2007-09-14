//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/LeaseCleaner.cc#3 $
//
// Created 2006/10/16
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
