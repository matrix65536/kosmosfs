//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/LeaseCleaner.h#4 $
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
// \brief A lease cleaner to periodically cleanout dead leases.  It does so,
// by sending a "MetaLeaseCleanup" op to do the work.
//----------------------------------------------------------------------------

#ifndef META_LEASECLEANER_H
#define META_LEASECLEANER_H

#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/Event.h"
#include "request.h"

namespace KFS
{

class LeaseCleaner : public KfsCallbackObj {
public:
	// The interval with which we should do the cleanup
	static const int CLEANUP_INTERVAL_SECS = 60;
	static const int CLEANUP_INTERVAL_MSECS = CLEANUP_INTERVAL_SECS * 1000;

	LeaseCleaner();
	int HandleEvent(int code, void *data);

private:
	/// If a cleanup op is in progress, skip a send
	bool mInProgress;
	/// The event registered with the Event Manager to signify timeout events
	EventPtr mEvent;
	/// The op for doing the cleanup
	MetaLeaseCleanup mOp;
};

}

#endif // META_LEASECLEANER_H
