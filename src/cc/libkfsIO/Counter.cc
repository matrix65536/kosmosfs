//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/Counter.cc#3 $
//
// Created 2006/07/20
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
// \brief Implementation of the CounterManager and declarations for a
//few counters.
//----------------------------------------------------------------------------

#include "Counter.h"

/// A few commonly needed counters
Counter gOpenNetFds("Open network fds");
Counter gOpenDiskFds("Open disk fds");
Counter gNetBytesRead("Bytes read from network");
Counter gNetBytesWritten("Bytes written to network");
Counter gDiskBytesRead("Bytes read from disk");
Counter gDiskBytesWritten("Bytes written to disk");
// track the # of failed read/writes
Counter gDiskIOErrors("Disk I/O errors");

CounterManager gCounterManager;

void InitCounterManager()
{
    gCounterManager.AddCounter(&gOpenNetFds);
    gCounterManager.AddCounter(&gOpenDiskFds);

    gCounterManager.AddCounter(&gNetBytesRead);
    gCounterManager.AddCounter(&gNetBytesWritten);

    gCounterManager.AddCounter(&gDiskBytesRead);
    gCounterManager.AddCounter(&gDiskBytesWritten);

    gCounterManager.AddCounter(&gDiskIOErrors);

}


