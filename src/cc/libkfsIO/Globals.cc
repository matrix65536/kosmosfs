//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/Globals.cc#3 $
//
// Created 2006/10/09
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
// \brief Define the symbol for the KFS IO library global variables.

//----------------------------------------------------------------------------

#include "Globals.h"
using namespace libkfsio;

Globals_t & libkfsio::globals()
{
    static Globals_t g;
    return g;
}

//
// Initialize the globals once.
//
void
libkfsio::InitGlobals()
{
    static bool calledOnce = false;

    if (calledOnce) 
        return;

    calledOnce = true;

    globals().diskManager.Init();
    globals().eventManager.Init();

    globals().ctrOpenNetFds.SetName("Open network fds");
    globals().ctrOpenDiskFds.SetName("Open disk fds");
    globals().ctrNetBytesRead.SetName("Bytes read from network");
    globals().ctrNetBytesWritten.SetName("Bytes written to network");
    globals().ctrDiskBytesRead.SetName("Bytes read from disk");
    globals().ctrDiskBytesWritten.SetName("Bytes written to disk");
        // track the # of failed read/writes
    globals().ctrDiskIOErrors.SetName("Disk I/O errors");

    globals().counterManager.AddCounter(&globals().ctrOpenNetFds);
    globals().counterManager.AddCounter(&globals().ctrOpenDiskFds);
    globals().counterManager.AddCounter(&globals().ctrNetBytesRead);
    globals().counterManager.AddCounter(&globals().ctrNetBytesWritten);
    globals().counterManager.AddCounter(&globals().ctrDiskBytesRead);
    globals().counterManager.AddCounter(&globals().ctrDiskBytesWritten);
    globals().counterManager.AddCounter(&globals().ctrDiskIOErrors);
}
