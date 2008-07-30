//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/10/09
// Author: Sriram Rao
//
// Copyright 2008 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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
// \brief Define the symbol for the KFS IO library global variables.

//----------------------------------------------------------------------------

#include "Globals.h"

using namespace KFS::libkfsio;

Globals_t & KFS::libkfsio::globals()
{
    static Globals_t g;
    return g;
}

//
// Initialize the globals once.
//
void
KFS::libkfsio::InitGlobals()
{
    static bool calledOnce = false;

    if (calledOnce) 
        return;

    calledOnce = true;

    globals().diskManager.Init();
    globals().eventManager.Init();

    globals().netKicker.Init(globals().netManager);

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
