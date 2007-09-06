//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/Globals.h#3 $
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
// \brief Define the globals needed by the KFS IO library.  These
//globals are also available to any app that uses the KFS IO library.
//----------------------------------------------------------------------------

#ifndef LIBKFSIO_GLOBALS_H
#define LIBKFSIO_GLOBALS_H

#include "DiskManager.h"
#include "NetManager.h"
#include "EventManager.h"
#include "Counter.h"

namespace libkfsio
{

    struct Globals_t {
        DiskManager diskManager;
        NetManager netManager;
        EventManager eventManager;
        CounterManager counterManager;
        // Commonly needed counters
        Counter ctrOpenNetFds;
        Counter ctrOpenDiskFds;
        Counter ctrNetBytesRead;
        Counter ctrNetBytesWritten;
        Counter ctrDiskBytesRead;
        Counter ctrDiskBytesWritten;
        // track the # of failed read/writes
        Counter ctrDiskIOErrors;
    };

    void InitGlobals();

    Globals_t & globals();
}

#endif // LIBKFSIO_GLOBALS_H
