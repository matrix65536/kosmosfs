//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/DiskManager.h#3 $
//
// Created 2006/03/16
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

#ifndef _LIBIO_DISK_MANAGER_H
#define _LIBIO_DISK_MANAGER_H

#include <list>
using std::list;

#include "DiskConnection.h"
#include "NetManager.h"
#include "ITimeout.h"

///
/// \file DiskManager.h
/// \brief DiskManager uses aio to schedule disk operations.  It
/// periodically checks the status of the operations and notifies the
/// completion of the associated events.
///

///
/// \brief Define the length of a path name.
///
const int MAX_FILE_NAME_LEN = 256;

// forward declaration
class DiskManagerTimeoutImpl;

class DiskManager {
public:
    DiskManager();
    ~DiskManager();

    /// Register a timeout handler with the NetManager.  The
    /// NetManager will call the handler whenever a timeout occurs.
    void 	Init();

    /// Schedule a read on fd at the specified offset for specified #
    /// of bytes.
    /// @param[in] conn DiskConnection on which the read is scheduled
    /// @param[in] fd file descriptor that will be used in aio_read().
    /// @param[in] data IOBufferData to hold the result of the read
    /// @param[in] offset offset in the file at which read should start
    /// @param[in] numBytes # of bytes to be read
    /// @param[out] resultEvent DiskEventPtr that stores information
    /// about the scheduled read event.
    /// @retval 0 on success; -1 on failure
    int		Read(DiskConnection *conn, int fd,
                     IOBufferDataPtr &data,
                     off_t offset, int numBytes,
                     DiskEventPtr &resultEvent);

    /// Schedule a write
    /// @param[in] conn DiskConnection on which the write is scheduled
    /// @param[in] fd file descriptor that will be used in aio_write().
    /// @param[in] data IOBufferData that holds the data to be written.
    /// @param[in] offset offset in the file at which write should start
    /// @param[in] numBytes # of bytes to be written
    /// @param[out] resultEvent DiskEventPtr that stores information
    /// about the scheduled write event.
    /// @retval 0 on success; -1 on failure
    int 	Write(DiskConnection *conn, int fd,
                      IOBufferDataPtr &data,
                      off_t offset, int numBytes,
                      DiskEventPtr &resultEvent);

    /// Schedule a sync
    /// @param[in] conn DiskConnection on which the sync is scheduled
    /// @param[in] fd file descriptor that will be used in aio_sync().
    /// @param[out] resultEvent DiskEventPtr that stores information
    /// about the scheduled sync event.
    /// @retval 0 on success; -1 on failure
    int		Sync(DiskConnection *conn,
                     int fd, DiskEventPtr &resultEvent);


    /// Whenever a timeout occurs, walk the list of scheduled events
    /// to determine if any have finished; for completed ones, signal
    /// the associated event.
    void 	Timeout();
private:
    DiskManagerTimeoutImpl	*mDiskManagerTimeoutImpl;
    /// list of read/write/sync events that have been scheduled
    list<DiskEventPtr>		mDiskEvents;

};

///
/// \class DiskManagerTimeoutImpl
/// \brief Implements the ITimeout interface (@see ITimeout).
///
class DiskManagerTimeoutImpl : public ITimeout {
public:
    /// The owning object of a DiskManagerTimeoutImpl is the DiskManager.
    DiskManagerTimeoutImpl(DiskManager *mgr) {
        mDiskManager = mgr;
    };
    /// Callback the owning object whenever a timeout occurs.
    virtual void Timeout() {
        mDiskManager->Timeout();
    };
private:
    /// Owning object.
    DiskManager		*mDiskManager;

};

#endif // _LIBIO_DISK_MANAGER_H
