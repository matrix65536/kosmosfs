//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/DiskManager.cc#3 $
//
// Created 2006/03/22
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

#include "DiskManager.h"
#include "Globals.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <aio.h>
#include <string.h>

#include <cerrno>

#include "common/log.h"

using namespace libkfsio;

///
/// \file DiskManager.cc
/// \brief Implements methods defined in DiskManager.h
///

DiskManager::DiskManager()
{
    mDiskManagerTimeoutImpl = new DiskManagerTimeoutImpl(this);
}

DiskManager::~DiskManager()
{
    globals().netManager.UnRegisterTimeoutHandler(mDiskManagerTimeoutImpl);

    delete mDiskManagerTimeoutImpl;
}

void
DiskManager::Init()
{
    globals().netManager.RegisterTimeoutHandler(mDiskManagerTimeoutImpl);
}

///
/// When a timeout occurs, check the status of the scheduled disk
/// events.  For those events that have not been cancelled, call them
/// back with the result of the event.
///
void
DiskManager::Timeout()
{
    int aioRes, aioStatus;
    list<DiskEventPtr>::iterator iter;
    DiskEventPtr event;

    // walk the list of aio's and for each one that has finished,
    // remove it from the list and callback the associated
    // connection.  remove the event from the queue.

    for (iter = mDiskEvents.begin(); iter != mDiskEvents.end(); ) {
        event = *iter;
        if (event->status == EVENT_CANCELLED) {
            iter = mDiskEvents.erase(iter);
            continue;
        }
        aioStatus = aio_error(&(event->aio_cb));
        switch (aioStatus) {
            case EINPROGRESS:
                break;
            case ECANCELED:
                // remove the event from the queue.
                break;
            case 0:
            default:
                if (aioStatus != 0) {
                    COSMIX_LOG_DEBUG("AIO for event: %s, returned (errno value): %d", 
                                     event->ToString(),
                                     aioStatus);
                }
                // completed successfully or there was an error.
                aioRes = aio_return(&(event->aio_cb));
                // if aioRes = -1, aioStatus contains the value of errno
                
                /*
                COSMIX_LOG_INFO("AIO done for event: %s (bytes = %d)", 
                                event->ToString(),
                                event->aio_cb.aio_nbytes);
                */

                event->status = EVENT_DONE;
		if ((event->op == OP_READ) &&
                    (aioRes > 0)) {
                    // if the read finished successfully, aioRes
                    // contains the # of bytes that were read
                    event->data->Fill(aioRes);
                }

                event->retVal = aioRes;

                event->conn->HandleDone(event, aioStatus);
                iter = mDiskEvents.erase(iter);
                break;
        }
    }
}

///
/// See the comments in DiskManager.h.  
///
int
DiskManager::Read(DiskConnection *conn, int fd,
                  IOBufferDataPtr &data,
                  off_t offset, int numBytes,
                  DiskEventPtr &resultEvent)
{
    DiskEventPtr event(new DiskEvent_t(conn->shared_from_this(), data, OP_READ));
    struct aiocb *aio_cb = &event->aio_cb;

/*
    COSMIX_LOG_DEBUG("reading from fd=%d at offset=%d, numbytes = %d",
                     fd, offset, numBytes);
*/

    // schedule a read request
    aio_cb->aio_fildes = fd;
    aio_cb->aio_offset = offset;
    aio_cb->aio_nbytes = numBytes;
    aio_cb->aio_buf = data->Producer();
    if (aio_read(aio_cb) < 0) {
        perror("aio_read: ");
        return -1;
    }
    mDiskEvents.push_back(event);
    resultEvent = event;

    return 0;
}

///
/// See the comments in DiskManager.h.  
///
int
DiskManager::Write(DiskConnection *conn, int fd,
                   IOBufferDataPtr &data,
                   off_t offset, int numBytes,
                   DiskEventPtr &resultEvent)
{
    DiskEventPtr event(new DiskEvent_t(conn->shared_from_this(), data, OP_WRITE));
    struct aiocb *aio_cb = &event->aio_cb;

/*
    COSMIX_LOG_DEBUG("writing at fd=%d at offset=%d, numbytes = %d",
                     fd, offset, numBytes);
*/

    assert(numBytes <= data->BytesConsumable());

    // schedule a write request
    aio_cb->aio_fildes = fd;
    aio_cb->aio_offset = offset;
    aio_cb->aio_nbytes = numBytes;
    aio_cb->aio_buf = data->Consumer();
    if (aio_write(aio_cb) < 0) {
        perror("aio_write: ");
        return -1;
    }
    mDiskEvents.push_back(event);
    resultEvent = event;

    return 0;
}

///
/// For a sync, we use O_DSYNC.  This only sync's the data, but
/// doesn't update the associated inode information.  We do this to
/// save a disk I/O.  Should updating the inode information become
/// important, replace O_DSYNC in the aio_fsync() with O_SYNC.
///
int
DiskManager::Sync(DiskConnection *conn, int fd,
                  DiskEventPtr &resultEvent)
{
    DiskEventPtr event(new DiskEvent_t(conn->shared_from_this(), OP_SYNC));
    struct aiocb *aio_cb = &event->aio_cb;

    COSMIX_LOG_DEBUG("syncing fd = %d", fd);

    // schedule a datasync request
    aio_cb->aio_fildes = fd;
    if (aio_fsync(O_DSYNC, aio_cb) < 0) {
        perror("aio_sync: ");
        return -1;
    }
    mDiskEvents.push_back(event);
    resultEvent = event;

    return 0;
}
