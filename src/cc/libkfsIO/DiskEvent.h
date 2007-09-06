//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/DiskEvent.h#3 $
//
// Created 2006/03/28
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

#ifndef _LIBIO_DISKEVENT_H
#define _LIBIO_DISKEVENT_H

#include <aio.h>
#include <boost/shared_ptr.hpp>

#include "Chunk.h"
#include "Event.h"
#include "IOBuffer.h"

///
/// \file DiskEvent.h
/// \brief Declarations related to events related to Disk-I/O.
///

///
/// \enum DiskEventOp_t
/// \brief Code corresponding to the various disk events: read/write/sync
///
enum DiskEventOp_t {
    OP_NONE,
    OP_READ,
    OP_WRITE,
    OP_SYNC
};

class DiskConnection;


///
/// \struct DiskEvent_t
/// \brief A disk event stores information about the event
/// (read/write/sync). 
/// Disk events are asynchronous events.  The DiskManager uses aio to
/// (1) schedule the events and (2) retrieve the result of the event
/// execution later.  A scheduled event can be cancelled at any time.
///
/// For I/O, a disk event has two pieces of information: (1) a
/// DiskConnection (@see DiskConnection), that encapsulates the
/// information about the file descriptor, and (2) a buffer for
/// I/O(@see IOBuffer). 
///
struct DiskEvent_t {
    DiskEvent_t(DiskConnectionPtr c, DiskEventOp_t o) {
        op = o;
        conn = c;
        memset(&aio_cb, 0, sizeof(struct aiocb));
        status = EVENT_STATUS_NONE;
    }
    DiskEvent_t(DiskConnectionPtr c, IOBufferDataPtr &d,
                DiskEventOp_t o) {
        op = o;
        conn = c;
        data = d;
        memset(&aio_cb, 0, sizeof(struct aiocb));
        status = EVENT_STATUS_NONE;
    }
    ~DiskEvent_t() {
        assert ((status == EVENT_CANCELLED) || 
                (status == EVENT_DONE) ||
                (status == EVENT_STATUS_NONE));
        // XXX: What if the event isn't cancelled?
        
    }
    /// Cancel the event if possible.
    /// @param fd The file descriptor associated with this event.
    int Cancel(int fd) {
        if (aio_cancel(fd, &aio_cb) == -1) {
            perror("aio cancel: ");
            status = EVENT_CANCELLED;
            return -1;
        }
        status = EVENT_CANCELLED;
        return 0;
    }
    
    /// Returns the string that describes the type of event.
    const char* ToString();

    /// Type of operation associated with this event.
    DiskEventOp_t	op;
    /// DiskConnection associated with this event.
    DiskConnectionPtr	conn;
    /// The buffer on which I/O is to be done on a read or write.
    IOBufferDataPtr	data;
    /// The aio related information about the event
    struct aiocb	aio_cb;
    /// Status of this event
    EventStatus_t	status;
    /// Status of executing the event: That is, return value from
    /// read/write
    ssize_t		retVal;
};

///
/// \typedef DiskEventPtr
/// DiskEvent_t are encapsulated in a smart pointer, so that when the
/// last reference is released, appropriate cleanup occurs.
/// 
typedef boost::shared_ptr<DiskEvent_t> DiskEventPtr;


#endif // _LIBIO_DISKEVENT_H
