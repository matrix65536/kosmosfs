//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/Event.h#4 $
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

#ifndef _LIBKFSIO_EVENT_H
#define _LIBKFSIO_EVENT_H

#include "KfsCallbackObj.h"
#include <boost/shared_ptr.hpp>

///
/// \enum EventCode_t
/// Various event codes that a KfsCallbackObj is notified with when
/// events occur.
///
enum EventCode_t {
    EVENT_NEW_CONNECTION,
    EVENT_NET_READ,
    EVENT_NET_WROTE,
    EVENT_NET_ERROR,
    EVENT_DISK_READ,
    EVENT_DISK_WROTE,
    EVENT_DISK_ERROR,
    EVENT_SYNC_DONE,
    EVENT_CMD_DONE,
    EVENT_TIMEOUT
};

///
/// \enum EventStatus_t
/// \brief Code corresponding to the status of an event:
/// scheduled/done/cancelled. 
///
enum EventStatus_t {
    EVENT_STATUS_NONE,
    EVENT_SCHEDULED,
    EVENT_DONE,
    EVENT_CANCELLED
};

class Event {
public:
    Event (KfsCallbackObj *callbackObj, void *data, int timeoutMs, bool periodic) {
        mCallbackObj = callbackObj;
        mEventData = data;
        mEventStatus = EVENT_STATUS_NONE;
        mTimeoutMs = timeoutMs;
        mPeriodic = periodic;
        mLongtermWait = 0;
    };

    ~Event() {
        assert(mEventStatus != EVENT_SCHEDULED);
        Cancel();
        mEventData = NULL;
    }

    void SetStatus(EventStatus_t status) {
        mEventStatus = status;
    }

    int EventOccurred() {
        if (mEventStatus == EVENT_CANCELLED)
            return 0;
        mEventStatus = EVENT_DONE;
        return mCallbackObj->HandleEvent(EVENT_TIMEOUT, mEventData);
    }

    void Cancel() {
        mEventStatus = EVENT_CANCELLED;
    }
    
    bool IsPeriodic() {
        return mPeriodic;
    }

    int GetTimeout() {
        return mTimeoutMs;
    }

    void SetLongtermWait(int waitMs) {
        mLongtermWait = waitMs;
    }
    
    int DecLongtermWait(int numMs) {
        mLongtermWait -= numMs;
        if (mLongtermWait < 0)
            mLongtermWait = 0;
        return mLongtermWait;
    }

private:
    EventStatus_t	mEventStatus;
    KfsCallbackObj	*mCallbackObj;
    void		*mEventData;
    int			mTimeoutMs;
    bool		mPeriodic;
    int			mLongtermWait;
};

typedef boost::shared_ptr<Event> EventPtr;

#endif // _LIBKFSIO_EVENT_H
