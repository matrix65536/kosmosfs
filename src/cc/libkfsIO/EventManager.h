//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/EventManager.h#3 $
//
// Created 2006/03/31
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

#ifndef _LIBKFSIO_EVENTMANAGER_H
#define _LIBKFSIO_EVENTMANAGER_H

#include "Event.h"
#include "ITimeout.h"
#include "NetManager.h"

///
/// \file EventManager.h
/// \brief EventManager supports execution of time-based events.
/// Events can be scheduled at milli-second granularity and they are
/// notified whenever the time to execute them arises.  
///

class EventManagerTimeoutImpl;

class EventManager {
public:

    static const int MAX_EVENT_SLOTS = 2000;
    // 10 ms granularity for events
    static const int EVENT_GRANULARITY_MS = 10;

    EventManager();
    ~EventManager();

    ///
    /// Schedule an event for execution.  If the event is periodic, it
    /// will be re-scheduled for execution.
    /// @param[in] event A reference to the event that has to be scheduled.
    /// @param[in] afterMs # of milli-seconds after which the event
    /// should be executed.
    ///
    void	Schedule(EventPtr &event, int afterMs);

    /// Register a timeout handler with the NetManager.  The
    /// NetManager will call the handler whenever a timeout occurs.
    void 	Init();

    /// Whenever a timeout occurs, walk the list of scheduled events
    /// to determine if any need to be signaled.
    void 	Timeout();

private:

    EventManagerTimeoutImpl	*mEventManagerTimeoutImpl;

    /// Events are held in a calendar queue: The calendar queue
    /// consists of a circular array of "slots".  Slots are a
    /// milli-second apart.  At each timeout, the list of events in
    /// the current slot are signaled.
    list<EventPtr>	mSlots[MAX_EVENT_SLOTS];
    
    /// Index into the above array that points to where we are
    /// currently.
    int			mCurrentSlot;

    /// Events for which the time of occurence is after the last slot
    /// (i.e., after 20 seconds).
    list <EventPtr>	mLongtermEvents;
};

///
/// \class EventManagerTimeoutImpl
/// \brief Implements the ITimeout interface (@see ITimeout).
///
class EventManagerTimeoutImpl : public ITimeout {
public:
    /// The owning object of a EventManagerTimeoutImpl is the EventManager.
    EventManagerTimeoutImpl(EventManager *mgr) {
        mEventManager = mgr;
    };
    /// Callback the owning object whenever a timeout occurs.
    virtual void Timeout() {
        mEventManager->Timeout();
    };
private:
    /// Owning object.
    EventManager		*mEventManager;

};

#endif // _LIBKFSIO_EVENTMANAGER_H
