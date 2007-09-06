//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/EventManager.cc#3 $
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

#include "EventManager.h"
#include "Globals.h"
using namespace libkfsio;

EventManager::EventManager()
{
    mCurrentSlot = 0;
    mEventManagerTimeoutImpl = new EventManagerTimeoutImpl(this);
}

EventManager::~EventManager()
{
    for (int i = 0; i < MAX_EVENT_SLOTS; i++)
        mSlots[i].clear();

    mLongtermEvents.clear();
}

void
EventManager::Init()
{
    // The event manager schedules events at 10ms granularity.
    mEventManagerTimeoutImpl->SetTimeoutInterval(EVENT_GRANULARITY_MS);
    globals().netManager.RegisterTimeoutHandler(mEventManagerTimeoutImpl);
}

void EventManager::Schedule(EventPtr &event, int afterMs)
{
    int slot;

    assert(afterMs >= 0);

    event->SetStatus(EVENT_SCHEDULED);

    if (afterMs <= 0)
        slot = (mCurrentSlot + 1) % MAX_EVENT_SLOTS;
    else
        slot = afterMs / EVENT_GRANULARITY_MS;

    if (slot > MAX_EVENT_SLOTS) {
        event->SetLongtermWait(afterMs);
        mLongtermEvents.push_back(event);
        return;
    }
    slot = (MAX_EVENT_SLOTS + slot - mCurrentSlot) % MAX_EVENT_SLOTS;
    mSlots[slot].push_back(event);

}

void EventManager::Timeout()
{
    list <EventPtr>::iterator iter, eltToRemove;
    EventPtr event;
    int waitMs;
    int msElapsed = mEventManagerTimeoutImpl->GetTimeElapsed();

    for (iter = mSlots[mCurrentSlot].begin(); 
         iter != mSlots[mCurrentSlot].end(); ++iter) {
        event = *iter;
        event->EventOccurred();
        // Reschedule it if it is a periodic event
        if (event->IsPeriodic())
            Schedule(event, event->GetTimeout());
        
    }
    mSlots[mCurrentSlot].clear();
    mCurrentSlot++;
    if (mCurrentSlot == MAX_EVENT_SLOTS)
        mCurrentSlot = 0;

    if ((mLongtermEvents.size() > 0) &&
        (msElapsed - EVENT_GRANULARITY_MS >= 3 * EVENT_GRANULARITY_MS)) {
        COSMIX_LOG_DEBUG("Elapsed ms = %d", msElapsed);
    }
    // Now, pull all the long-term events
    iter = mLongtermEvents.begin();
    while (iter != mLongtermEvents.end()) {
        event = *iter;
        // count down for each ms that ticks by
        waitMs = event->DecLongtermWait(msElapsed);
        if (waitMs >= MAX_EVENT_SLOTS) {
            ++iter;
            continue;
        }
        // we have counted down to "short-term" amount
        Schedule(event, waitMs);
        eltToRemove = iter;
        ++iter;
        mLongtermEvents.erase(eltToRemove);
    }

}
