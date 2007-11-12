//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/03/25
// Author: Sriram Rao (Kosmix Corp.) 
//
// Copyright 2006 Kosmix Corp.
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
// 
//----------------------------------------------------------------------------

#ifndef LIBIO_I_TIMEOUT_H
#define LIBIO_I_TIMEOUT_H

extern "C" {
#include <sys/types.h>
#include <sys/time.h>
}

namespace KFS
{

///
/// \file ITimeout.h
/// \brief Define the ITimeout interface.
///
 
/// 
/// \class ITimeout
/// Abstract class that defines a Timeout interface.  Whenever a
/// timeout occurs, the Timeout() method will be invoked.  An optional
/// setting, interval can be specified, which signifies the time
/// interval between successive invocations of Timeout().
/// 
/// NOTE: Timeout interface supports only a pseudo-real-time timers.
/// There is no guarantee that the desired interval will hold between
/// successive invocations of Timeout().
///
class ITimeout {
public:
    ITimeout() {
        mIntervalMs = 0;
        gettimeofday(&mLastCall, NULL);
    }
    virtual ~ITimeout() { }
    
    /// Specify the interval in milli-seconds at which the timeout
    /// should occur. 
    void SetTimeoutInterval(int intervalMs) {
        mIntervalMs = intervalMs;
    }

    int GetTimeElapsed() {
        struct timeval timeNow;

        gettimeofday(&timeNow, NULL);
        return ((timeNow.tv_sec - mLastCall.tv_sec) * 1000 * 1000 +
                (timeNow.tv_usec - mLastCall.tv_usec)) / 1000;

    }

    /// Whenever a timer expires (viz., a call to select returns),
    /// this method gets invoked.  Depending on the time-interval
    /// specified, the timeout is appropriately invoked.
    void TimerExpired() {
        int timeMs;
        struct timeval timeNow;

        if (mIntervalMs == 0) {
            // aperiodic timeout.
            Timeout();
            return;
        }

        // Periodic timeout.
        gettimeofday(&timeNow, NULL);
        timeMs = ((timeNow.tv_sec - mLastCall.tv_sec) * 1000 * 1000 +
                  (timeNow.tv_usec - mLastCall.tv_usec)) / 1000;

        if (timeMs >= mIntervalMs) {
            Timeout();
            // remember the last call time
            mLastCall.tv_sec = timeNow.tv_sec;
            mLastCall.tv_usec = timeNow.tv_usec;
        }
    }
    
    /// This method will be invoked when a timeout occurs.
    virtual void Timeout() = 0;
protected:
    int		mIntervalMs;
private:
    struct timeval 	mLastCall;
};

}

#endif // LIBIO_I_TIMEOUT_H
