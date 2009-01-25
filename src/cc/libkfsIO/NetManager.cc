//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/03/14
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
// 
//----------------------------------------------------------------------------

#include <sys/poll.h>
#include <cerrno>
#include <boost/scoped_array.hpp>

#include "NetManager.h"
#include "TcpSocket.h"
#include "Globals.h"

#include "common/log.h"

using std::mem_fun;
using std::list;

using namespace KFS;
using namespace KFS::libkfsio;

NetManager::NetManager() : mDiskOverloaded(false), mNetworkOverloaded(false), mMaxOutgoingBacklog(0)
{
    pthread_mutexattr_t mutexAttr;
    int rval;

    rval = pthread_mutexattr_init(&mutexAttr);
    assert(rval == 0);
    rval = pthread_mutexattr_settype(&mutexAttr, PTHREAD_MUTEX_RECURSIVE);
    assert(rval == 0);

    mSelectTimeout.tv_sec = 10;
    mSelectTimeout.tv_usec = 0;
}

NetManager::NetManager(const struct timeval &selectTimeout) : 
    mDiskOverloaded(false), mNetworkOverloaded(false), mMaxOutgoingBacklog(0)
{
    mSelectTimeout.tv_sec = selectTimeout.tv_sec;
    mSelectTimeout.tv_usec = selectTimeout.tv_usec;
}

NetManager::~NetManager()
{
    NetConnectionListIter_t iter;
    NetConnectionPtr conn;
    
    mTimeoutHandlers.clear();
    mConnections.clear();
}

void
NetManager::AddConnection(NetConnectionPtr &conn)
{
    mConnections.push_back(conn);
}

void
NetManager::RegisterTimeoutHandler(ITimeout *handler)
{
    mTimeoutHandlers.push_back(handler);
}

void
NetManager::UnRegisterTimeoutHandler(ITimeout *handler)
{
    list<ITimeout *>::iterator iter;
    ITimeout *tm;
    
    for (iter = mTimeoutHandlers.begin(); iter != mTimeoutHandlers.end(); 
         ++iter) {
        tm = *iter;
        if (tm == handler) {
            mUnregisteredTimeoutHandlers.push_back(tm);
            return;
        }
    }
}

void
NetManager::RemoveUnregisteredTimeoutHandlers()
{
    list<ITimeout *>::iterator iter1, iter2;
    ITimeout *i1, *i2;

    for (iter1 = mUnregisteredTimeoutHandlers.begin(); iter1 != mUnregisteredTimeoutHandlers.end(); ++iter1) {
        for (iter2 = mTimeoutHandlers.begin(); iter2 != mTimeoutHandlers.end(); ++iter2) {
            i1 = *iter1;
            i2 = *iter2;
            if (i1 == i2) {
                mTimeoutHandlers.erase(iter2);
                break;
            }
        }
    }
    mUnregisteredTimeoutHandlers.clear();
}



void
NetManager::MainLoop()
{
    boost::scoped_array<struct pollfd> pollfds;
    uint32_t pollFdSize = 1024;
    int numPollFds, fd, res;
    NetConnectionPtr conn;
    NetConnectionListIter_t iter, eltToRemove;
    int pollTimeoutMs;

    // if we have too many bytes to send, throttle incoming
    int64_t totalNumBytesToSend = 0;
    bool overloaded = false;

    pollfds.reset(new struct pollfd[pollFdSize]);
    while (1) {

        if (mConnections.size() > pollFdSize) {
            pollFdSize = mConnections.size();
            pollfds.reset(new struct pollfd[pollFdSize]);
        }
        // build poll vector: 

        // make sure we are listening to the net kicker
        fd = globals().netKicker.GetFd();
        pollfds[0].fd = fd;
        pollfds[0].events = POLLIN;
        pollfds[0].revents = 0;

        numPollFds = 1;

        overloaded = IsOverloaded(totalNumBytesToSend);
        int numBytesToSend;

        totalNumBytesToSend = 0;

        for (iter = mConnections.begin(); iter != mConnections.end(); ++iter) {
            conn = *iter;
            fd = conn->GetFd();

            if (fd < 0) {
                // we'll get rid of this connection in the while loop below
                conn->mPollVectorIndex = -2;
                continue;
            }

            if (fd == globals().netKicker.GetFd()) {
                conn->mPollVectorIndex = -1;
                continue;
            }

            conn->mPollVectorIndex = numPollFds;
            pollfds[numPollFds].fd = fd;
            pollfds[numPollFds].events = 0;
            pollfds[numPollFds].revents = 0;
            if (conn->IsReadReady(overloaded)) {
                // By default, each connection is read ready.  We
                // expect there to be 2-way data transmission, and so
                // we are read ready.  In overloaded state, we only
                // add the fd to the poll vector if the fd is given a
                // special pass
                pollfds[numPollFds].events |= POLLIN;
            }

            numBytesToSend = conn->GetNumBytesToWrite();
            if (numBytesToSend > 0) {
                totalNumBytesToSend += numBytesToSend;
                // An optimization: if we are not sending any data for
                // this fd in this round of poll, don't bother adding
                // it to the poll vector.
                pollfds[numPollFds].events |= POLLOUT;
            }
            numPollFds++;
        }

        if (!overloaded) {
            overloaded = IsOverloaded(totalNumBytesToSend);
            if (overloaded)
                continue;
        }

        struct timeval startTime, endTime;

        gettimeofday(&startTime, NULL);

        pollTimeoutMs = mSelectTimeout.tv_sec * 1000;
        res = poll(pollfds.get(), numPollFds, pollTimeoutMs);

        if ((res < 0) && (errno != EINTR)) {
            perror("poll(): ");
            continue;
        }

        gettimeofday(&endTime, NULL);

        // list of timeout handlers...call them back

        fd = globals().netKicker.GetFd();
        if (pollfds[0].revents & POLLIN) {
            globals().netKicker.Drain();
            globals().diskManager.ReapCompletedIOs();
        }

        RemoveUnregisteredTimeoutHandlers();
        //
        // This call can cause a handler to unregister itself.  Doing
        // that while we are iterating thru this list is fatal.
        // Hence, when a handler unregisters itself, we put that
        // handler into a separate list.  After we called all the
        // handlers, we cleanout the unregistered handlers.
        //

        for_each (mTimeoutHandlers.begin(), mTimeoutHandlers.end(), 
                  mem_fun(&ITimeout::TimerExpired));

        RemoveUnregisteredTimeoutHandlers();

        iter = mConnections.begin();
        while (iter != mConnections.end()) {
            conn = *iter;
            // Something happened and the connection has closed.  So,
            // remove the connection from our list.
            if (conn->GetFd() < 0) {
                eltToRemove = iter;
                ++iter;
                mConnections.erase(eltToRemove);
                continue;
            }

            if ((conn->GetFd() == globals().netKicker.GetFd()) ||
                (conn->mPollVectorIndex < 0)) {
                ++iter;
                continue;
            }

            if (pollfds[conn->mPollVectorIndex].revents & POLLIN) {
                fd = conn->GetFd();
                if (fd > 0) {
                    conn->HandleReadEvent(overloaded);
                }
            }
            // conn could have closed due to errors during read.  so,
            // need to re-get the fd and check that all is good
            if (pollfds[conn->mPollVectorIndex].revents & POLLOUT) {
                fd = conn->GetFd();
                if (fd > 0) {
                    conn->HandleWriteEvent();
                }
            }

            if ((pollfds[conn->mPollVectorIndex].revents & POLLERR) ||
                (pollfds[conn->mPollVectorIndex].revents & POLLHUP)) {
                fd = conn->GetFd();
                if (fd > 0) {
                    conn->HandleErrorEvent();
                }
            }
            ++iter;
        }

    }
}

bool
NetManager::IsOverloaded(int64_t numBytesToSend)
{
    static bool wasOverloaded = false;

    if (mMaxOutgoingBacklog > 0) {
        if (!mNetworkOverloaded) {
            mNetworkOverloaded = (numBytesToSend > mMaxOutgoingBacklog);
        } else if (numBytesToSend <= mMaxOutgoingBacklog / 2) {
            // network was overloaded and that has now cleared
            mNetworkOverloaded = false;
        }
    }

    bool isOverloaded = mDiskOverloaded || mNetworkOverloaded;

    if (!wasOverloaded && isOverloaded) {
        KFS_LOG_VA_INFO("System is now in overloaded state (%ld bytes to send; %d disk IO's) ",
                        numBytesToSend, globals().diskManager.NumDiskIOOutstanding());
    } else if (wasOverloaded && !isOverloaded) {
        KFS_LOG_VA_INFO("Clearing system overload state (%ld bytes to send; %d disk IO's)",
                        numBytesToSend, globals().diskManager.NumDiskIOOutstanding());
    }
    wasOverloaded = isOverloaded;
    return isOverloaded;
}

void
NetManager::ChangeDiskOverloadState(bool v)
{
    if (mDiskOverloaded == v)
        return;
    mDiskOverloaded = v;
}
