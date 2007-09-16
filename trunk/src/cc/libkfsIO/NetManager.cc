//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/03/14
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

#include <sys/select.h>

#include "NetManager.h"
#include "TcpSocket.h"
#include "Globals.h"
using namespace libkfsio;

#include "common/log.h"

using std::mem_fun;

NetManager::NetManager()
{
    mSelectTimeout.tv_sec = 0;
    mSelectTimeout.tv_usec = 100;
}

NetManager::NetManager(const struct timeval &selectTimeout)
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
            mTimeoutHandlers.erase(iter);
            return;
        }
    }
}

void
NetManager::MainLoop()
{
    fd_set readSet, writeSet, errSet;
    int maxFd, fd, res;
    NetConnectionPtr conn;
    NetConnectionListIter_t iter, eltToRemove;
    struct timeval selectTimeout;

    while (1) {

        maxFd = 0;
        FD_ZERO(&readSet);
        FD_ZERO(&writeSet);
        FD_ZERO(&errSet);
        // build poll vector: 

        for (iter = mConnections.begin(); iter != mConnections.end(); ++iter) {
            conn = *iter;
            fd = conn->GetFd();

            assert(fd > 0);
            if (fd < 0)
                continue;

            if (fd > maxFd)
                maxFd = fd;

            if (conn->IsReadReady()) {
                // By default, each connection is read ready.  We
                // expect there to be 2-way data transmission, and so
                // we are read ready.  If we do any throttling, then
                // read ready will fail.
                FD_SET(fd, &readSet);
            }

            if (conn->IsWriteReady()) {
                // An optimization: if we are not sending any data for
                // this fd in this round of poll, don't bother adding
                // it to the poll vector.
                FD_SET(fd, &writeSet);
            }

            FD_SET(fd, &errSet);
        }

        selectTimeout = mSelectTimeout;
        res = select(maxFd + 1, &readSet, &writeSet, &errSet, 
                     &selectTimeout);

        if (res < 0) {
            perror("select(): ");
            continue;
        }
        
        // list of timeout handlers...call them back
        for_each (mTimeoutHandlers.begin(), mTimeoutHandlers.end(), 
                  mem_fun(&ITimeout::TimerExpired));

        iter = mConnections.begin();
        while (iter != mConnections.end()) {
            conn = *iter;
            fd = conn->GetFd();
            if ((fd > 0) && (FD_ISSET(fd, &readSet))) {
                conn->HandleReadEvent();
                FD_CLR(fd, &readSet);
            }
            // conn could have closed due to errors during read.  so,
            // need to re-get the fd and check that all is good
            fd = conn->GetFd();
            if ((fd > 0) && (FD_ISSET(fd, &writeSet))) {
                conn->HandleWriteEvent();
                FD_CLR(fd, &writeSet);
            }
            fd = conn->GetFd();
            if ((fd > 0) && (FD_ISSET(fd, &errSet))) {
                conn->HandleErrorEvent();
                FD_CLR(fd, &errSet);
            }
            // Something happened and the connection has closed.  So,
            // remove the connection from our list.
            if (conn->GetFd() < 0) {
                COSMIX_LOG_DEBUG("Removing fd from poll list");
                eltToRemove = iter;
                ++iter;
                mConnections.erase(eltToRemove);
            } else {
                ++iter;
            }
        }


    }
}
