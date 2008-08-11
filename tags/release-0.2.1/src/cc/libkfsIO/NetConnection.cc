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

#include "Globals.h"
#include "NetConnection.h"
#include "Globals.h"

using namespace KFS;
using namespace KFS::libkfsio;

void NetConnection::HandleReadEvent(bool isSystemOverloaded)
{
    NetConnectionPtr conn;
    TcpSocket *sock;
    int nread;

    if (mListenOnly) {
        sock = mSock->Accept();
#ifdef DEBUG
        if (sock == NULL) {
            KFS_LOG_VA_DEBUG("# of open-fd's: disk=%d, net=%d",
                             globals().ctrOpenDiskFds.GetValue(),
                             globals().ctrOpenNetFds.GetValue());
        }
#endif
        if (sock == NULL) 
            return;
        conn.reset(new NetConnection(sock, NULL));
        mCallbackObj->HandleEvent(EVENT_NEW_CONNECTION, (void *) &conn);
    }
    else {
        if (isSystemOverloaded && (!mEnableReadIfOverloaded))
            return;

        if (mInBuffer == NULL) {
            mInBuffer = new IOBuffer();
        }
        // XXX: Need to control how much we read...
        nread = mInBuffer->Read(mSock->GetFd());
        if (nread == 0) {
            KFS_LOG_DEBUG("Read 0 bytes...connection dropped");
            mCallbackObj->HandleEvent(EVENT_NET_ERROR, NULL);
        } else if (nread > 0) {
            mCallbackObj->HandleEvent(EVENT_NET_READ, (void *) mInBuffer);
        }
    }
}

void NetConnection::HandleWriteEvent()
{
    int nwrote;

    // if non-blocking connect was set, then the first callback
    // signaling write-ready means that connect() has succeeded.
    mDoingNonblockingConnect = false;

    // clear the value so we can let flushes thru when possible
    mLastFlushResult = 0;

    if (!IsWriteReady())
    	return;

    // XXX: Need to pay attention to mNumBytesOut---that is, write out
    // only as much as is asked for.
    nwrote = mOutBuffer->Write(mSock->GetFd());
    if (nwrote == 0) {
        KFS_LOG_DEBUG("Wrote 0 bytes...connection dropped");
        mCallbackObj->HandleEvent(EVENT_NET_ERROR, NULL);
    } else if (nwrote > 0) {
        mCallbackObj->HandleEvent(EVENT_NET_WROTE, (void *) mOutBuffer);
    }
}

void NetConnection::HandleErrorEvent()
{
    KFS_LOG_DEBUG("Got an error on socket.  Closing connection");
    mSock->Close();
    mCallbackObj->HandleEvent(EVENT_NET_ERROR, NULL);
}

bool NetConnection::IsReadReady(bool isSystemOverloaded)
{
    if (!isSystemOverloaded)
        return true;
    // under load, this instance variable controls poll state
    return mEnableReadIfOverloaded;
}

bool NetConnection::IsWriteReady()
{
    if (mDoingNonblockingConnect)
        return true;

    if (mOutBuffer == NULL)
    	return false;

    return (mOutBuffer->BytesConsumable() > 0);
}

int NetConnection::GetNumBytesToWrite()
{
    // force addition to the poll vector
    if (mDoingNonblockingConnect)
        return 1;

    if (mOutBuffer == NULL)
    	return 0;

    return mOutBuffer->BytesConsumable();
}


bool NetConnection::IsGood()
{
    return mSock->IsGood();
}

