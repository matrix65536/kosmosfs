//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/NetConnection.cc#4 $
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

#include "Globals.h"
#include "NetConnection.h"
#include "Globals.h"

using namespace libkfsio;

void NetConnection::HandleReadEvent()
{
    NetConnectionPtr conn;
    TcpSocket *sock;
    int nread;

    if (mListenOnly) {
        sock = mSock->Accept();
#ifdef DEBUG
        if (sock == NULL) {
            COSMIX_LOG_DEBUG("# of open-fd's: disk=%d, net=%d",
                             globals().ctrOpenDiskFds.GetValue(),
                             globals().ctrOpenNetFds.GetValue());
        }
#endif
	assert(sock != NULL);
        if (sock == NULL) 
            return;
        conn.reset(new NetConnection(sock, NULL));
        mCallbackObj->HandleEvent(EVENT_NEW_CONNECTION, (void *) &conn);
    }
    else {
        if (mInBuffer == NULL) {
            mInBuffer = new IOBuffer();
        }
        nread = mInBuffer->Read(mSock->GetFd());
        if (nread == 0) {
            COSMIX_LOG_DEBUG("Read 0 bytes...connection dropped");
            mCallbackObj->HandleEvent(EVENT_NET_ERROR, NULL);
        } else if (nread > 0) {
            mCallbackObj->HandleEvent(EVENT_NET_READ, (void *) mInBuffer);
        }
    }
}

void NetConnection::HandleWriteEvent()
{
    int nwrote;

    if (!IsWriteReady())
    	return;

    // XXX: Need to pay attention to mNumBytesOut---that is, write out
    // only as much as is asked for.
    nwrote = mOutBuffer->Write(mSock->GetFd());
    if (nwrote == 0) {
        COSMIX_LOG_DEBUG("Wrote 0 bytes...connection dropped");
        mCallbackObj->HandleEvent(EVENT_NET_ERROR, NULL);
    } else if (nwrote > 0) {
        mCallbackObj->HandleEvent(EVENT_NET_WROTE, (void *) mOutBuffer);
    }
}

void NetConnection::HandleErrorEvent()
{
    COSMIX_LOG_DEBUG("Got an error on socket.  Closing connection");
    mSock->Close();
    mCallbackObj->HandleEvent(EVENT_NET_ERROR, NULL);
}

bool NetConnection::IsReadReady()
{
    return true;
}

bool NetConnection::IsWriteReady()
{
    if (mOutBuffer == NULL)
    	return false;

    return (mOutBuffer->BytesConsumable() > 0);
}

bool NetConnection::IsGood()
{
    return mSock->IsGood();
}

