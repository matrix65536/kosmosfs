//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/NetConnection.cc#4 $
//
// Created 2006/03/14
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

