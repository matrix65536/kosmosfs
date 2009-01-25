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

#ifndef _LIBIO_NETCONNECTION_H
#define _LIBIO_NETCONNECTION_H

#include "KfsCallbackObj.h"
#include "Event.h"
#include "IOBuffer.h"
#include "TcpSocket.h"

#include "common/log.h"

#include <boost/shared_ptr.hpp>

namespace KFS
{
///
/// \file NetConnection.h
/// \brief A network connection uses TCP sockets for doing I/O.
/// 
/// A network connection contains a socket and data in buffers.
/// Whenever data is read from the socket it is held in the "in"
/// buffer; whenever data needs to be written out on the socket, that
/// data should be dropped into the "out" buffer and it will
/// eventually get sent out.
/// 

///
/// \class NetConnection
/// A net connection contains an underlying socket and is associated
/// with a KfsCallbackObj.  Whenever I/O is done on the socket (either
/// for read or write) or when an error occurs (such as the remote
/// peer closing the connection), the associated KfsCallbackObj is
/// called back with an event notification.
/// 
class NetConnection {
public:
    /// @param[in] sock TcpSocket on which I/O can be done
    /// @param[in] c KfsCallbackObj associated with this connection
    NetConnection(TcpSocket *sock, KfsCallbackObj *c) : 
        mListenOnly(false), mEnableReadIfOverloaded(false), 
        mDoingNonblockingConnect(false),
        mCallbackObj(c), 
        mSock(sock), mInBuffer(NULL), mOutBuffer(NULL), 
        mNumBytesOut(0), mLastFlushResult(0), mInactivityTimeoutSecs(-1) { 
        mPollVectorIndex = -1;
        mLastActivityTime = time(0);
    } 

    /// @param[in] sock TcpSocket on which I/O can be done
    /// @param[in] c KfsCallbackObj associated with this connection
    /// @param[in] listenOnly boolean that specifies whether this
    /// connection is setup only for accepting new connections.
    NetConnection(TcpSocket *sock, KfsCallbackObj *c, bool listenOnly) :
        mListenOnly(listenOnly),  
        mDoingNonblockingConnect(false),
        mCallbackObj(c), mSock(sock), 
        mInBuffer(NULL), mOutBuffer(NULL), 
        mNumBytesOut(0), mLastFlushResult(0), mInactivityTimeoutSecs(-1)
    {
        mPollVectorIndex = -1;
        if (listenOnly)
            mEnableReadIfOverloaded = true;
        mLastActivityTime = time(0);
    }

    ~NetConnection() {
        delete mSock;
        delete mOutBuffer;
        delete mInBuffer;
    }

    void SetDoingNonblockingConnect() {
        mDoingNonblockingConnect = true;
    }
    void SetOwningKfsCallbackObj(KfsCallbackObj *c) {
        mCallbackObj = c;
    }

    void EnableReadIfOverloaded() {
        mEnableReadIfOverloaded = true;
    }

    /// If there is no activity on this socket for nsecs, then notify
    /// the owning object; maybe time to close the connection
    void SetInactivityTimeout(int nsecs) {
        mInactivityTimeoutSecs = nsecs;
    }

    int GetFd() { return mSock->GetFd(); }

    std::string GetPeerName() { return mSock->GetPeerName(); }

    /// Callback for handling a read.  That is, select() thinks that
    /// data is available for reading. So, do something.  If system is
    /// overloaded and we don't have a special pass, leave the data in
    /// the buffer alone.
    void HandleReadEvent(bool isSystemOverloaded);

    /// Callback for handling a writing.  That is, select() thinks that
    /// data can be sent out.  So, do something.
    void HandleWriteEvent();

    /// Callback for handling errors.  That is, select() thinks that
    /// an error occurred.  So, do something.
    void HandleErrorEvent();

    /// Do we expect data to be read in?  If the system is overloaded,
    /// check the connection poll state to determine the return value.
    bool IsReadReady(bool isSystemOverloaded);
    /// Is data available for writing?
    bool IsWriteReady();

    /// # of bytes available for writing
    int GetNumBytesToWrite();

    /// Is the connection still good?
    bool IsGood();

    /// Enqueue data to be sent out.
    void Write(IOBufferDataPtr &ioBufData) {
        mOutBuffer->Append(ioBufData);
        mNumBytesOut += ioBufData->BytesConsumable();
        mLastActivityTime = time(0);
    }

    void Write(IOBuffer *ioBuf, int numBytes) {
        mOutBuffer->Append(ioBuf);
        mNumBytesOut += numBytes;
    }
    
    /// Enqueue data to be sent out.
    void Write(const char *data, int numBytes) {
	if (mOutBuffer == NULL) {
		mOutBuffer = new IOBuffer();
	}
        mOutBuffer->CopyIn(data, numBytes);
        mNumBytesOut += numBytes;
    }

    void StartFlush() {
        if ((mLastFlushResult < 0) || (mDoingNonblockingConnect))
            return;
        // if there is any data to be sent out, start the send
        if (mOutBuffer && mOutBuffer->BytesConsumable() > 0)
            mLastFlushResult = mOutBuffer->Write(mSock->GetFd());
    }
    /// Close the connection.
    void Close() {
        // KFS_LOG_DEBUG("Closing socket: %d", mSock->GetFd());
        mSock->Close();
    }
    /// index into the poll fd's vector
    int			mPollVectorIndex;
private:
    bool		mListenOnly;
    /// should we add this connection to the poll vector for reads
    /// even when the system is overloaded? 
    bool		mEnableReadIfOverloaded;

    /// Set if the connect was done in non-blocking manner.  In this
    /// case, the first callback for "write ready" will mean that we
    /// connect has finished.
    bool		mDoingNonblockingConnect;

    /// KfsCallbackObj that will be notified whenever "events" occur.
    KfsCallbackObj	*mCallbackObj;
    /// Socket on which I/O will be done.
    TcpSocket		*mSock;
    /// Buffer that contains data read from the socket
    IOBuffer		*mInBuffer;
    /// Buffer that contains data that should be sent out on the socket.
    IOBuffer		*mOutBuffer;

    /// When was the last activity on this connection
    time_t		mLastActivityTime;
    /// # of bytes from the out buffer that should be sent out.
    int			mNumBytesOut;
    int			mLastFlushResult;
    int			mInactivityTimeoutSecs;
};

typedef boost::shared_ptr<NetConnection> NetConnectionPtr;

}
#endif // LIBIO_NETCONNECTION_H
