//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/NetConnection.h#4 $
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

#ifndef _LIBIO_NETCONNECTION_H
#define _LIBIO_NETCONNECTION_H

#include "KfsCallbackObj.h"
#include "Event.h"
#include "IOBuffer.h"
#include "TcpSocket.h"

#include "common/log.h"

#include <boost/shared_ptr.hpp>

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
    NetConnection(TcpSocket *sock, KfsCallbackObj *c) {
	mSock = sock;
	mCallbackObj = c;
	mListenOnly = false;
	mInBuffer = mOutBuffer = NULL;
        mNumBytesOut = 0;
    }

    /// @param[in] sock TcpSocket on which I/O can be done
    /// @param[in] c KfsCallbackObj associated with this connection
    /// @param[in] listenOnly boolean that specifies whether this
    /// connection is setup only for accepting new connections.
    NetConnection(TcpSocket *sock, KfsCallbackObj *c, bool listenOnly) {
        mSock = sock;
        mCallbackObj = c;
        mListenOnly = listenOnly;
        mInBuffer = NULL;
        mOutBuffer = NULL;
        mNumBytesOut = 0;
    }

    ~NetConnection() {
        delete mSock;
        delete mOutBuffer;
        delete mInBuffer;
    }

    void SetOwningKfsCallbackObj(KfsCallbackObj *c) {
        mCallbackObj = c;
    }

    int GetFd() { return mSock->GetFd(); }

    /// Callback for handling a read.  That is, select() thinks that
    /// data is available for reading. So, do something.
    void HandleReadEvent();

    /// Callback for handling a writing.  That is, select() thinks that
    /// data can be sent out.  So, do something.
    void HandleWriteEvent();

    /// Callback for handling errors.  That is, select() thinks that
    /// an error occurred.  So, do something.
    void HandleErrorEvent();

    /// Do we expect data to be read in?
    bool IsReadReady();
    /// Is data available for writing?
    bool IsWriteReady();

    /// Is the connection still good?
    bool IsGood();

    /// Enqueue data to be sent out.
    void Write(IOBufferDataPtr &ioBufData) {
        mOutBuffer->Append(ioBufData);
        mNumBytesOut += ioBufData->BytesConsumable();
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

    /// Close the connection.
    void Close() {
        COSMIX_LOG_DEBUG("Closing socket: %d", mSock->GetFd());
        mSock->Close();
    }
    

private:
    bool		mListenOnly;
    /// KfsCallbackObj that will be notified whenever "events" occur.
    KfsCallbackObj	*mCallbackObj;
    /// Socket on which I/O will be done.
    TcpSocket		*mSock;
    /// Buffer that contains data read from the socket
    IOBuffer		*mInBuffer;
    /// Buffer that contains data that should be sent out on the socket.
    IOBuffer		*mOutBuffer;
    /// # of bytes from the out buffer that should be sent out.
    int			mNumBytesOut;
};

typedef boost::shared_ptr<NetConnection> NetConnectionPtr;

#endif // LIBIO_NETCONNECTION_H
