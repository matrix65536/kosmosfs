//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/Acceptor.cc#4 $
//
// Created 2006/03/23
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

#include "Acceptor.h"
#include "NetManager.h"
#include "Globals.h"
using namespace libkfsio;

///
/// Create a TCP socket, bind it to the port, and listen for incoming connections.
///
Acceptor::Acceptor(int port, IAcceptorOwner *owner)
{
    TcpSocket *sock;

    sock = new TcpSocket();

    mAcceptorOwner = owner;
    sock->Listen(port);
    mConn.reset(new NetConnection(sock, this, true));
    SET_HANDLER(this, &Acceptor::RecvConnection);
    globals().netManager.AddConnection(mConn);
}

Acceptor::~Acceptor()
{
    mConn->Close();
    mConn.reset();
}

///
/// Event handler that gets called back whenever a new connection is
/// received.  In response, the AcceptorOwner object is first notified of
/// the new connection and then, the new connection is added to the
/// list of connections owned by the NetManager. @see NetManager 
///
int
Acceptor::RecvConnection(int code, void *data)
{
    NetConnectionPtr conn = *(NetConnectionPtr *) data;
    KfsCallbackObj *callbackObj;

    // Shut-up g++
    (void) code;

    callbackObj = mAcceptorOwner->CreateKfsCallbackObj(conn);
    conn->SetOwningKfsCallbackObj(callbackObj);

    ///
    /// Add the connection to the net manager's list of "polling"
    /// fd's. 
    ///
    globals().netManager.AddConnection(conn);

    return 0;
}
