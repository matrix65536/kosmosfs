//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/Acceptor.h#4 $
//
// Created 2006/03/22
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

#ifndef _LIBIO_ACCEPTOR_H
#define _LIBIO_ACCEPTOR_H

#include "KfsCallbackObj.h"
#include "NetConnection.h"

///
/// \file Acceptor.h
/// \brief Mechanism for accepting TCP connections.
///
/// Accepting a TCP connection consists of two pieces:
///
///   1. Setting up a TCP socket and marking it for listen: This is
/// handled by the Acceptor.
///
///   2.  Once a connection is received, "doing something" with it:
/// This is handled by the IAcceptorOwner.
///

///
/// \class IAcceptorOwner
/// Abstract class defines the interface that must be implemented by an
/// owner of an acceptor object.
///

class IAcceptorOwner {
public:
    virtual ~IAcceptorOwner() { };

    ///
    /// Callback that will be invoked whenever a new connection is
    /// received.  The callback is expected to create a continuation
    /// and return that as the result.
    /// @param conn A smart pointer that encapsulates the connection
    /// that was received.  @see NetConnectionPtr
    /// 
    virtual KfsCallbackObj *CreateKfsCallbackObj(NetConnectionPtr &conn) = 0;
};

///
/// \class Acceptor
/// A continuation for receiving connections on a TCP port.  Calls
/// back the associated IAcceptorOwner whenever a connection is received.
///
class Acceptor : public KfsCallbackObj {
public:

    ///
    /// @param port Port number used to listen for incoming
    /// connections.
    /// @param owner The IAcceptorOwner object that "owns" this Acceptor.
    ///
    Acceptor(int port, IAcceptorOwner *owner);
    ~Acceptor();

    ///
    /// Event handler to handle incoming connections.  @see KfsCallbackObj
    /// @param code Unused argument
    /// @param data NetConnectionPtr object that encapsulates the
    /// accepted connection.
    /// @result Returns 0.
    ///
    int RecvConnection(int code, void *data);

private:
    ///
    /// The encapsulated connection object that corresponds to the TCP
    /// port on which the Acceptor is listening for connections.
    /// 
    NetConnectionPtr	mConn;
    IAcceptorOwner	*mAcceptorOwner;
};


#endif // _LIBIO_ACCEPTOR_H
