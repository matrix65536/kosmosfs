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

#ifndef _LIBIO_NETMANAGER_H
#define _LIBIO_NETMANAGER_H

#include <sys/time.h>

#include <list>
using std::list;

#include "ITimeout.h"
#include "NetConnection.h"

///
/// \file NetManager.h
/// The net manager provides facilities for multiplexing I/O on network
/// connections.  It keeps a list of connections on which it has to
/// call select.  Whenever an "event" occurs on a connection (viz.,
/// read/write/error), it calls back the connection to handle the
/// event.  
/// 
/// The net manager also provides support for timeout notification.
/// Whenever a call to select returns, that is an occurence of a
/// timeout.  Interested handlers can register with the net manager to
/// be notified of timeout.  In the current implementation, the
/// timeout interval is mSelectTimeout.
//

typedef list<NetConnectionPtr> NetConnectionList_t;
typedef list<NetConnectionPtr>::iterator NetConnectionListIter_t;

class NetManager {
public:
    NetManager();
    NetManager(const struct timeval &selectTimeout);
    ~NetManager();
    /// Add a connection to the net manager's list of connections that
    /// are used for building poll vector.
    /// @param[in] conn The connection that should be added.
    void AddConnection(NetConnectionPtr &conn);
    void RegisterTimeoutHandler(ITimeout *handler);
    void UnRegisterTimeoutHandler(ITimeout *handler);
    
    ///
    /// This function never returns.  It builds a poll vector, calls
    /// select(), and then evaluates the result of select():  for
    /// connections on which data is I/O is possible---either for
    /// reading or writing are called back.  In the callback, the
    /// connections should take appropriate action.  
    ///
    /// NOTE: When a connection is closed (such as, via a call to
    /// NetConnection::Close()), then it automatically falls out of
    /// the net manager's list of connections that are polled.
    ///  
    void MainLoop();
    
private:

    /// List of connections that are used for building the poll vector.
    NetConnectionList_t	mConnections;
    /// timeout interval specified in the call to select().
    struct timeval 	mSelectTimeout;
    /// Handlers that are notified whenever a call to select()
    /// returns.  To the handlers, the notification is a timeout signal.
    list<ITimeout *>	mTimeoutHandlers;
};

#endif // _LIBIO_NETMANAGER_H
