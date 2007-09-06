//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/NetDispatch.h#3 $
//
// Created 2006/06/01
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
// \file NetDispatch.h
// \brief Meta-server network dispatcher
// 
//----------------------------------------------------------------------------

#ifndef META_NETDISPATCH_H
#define META_NETDISPATCH_H

#include "libkfsIO/NetManager.h"
#include "libkfsIO/Acceptor.h"
#include "ChunkServerFactory.h"
#include "ClientManager.h"
#include "thread.h"

namespace KFS
{
    class NetDispatchTimeoutImpl;
    
    class NetDispatch {
    public:
        NetDispatch();
        ~NetDispatch();
        void Start(int clientAcceptPort, int chunkServerAcceptPort);
        //!< Dispatch the results of RPC requests that have finished execution.
        //!< Also, dispatch layout related RPCs to chunk servers.
        void Dispatch();
        ChunkServerFactory *GetChunkServerFactory() {
            return mChunkServerFactory;
        }

    private:
        //!< Timer that periodically checks to see if
        //!< requests have completed execution/layout RPCs need to be
        //!< dispatched.
        NetDispatchTimeoutImpl *mNetDispatchTimeoutImpl;
        ClientManager *mClientManager; //!< tracks the connected clients
        ChunkServerFactory *mChunkServerFactory; //!< creates chunk servers when they connect
        MetaThread mWorker; //!< runs the poll loop in the net manager
    };

    class NetDispatchTimeoutImpl : public ITimeout {
    public:
        NetDispatchTimeoutImpl(NetDispatch *dis) {
            mNetDispatch = dis;
            // poll the logger/layout-mgr for RPCs every 100ms
            SetTimeoutInterval(100);
        };
        ~NetDispatchTimeoutImpl() {
            mNetDispatch = NULL;
	};
        // On a timeout call the network dispatcher to see if any
        // RPC requests/replies are ready to be sent out.
        void Timeout() {
            mNetDispatch->Dispatch();
        };
    private:
        NetDispatch *mNetDispatch; //!< pointer to the owner (dispatch)
    };

    extern NetDispatch gNetDispatch;


}

#endif // META_NETDISPATCH_H
