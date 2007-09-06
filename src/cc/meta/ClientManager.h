//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/ClientManager.h#4 $
//
// Created 2006/06/02
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
// \file ClientManager.h
// \brief Create client state machines whenever clients connect to meta server.
// 
//----------------------------------------------------------------------------

#ifndef META_CLIENTMANAGER_H
#define META_CLIENTMANAGER_H

#include "libkfsIO/Acceptor.h"
#include "libkfsIO/KfsCallbackObj.h"
#include "ClientSM.h"

namespace KFS
{

    class ClientManager : public IAcceptorOwner {
    public:
        ClientManager() {
            mAcceptor = NULL;
        };
        virtual ~ClientManager() {
            delete mAcceptor;
        };
        void StartAcceptor(int port) {
            mAcceptor = new Acceptor(port, this);
        };
        KfsCallbackObj *CreateKfsCallbackObj(NetConnectionPtr &conn) {
            // XXX: Should we keep a list of all client state machines?
            return new ClientSM(conn);
        }
    private:
        // The socket object which is setup to accept connections.
        Acceptor *mAcceptor;
    };


}

#endif // META_CLIENTMANAGER_H
