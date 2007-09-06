//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/ClientManager.h#3 $
//
// Created 2006/03/28
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

#ifndef _CLIENTMANAGER_H
#define _CLIENTMANAGER_H

#include "libkfsIO/Acceptor.h"
#include "ClientSM.h"

class ClientManager : public IAcceptorOwner {
public:
    ClientManager() {
        mAcceptor = NULL;
    };
    virtual ~ClientManager() {
        delete mAcceptor;
    };
    void StartAcceptor(int port);
    KfsCallbackObj *CreateKfsCallbackObj(NetConnectionPtr &conn) {
        ClientSM *clnt = new ClientSM(conn);
        mClients.push_back(clnt);
        return clnt;
    }
    void Remove(ClientSM *clnt);
private:
    list<ClientSM *> mClients;
    Acceptor	*mAcceptor;
};

extern ClientManager gClientManager;

#endif // _CLIENTMANAGER_H
