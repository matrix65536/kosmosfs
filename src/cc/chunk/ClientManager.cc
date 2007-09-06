//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/ClientManager.cc#2 $
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

#include "ClientManager.h"

void 
ClientManager::StartAcceptor(int port)
{
    mAcceptor = new Acceptor(port, this);
}

void
ClientManager::Remove(ClientSM *clnt)
{
    list<ClientSM *>::iterator iter = find(mClients.begin(), mClients.end(), clnt);

    assert(iter != mClients.end());
    if (iter == mClients.end())
        return;
    mClients.erase(iter);
}
