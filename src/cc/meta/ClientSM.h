//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/ClientSM.h#4 $
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
// \file ClientSM.h
// \brief Object for handling interactions with a KFS client.
// 
//----------------------------------------------------------------------------

#ifndef META_CLIENTSM_H
#define META_CLIENTSM_H

#include "request.h"
#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/NetConnection.h"

namespace KFS
{

    class ClientSM : public KfsCallbackObj {
    public:

        ClientSM(NetConnectionPtr &conn);

        ~ClientSM(); 

        //
        // Sequence:
        //  Client connects.
        //   - A new client sm is born
        //   - reads a request out of the connection
        //   - submit the request for execution
        //   - when the request is done, send a response back.
        //
        int HandleRequest(int code, void *data);
        int HandleTerminate(int code, void *data);

    private:
        /// A handle to a network connection
        NetConnectionPtr	mNetConnection;

        /// The op (if any) that is currently being executed
        MetaRequest		*mOp;

        /// Given a (possibly) complete op in a buffer, run it.
        void		HandleClientCmd(IOBuffer *iobuf, int cmdLen);

        /// Op has finished execution.  Send a response to the client.
        void		SendResponse(MetaRequest *op);
    };

}

#endif // META_CLIENTSM_H
