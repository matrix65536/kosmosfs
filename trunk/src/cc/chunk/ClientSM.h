//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/ClientSM.h#3 $
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

#ifndef _CLIENTSM_H
#define _CLIENTSM_H

class ClientSM; // forward declaration to get things to build...

#include <deque>
using std::deque;

#include "libkfsIO/Chunk.h"
#include "libkfsIO/KfsCallbackObj.h"
#include "libkfsIO/DiskConnection.h"
#include "libkfsIO/NetConnection.h"
#include "KfsOps.h"


class ClientSM : public KfsCallbackObj {
public:

    ClientSM(NetConnectionPtr &conn);

    ~ClientSM(); 

    //
    // Sequence:
    //  Client connects.
    //   - A new client sm is born
    //   - reads a request out of the connection
    //   - client says READ chunkid
    //   - request handler calls the disk manager to get the size
    //   -- the request handler then runs in a loop:
    //       -- in READ START: schedule a read for 4k; transition to READ DONE
    //       -- in READ DONE: data that was read arrives; 
    //            schedule that data to be sent out and transition back to READ START
    //       
    int HandleRequest(int code, void *data);

    // This is a terminal state handler.  In this state, we wait for
    // all outstanding ops to finish and then destroy this.
    int HandleTerminate(int code, void *data);
    
private:
    NetConnectionPtr	mNetConnection;
    /// Queue of outstanding ops from the client.  We reply to ops in FIFO
    deque<KfsOp *>	mOps;

    /// Given a (possibly) complete op in a buffer, run it.
    /// @retval True if the command was handled (i.e., we have all the
    /// data and we could execute it); false otherwise.
    bool		HandleClientCmd(IOBuffer *iobuf, int cmdLen);

    /// Op has finished execution.  Send a response to the client.
    void		SendResponse(KfsOp *op);
};


#endif // _CLIENTSM_H
