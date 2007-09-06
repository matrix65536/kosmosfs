//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/ChunkServerFactory.h#4 $
//
// Created 2006/06/06
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
// \file ChunkServerFactory.h
// \brief Create ChunkServer objects whenever a chunk server connects
// to us (namely, the meta server).
// 
//----------------------------------------------------------------------------

#ifndef META_CHUNKSERVERFACTORY_H
#define META_CHUNKSERVERFACTORY_H

#include <list>
using std::list;

#include "libkfsIO/Acceptor.h"
#include "libkfsIO/KfsCallbackObj.h"
#include "kfstypes.h"
#include "ChunkServer.h"

namespace KFS
{
        ///
        /// ChunkServerFactory creates a ChunkServer object whenever
        /// a chunk server connects to us.  The ChunkServer object is
        /// responsible for all the communication with that chunk
        /// server.
        ///
        class ChunkServerFactory : public IAcceptorOwner {
        public:
                ChunkServerFactory() {
                        mAcceptor = NULL;
                }

                virtual ~ChunkServerFactory() {
                        delete mAcceptor;
                }

                /// Start an acceptor to listen on the specified port.
                void StartAcceptor(int port) {
                        mAcceptor = new Acceptor(port, this);
                }

                /// Callback that gets invoked whenever a chunkserver
                /// connects to the acceptor port.  The accepted socket
                /// connection is passed in.
                /// @param[in] conn: The accepted connection
                /// @retval The continuation object that was created as a
                /// result of this call.
                KfsCallbackObj *CreateKfsCallbackObj(NetConnectionPtr &conn) {
                        ChunkServerPtr cs(new ChunkServer(conn));
                        mChunkServers.push_back(cs);
                        return cs.get();
                }
		void RemoveServer(const ChunkServer *target);
        private:
                // The socket object which is setup to accept connections from
                /// chunkserver. 
                Acceptor *mAcceptor;
                // List of connected chunk servers.
                list<ChunkServerPtr> mChunkServers;
        };
}

#endif // META_CHUNKSERVERFACTORY_H
