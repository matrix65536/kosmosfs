//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/06/01
// Author: Sriram Rao (Kosmix Corp.)
//
// Copyright 2006 Cosmix Corporation.  All rights reserved.
// Cosmix PROPRIETARY and CONFIDENTIAL.
//
// \file NetDispatch.cc
//
// \brief Meta-server network dispatcher.
//
//----------------------------------------------------------------------------

#include "NetDispatch.h"
#include "logger.h"
#include "LayoutManager.h"
#include "libkfsIO/Globals.h"
using namespace libkfsio;

using namespace KFS;

NetDispatch::NetDispatch()
{
        mClientManager = new ClientManager();
        mChunkServerFactory = new ChunkServerFactory();
        mNetDispatchTimeoutImpl = new NetDispatchTimeoutImpl(this);
}

NetDispatch::~NetDispatch()
{
        globals().netManager.UnRegisterTimeoutHandler(mNetDispatchTimeoutImpl);

        delete mNetDispatchTimeoutImpl;
        delete mClientManager;
        delete mChunkServerFactory;
}

//
// Since we can't take a pointer to a member function easily, this is
// a workaround.  When the net-dispatch thread starts, it calls this
// function which in turn calls the real thing.
//
void *
net_dispatch_main(void *dummy)
{
        (void) dummy; // shut up g++
        globals().netManager.MainLoop();
        return NULL;
}

//
// Open up the server for connections.
//
void
NetDispatch::Start(int clientAcceptPort, int chunkServerAcceptPort)
{
        // Start the acceptors so that it sets up a connection with the net
        // manager for listening. 
        mClientManager->StartAcceptor(clientAcceptPort);
        mChunkServerFactory->StartAcceptor(chunkServerAcceptPort);
        // Setup the handler for polling the logger
        globals().netManager.RegisterTimeoutHandler(mNetDispatchTimeoutImpl);
        // Start polling....
        mWorker.start(net_dispatch_main, NULL);
}

///
/// Poll the logger to see if any op's have finished execution.  For
/// such ops, send a response back to the client.  Also, if there any
/// layout related RPCs, dispatch them now.
///
void
NetDispatch::Dispatch()
{
        MetaRequest *r;

        while ((r = oplog.next_result_nowait()) != NULL) {
                // The Client will send out a response and destroy r.
		if (r->clnt)
			r->clnt->HandleEvent(EVENT_CMD_DONE, (void *) r);
                else if (r->op == META_ALLOCATE) {
			// For truncations, we may need to internally
			// generate an allocation request.  In such a
			// case, run the allocation thru the entire gamut
			// (dispatch the request to chunkserver and log)
			// and then resume processing of the truncation.
			MetaAllocate *alloc = static_cast<MetaAllocate *> (r);
			assert(alloc->req != NULL);

			r = alloc->req;
			assert(r->op == META_TRUNCATE);
			r->suspended = false;
			submit_request(r);
			delete alloc;
                }
		else if (r->op == META_CHANGE_CHUNKVERSIONINC) {
			MetaChangeChunkVersionInc *ccvi = 
				static_cast<MetaChangeChunkVersionInc *> (r);
			if (ccvi->req != NULL) {
				// req->op was waiting for the chunkVersionInc change to
				// make it to disk.  Now that is done, we can push out req->op.
				r = ccvi->req;
				r->suspended = false;
				assert((r->op == META_ALLOCATE) && (r->clnt != NULL));
				r->clnt->HandleEvent(EVENT_CMD_DONE, (void *) r);
			}
			delete ccvi;
		}
		else if (r->op == META_CHUNK_REPLICATE) {
			// For replicating a chunk, we sent a request to
			// a chunkserver; it did the work and sent back a reply.
			// We have processed the reply and that message is now here.
			// Nothing more to do.  So, get rid of it
			delete r;
		}
		else {
			// somehow, occasionally we are getting checkpoint requests here...
			COSMIX_LOG_DEBUG("Getting an op (%d) with no client",
					r->op);
		}
        }

        gLayoutManager.Dispatch();
}

