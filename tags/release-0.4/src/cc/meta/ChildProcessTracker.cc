
//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/04/30
// Author: Sriram Rao
//
// Copyright 2009 Quantcast Corp.
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
// \file ChildProcessTracker.cc
// \brief Handler for tracking child process that are forked off, retrieve
// their exit status.
//
//----------------------------------------------------------------------------

#include "request.h"
#include "logger.h"
#include "ChildProcessTracker.h"
#include "libkfsIO/Globals.h"
#include "common/log.h"
using namespace KFS;
using namespace KFS::libkfsio;
using std::pair;
using std::list;

ChildProcessTrackingTimer KFS::gChildProcessTracker;

void KFS::ChildProcessTrackerInit()
{
	globals().netManager.RegisterTimeoutHandler(&gChildProcessTracker);
}

void ChildProcessTrackingTimer::Track(pid_t pid, MetaRequest *r)
{
	mPending.push_back(pair<pid_t, MetaRequest *>(pid, r));
}

void ChildProcessTrackingTimer::Timeout()
{
	for (list<pair<pid_t, MetaRequest *> >::iterator curr = mPending.begin();
		curr != mPending.end(); ) {

		int status;
		MetaRequest *req = curr->second;
		pid_t pid = waitpid(curr->first, &status, WNOHANG);
		if ((pid == curr->first) && WIFEXITED(status))  {
			req->status = WEXITSTATUS(status);
			req->suspended = false;

			KFS_LOG_VA_INFO("Child pid: %d exited with status: %d",
					pid, req->status);

			oplog.dispatch(req);
			
			list<pair<pid_t, MetaRequest * > >::iterator toErase = curr;
			curr++;
			mPending.erase(toErase);
                }
		curr++;
	}
}
