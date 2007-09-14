/*!
 * $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/checkpoint.cc#3 $
 *
 * Copyright 2006 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 *
 * \file checkpoint.cc
 * \brief KFS metadata checkpointing
 *
 * Record the contents of the metadata tree on disk.  Outline of
 * the algorithm:
 *
 * 1) A timer or some other thread submits a checkpoint request;
 *
 * 2) The main processing loop notes that checkpointing is in
 *    progress and places the request on the pending list for
 *    the logger.
 *
 * 3) The logger thread closes the current log file and opens
 *    a new one.  It sends a message to the checkpoint thread
 *    including the final sequence number from the closed log
 *    file.
 *
 * 4) The checkpoint thread iterates through the leaf nodes,
 *    copying the contents of each to the checkpoint file.  On
 *    completion, it closes the file and submits an end-of-checkpoint
 *    request.
 *
 * 5) After the checkpoint is initiated, the main request loop
 *    creates copies of any mutated leaf nodes that are not yet
 *    captured in the checkpoint file.
 *
 * 6) On end of checkpoint, the request handler sends a message
 *    to the checkpointer to clean up the copied leaves; a new
 *    checkpoint cannot begin until this is done.
 */

#include <iostream>
#include <ctime>
#include <csignal>
#include "checkpoint.h"
#include "kfstree.h"
#include "request.h"
#include "logger.h"
#include "util.h"

using namespace KFS;

// default values
string KFS::CPDIR("./kfscp");		//!< directory for CP files
string KFS::LASTCP(CPDIR + "/latest");	//!< most recent CP file (link)

Checkpoint KFS::cp(CPDIR);

/*
 * We have a problem where the logger keeps creating new log
 * files even when no CPs are taken.  To prevent proliferation
 * of log files, rotate the log only when we know a CP will be taken.
*/
bool
Checkpoint::isCPNeeded()
{
	bool status;

	writer.lock();
	// There is a likelihood of a CP being taken
	status = (!running) && (mutations != 0);
	writer.unlock();
	return status;
}

void
Checkpoint::start_CP()
{
	writer.lock();
	if (!running && mutations != 0) {
		if (nostart)
			startblocked = true;
		else {
			running = true;
			mutations = 0;		// reset for next CP
			writer.wakeup();
		}
	}
	writer.unlock();
}

bool
Checkpoint::lock_running()
{
	writer.lock();
	nostart = true;
	writer.unlock();
	return running;
}

void
Checkpoint::unlock_running()
{
	bool startit;
	writer.lock();
	nostart = false;
	startit = startblocked;
	startblocked = false;
	writer.unlock();
	if (startit)
		start_CP();
}

int
Checkpoint::write_leaves()
{
	LeafIter li(metatree.firstLeaf(), 0);
	Node *p = li.parent();
	Meta *m = li.current();
	int status = 0;
	save_active(p);
	while (status == 0 && m != NULL) {
		if (m->skip())
			m->clearskip();
		else
			status = m->checkpoint(file);
		li.next();
		p = li.parent();
		m = (p == NULL) ? NULL : li.current();
		if (p != activeNode)
			save_active(p);
	}
	save_active(NULL);
	return status;
}

int
Checkpoint::write_zombies()
{
	int status = 0;
	while (!zombie.empty()) {
		Meta *m = zombie.dequeue();
		int s = m->checkpoint(file);
		if (status == 0)
			status = s;
		delete m;
	}
	return status;
}

void
Checkpoint::save_active(Node *n)
{
	writer.lock();
	if (activeNode != NULL)
		writer.wakeup();
	activeNode = n;
	writer.unlock();
}

void
Checkpoint::wait_if_active(Node *n)
{
	writer.lock();
	while (activeNode == n)
		writer.sleep();
	writer.unlock();
}

/*
 * At system startup, take a CP if the file that corresponds to the
 * latest CP doesn't exist.
*/
void
Checkpoint::initial_CP()
{
	seq_t highest = oplog.checkpointed();
	cpname = cpfile(highest);
	if (file_exists(cpname))
		return;
	MetaCheckpoint cpreq;
	submit_request(&cpreq);
	(void) oplog.wait_for_cp();
}

int
Checkpoint::do_CP()
{
	writer.lock();
	while (!running)	// wait till someone starts us
		writer.sleep();
	writer.unlock();

	seq_t highest = oplog.checkpointed();
	cpname = cpfile(highest);
	file.open(cpname.c_str());
	int status = file.fail() ? -EIO : 0;
	if (status == 0) {
		file << "checkpoint/" << highest << '\n';
		file << "version/" << VERSION << '\n';
		file << "fid/" << fileID.getseed() << '\n';
		file << "chunkId/" << chunkID.getseed() << '\n';
		file << "chunkVersionInc/" << chunkVersionInc << '\n';
		time_t t = time(NULL);
		file << "time/" << ctime(&t);
		file << "log/" << oplog.name() << '\n' << '\n';
		status = write_leaves();
		if (status == 0)
		       status = write_zombies();
		file.close();
		link_latest(cpname, LASTCP);
	}
	writer.lock();
	running = false;
	++cpcount;
	writer.unlock();
	return status;
}

void *
cp_main(void *dummy)
{
	for (;;) {
		if (cp.do_CP() != 0)
			std::cerr << "checkpoint " + cp.name() + " failed\n";
	}

	return NULL;
}

void *
cptimer(void *dummy)
{
	int status, sig;
	sigset_t sset;
	sigemptyset(&sset);
	sigaddset(&sset, SIGALRM);

	alarm(CPMAXSEC);
	for (;;) {
		status = sigwait(&sset, &sig);
		if (status == EINTR)	// happens under gdb for some reason
			continue;
		assert(status == 0 && sig == SIGALRM);
		alarm(CPMAXSEC);
		MetaCheckpoint cpreq;
		if (!cp.isCPNeeded())
			continue;
		submit_request(&cpreq);
		(void) oplog.wait_for_cp();
	}

	return NULL;
}

void
KFS::checkpointer_setup_paths(const string &cpdir)
{
	if (cpdir != "") {
		CPDIR = cpdir;
		LASTCP = cpdir + "/latest";
		cp.setCPDir(cpdir);
	}
}

void
KFS::checkpointer_init()
{
	cp.start_writer(cp_main);

	// start a CP on restart.
	cp.initial_CP();

	// use a timer to keep CP's going
	cp.start_timer(cptimer);
}
