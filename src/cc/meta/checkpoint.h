/*
 * $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/checkpoint.h#3 $
 *
 * \file checkpoint.h
 * \brief KFS checkpointer
 * \author Blake Lewis (Kosmix Corp.)
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
 */
#if !defined(KFS_CHECKPOINT_H)
#define KFS_CHECKPOINT_H

#include <fstream>
#include <sstream>
#include <string>

#include "thread.h"
#include "kfstypes.h"
#include "queue.h"
#include "meta.h"
#include "kfstree.h"
#include "util.h"

using std::string;
using std::ofstream;

namespace KFS {

/*!
 * \brief keeps track of checkpoint status
 *
 * This class records the current state of the metadata server
 * with respect to checkpoints---the name of the checkpoint file,
 * whether the CP is running, whether the server has any recent
 * updates that would make a new CP worthwhile, etc.
 *
 * It contains two threads.  One is just a timer that wakes up
 * periodically and schedules a CP if needed.  The other is a
 * writer that iterates through the leaf nodes recording their
 * contents in the checkpoint file.
 *
 * Updates to the metatree can go on currently with checkpointing.
 * If such an update deletes a leaf node, we must preserve its
 * contents until it is recorded in the checkpoint.  Such nodes
 * are placed on the "zombie" queue; they are written out and
 * deleted at the end of the checkpoint.  The code also takes
 * care not to include any modifications that occur after the
 * checkpoint has begun.
 */
class Checkpoint {
	string cpdir;		//!< dir for CP files
	string cpname;		//!< name of CP file
	ofstream file;		//!< current CP file
	MetaThread writer;	//!< leaf writing thread
	MetaThread timer;	//!< timer thread to start CP
	MetaQueue <Meta> zombie; //!< deleted but needed in current CP
	bool running;		//!< CP currently in progress?
	bool nostart;		//!< don't allow CP to start
	bool startblocked;	//!< wanted to start but blocked
	int mutations;		//!< changes since last CP
	int cpcount;		//!< number of CP's since startup
	Node *activeNode;	//!< level-1 node currently being written
	void save_active(Node *n); //!< save new activeNode
	string cpfile(seq_t highest)	//!< generate the next file name
	{
		return makename(cpdir, "chkpt", highest);
	}
	int write_leaves();
	int write_zombies();
public:
	static const int VERSION = 1;
	Checkpoint(string d): cpdir(d), running(false), nostart(false),
			      startblocked(false), cpcount(0) { }
	~Checkpoint() { }
	void setCPDir(const string &d) 
	{
		cpdir = d;
	}
	const string name() const { return cpname; }
	bool isCPNeeded(); 	//!< return true if a CP will be taken
	void initial_CP();	//!< schedule a checkpoint on startup if needed
	void start_CP();	//!< schedule a checkpoint
	int do_CP();		//!< do the actual work
	bool lock_running();	//!< prevent new CP from starting
	void unlock_running();
	bool visited(Node *n) { return n->cpbit() == (cpcount & 1); }
	void note_mutation() { ++mutations; }
	void wait_if_active(Node *n);
	void start_writer(MetaThread::thread_start_t func)
	{
		writer.start(func, NULL);
	}
	void start_timer(MetaThread::thread_start_t func)
	{
		timer.start(func, NULL);
	}
	void zombify(Meta *m) { zombie.enqueue(m); }
};

extern string CPDIR;		//!< directory for CP files
extern string LASTCP;		//!< most recent CP file (link)
const unsigned int CPMAXSEC = 60;	//!< max. seconds between CP's

extern Checkpoint cp;
extern void checkpointer_setup_paths(const string &cpdir);
extern void checkpointer_init();

}

#endif // !defined(KFS_CHECKPOINT_H)
