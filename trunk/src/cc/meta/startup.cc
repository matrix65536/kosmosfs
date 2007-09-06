/*!
 * $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/startup.cc#3 $
 *
 * Copyright (C) 2006 Kosmix Corp.
 * Author: Blake Lewis (Kosmix Corp.)
 *
 * This file is part of Kosmos File System (KFS).
 *
 * KFS is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation under version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 * \file startup.cc
 * \brief code for starting up the metadata server
 */
#include "startup.h"
#include "thread.h"
#include "logger.h"
#include "checkpoint.h"
#include "kfstree.h"
#include "request.h"
#include "restore.h"
#include "replay.h"
#include "util.h"
#include "LayoutManager.h"

#include <cassert>

extern "C" {
#include <signal.h>
}

using namespace KFS;

/*!
 * \brief rebuild the metatree at startup
 *
 * If the latest CP file exists, use it to restore the contents
 * of the metatree, otherwise create a fresh tree with just "/"
 * and its associated "." and ".." links.
 *
 * Eventually, we should include an option here to restore from
 * a specified CP file instead of just the latest.
 *
 * After restoring from the checkpoint file, apply any log
 * records that are from after the CP.
 */
static int
setup_initial_tree()
{
	string logfile;
	int status;
	if (file_exists(LASTCP)) {
		Restorer r;
		status = r.rebuild(LASTCP) ? 0 : -EIO;
		gLayoutManager.InitRecoveryStartTime();
	} else {
		status = metatree.new_tree();
	}
	return status;
}

static MetaThread request_processor;	//<! request processing thread

/*!
 * \brief request-processing main loop
 */
static void *
request_consumer(void *dummy)
{
	for (;;) {
		process_request();
	}
	return NULL;
}

/*!
 * \brief call init functions and start threads
 *
 * Before starting any threads, block SIGALRM so that it is caught
 * only by the checkpoint timer thread; since the start_CP code
 * acquires locks, we have to be careful not to call it asynchronously
 * in other thread contexts.  Afterwards, initialize metadata request
 * handlers and start the various helper threads going.
 *
 * XXX Eventually, we will want more options here, for instance,
 * specifying a checkpoint file instead of just using "latest".
 */
void
KFS::kfs_startup(const string &logdir, const string &cpdir)
{
	sigset_t sset;
	sigemptyset(&sset);
	sigaddset(&sset, SIGALRM);
	int status = sigprocmask(SIG_BLOCK, &sset, NULL);
	if (status != 0)
		panic("kfs_startup: sigprocmask", true);
	// get the paths setup before we get going
	logger_setup_paths(logdir);
	checkpointer_setup_paths(cpdir);

	status = setup_initial_tree();
	if (status != 0)
		panic("setup_initial_tree failed", false);
	status = replayer.playlog();
	if (status != 0)
		panic("log replay failed", false);
	ChangeIncarnationNumber(NULL);
	// empty the dumpster dir on startup; if it doesn't exist, create it
	// whatever is in the dumpster needs to be nuked anyway; if we
	// remove all the file entries from that dir, the space for the
	// chunks of the file will get reclaimed: chunkservers will tell us
	// about chunks we don't know and those will nuked due to staleness
	emptyDumpsterDir();
	RegisterCounters();
	initialize_request_handlers();
	request_processor.start(request_consumer, NULL);
	logger_init();
	checkpointer_init();
}
