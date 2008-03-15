/*!
 * $Id$ 
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
 * \file logger.cc
 * \brief thread for logging metadata updates
 * \author Blake Lewis (Kosmix Corp.)
 */

#include "logger.h"
#include "queue.h"
#include "checkpoint.h"
#include "util.h"
#include "replay.h"
#include "common/log.h"

using namespace KFS;

// default values
string LOGDIR("./kfslog");
string LASTLOG(LOGDIR + "/last");

Logger KFS::oplog(LOGDIR);

/*!
 * \brief flush log entries to disk
 *
 * Make sure that all of the log entries are on disk and
 * update the highest sequence number logged.
 */
void
Logger::flushLog()
{
	thread.lock();
	seq_t last = nextseq;
	thread.unlock();

	file.flush();
	if (file.fail())
		panic("Logger::flushLog", true);

	thread.lock();
	committed = last;
	thread.unlock();
}

/*!
 * \brief open a new log file for writing
 * \param[in] seqno	the next log sequence number (lognum)
 * \return		0 if successful, negative on I/O error
 */
int
Logger::startLog(int seqno)
{
	assert(seqno >= 0);
	lognum = seqno;
	logname = logfile(lognum);
	if (file_exists(logname)) {
		// following log replay, until the next CP, we
		// should continue to append to the logfile that we replayed.
		// seqno will be set to the value we got from the chkpt file.
		// So, don't overwrite the log file.
		KFS_LOG_VA_DEBUG("Opening %s in append mode", logname.c_str());
		file.open(logname.c_str(), std::ios_base::app);
		return (file.fail()) ? -EIO : 0;
	}
	file.open(logname.c_str());
	file << "version/" << VERSION << '\n';

	// for debugging, record when the log was opened
	time_t t = time(NULL);

	file << "time/" << ctime(&t);
	return (file.fail()) ? -EIO : 0;
}

/*!
 * \brief close current log file and begin a new one
 */
int
Logger::finishLog()
{
	thread.lock();
	// for debugging, record when the log was closed
	time_t t = time(NULL);

	file << "time/" << ctime(&t);
	file.close();
	link_latest(logname, LASTLOG);
	if (file.fail())
		warn("link_latest", true);
	incp = committed;
	int status = startLog(lognum + 1);
	thread.unlock();
	cp.start_CP();
	return status;
}

/*!
 * \brief make sure result is on disk
 * \param[in] r	the result of interest
 *
 * If this result has a higher sequence number than what is
 * currently known to be on disk, flush the log to disk.
 */
void
Logger::flushResult(MetaRequest *r)
{
	if (r->seqno > committed) {
		flushLog();
		assert(r->seqno <= committed);
	}
}

/*!
 * \brief return next available result
 * \return result that was at the head of the result queue
 *
 * Return the next available result, making sure that it is
 * committed to disk
 */
MetaRequest *
Logger::next_result()
{
	MetaRequest *r = logged.dequeue();
	flushResult(r);
	return r;
}

/*!
 * \brief return next result, non-blocking
 *
 * Same as Logger::next_result() except that it returns NULL
 * if the result queue is empty.
 */
MetaRequest *
Logger::next_result_nowait()
{
	MetaRequest *r = logged.dequeue_nowait();
	if (r != NULL)
		flushResult(r);
	return r;
}

/*!
 * \brief logger main loop
 *
 * Pull requests from the pending queue and call the appropriate
 * log routine to write them into the log file.  Note any mutations
 * in the checkpoint structure (so that it will realize that there
 * are changes to record), and finally, move the request onto the
 * result queue for return to the clients.
 */
void *
logger_main(void *dummy)
{
	for (;;) {
		MetaRequest *r = oplog.get_pending();
		bool is_cp = (r->op == META_CHECKPOINT);
		if (r->mutation && r->status == 0) {
			oplog.log(r);
			if (!is_cp)
				cp.note_mutation();
		}
		if (is_cp) {
			oplog.save_cp(r);
		} else
			oplog.add_logged(r);
	}
	return NULL;
}

void
KFS::logger_setup_paths(const string &logdir)
{
	if (logdir != "") {
		LOGDIR = logdir;
		LASTLOG = LOGDIR + "/last";
		oplog.setLogDir(LOGDIR);
	}
}

void
KFS::logger_init()
{
	if (oplog.startLog(replayer.logno()) < 0)
		panic("KFS::logger_init, startLog", true);
	oplog.start(logger_main);
}
