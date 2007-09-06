/*!
 * $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/logger.h#3 $
 *
 * \file logger.h
 * \brief metadata logger
 * \author Blake Lewis (Kosmix Corp.)
 *
 * Copyright (C) 2006 Kosmix Corp.
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
 */
#if !defined(KFS_LOGGER_H)
#define KFS_LOGGER_H

#include <fstream>
#include <sstream>
#include <string>

#include "kfstypes.h"
#include "queue.h"
#include "thread.h"
#include "request.h"
#include "util.h"

using std::string;
using std::ofstream;

namespace KFS {

/*!
 * \brief Class for logging metadata updates
 */
class Logger {
	string logdir;		//!< directory where logs are kept
	int lognum;		//!< for generating log file names
	string logname;		//!< name of current log file
	ofstream file;		//!< the current log file
	seq_t nextseq;		//!< next request sequence no.
	seq_t committed;	//!< highest request known to be on disk
	seq_t incp;		//!< highest request in a checkpoint
	MetaQueue <MetaRequest> pending; //!< list of still-unlogged results
	MetaQueue <MetaRequest> logged;	//!< list of logged results
	MetaQueue <MetaRequest> cpdone; //!< completed CP
	MetaThread thread;	//!< thread synchronization
	string genfile(int n)	//!< generate a log file name
	{
		std::ostringstream f(std::ostringstream::out);
		f << n; 
		return logdir + "/log." + f.str();
	}
	void flushLog();
	void flushResult(MetaRequest *r);
public:
	static const int VERSION = 1;
	Logger(string d): logdir(d), lognum(-1), nextseq(0), committed(0) { }
	~Logger() { file.close(); }
	void setLogDir(const string &d)
	{
		logdir = d;
	}

	string logfile(int n)	//!< generate a log file name
	{
		return makename(logdir, "log", n);
	}
	/*!
	 * \brief check whether request is stored on disk
	 * \param[in] r the request of interest
	 * \return	whether it is on disk
	 */
	bool iscommitted(MetaRequest *r)
	{
		return r->seqno != 0 && r->seqno <= committed;
	}
	int log(MetaRequest *r) { return r->log(file); } //!< log a request
	void add_pending(MetaRequest *r) { pending.enqueue(r); }
	/*!
	 * \brief get a pending request and assign it a sequence number
	 * \return the request
	 */
	MetaRequest *get_pending()
	{
		MetaRequest *r = pending.dequeue();
		r->seqno = ++nextseq;
		return r;
	}
	MetaRequest *next_result();
	MetaRequest *next_result_nowait();
	seq_t checkpointed() { return incp; }	//!< highest seqno in CP
	void add_logged(MetaRequest *r) { logged.enqueue(r); }
	void save_cp(MetaRequest *r) { cpdone.enqueue(r); }
	MetaRequest *wait_for_cp() { return cpdone.dequeue(); }
	void start(MetaThread::thread_start_t func)	//!< start thread
	{
		thread.start(func, NULL);
	}
	int startLog(int seqno);	//!< start a new log file
	int finishLog();		//!< tie off log file before CP
	const string name() const { return logname; }	//!< name of log file
	/*!
	 * \brief set initial sequence numbers at startup
	 * \param[in] last last sequence number from checkpoint or log
	 */
	void set_seqno(seq_t last)
	{
		incp = committed = nextseq = last;
	}
};

extern Logger oplog;
extern void logger_setup_paths(const string &logdir);
extern void logger_init();
extern MetaRequest *next_result();

}
#endif // !defined(KFS_LOGGER_H)
