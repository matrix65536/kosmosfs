/*!
 * $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/replay.h#3 $
 *
 * \file replay.h
 * \brief log replay definitions
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
#if !defined(KFS_REPLAY_H)
#define KFS_REPLAY_H

#include <string>
#include <fstream>

using std::string;
using std::ifstream;

namespace KFS {

class Replay {
	ifstream file;		//!< the log file being replayed
	string path;		//!< path name for log file
	int number;		//!< sequence number for log file
public:
	Replay(): number(-1) { };
	~Replay() { };
	void openlog(const string &p);	//!< open the log file for replay
	int playlog();			//!< read and apply its contents
	int logno() { return number; }
};

extern Replay replayer;

}
#endif // !defined(KFS_REPLAY_H)
