/*!
 * $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/restore.h#3 $
 *
 * \file restore.h
 * \brief rebuild metatree from saved checkpoint
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
#if !defined(KFS_RESTORE_H)
#define KFS_RESTORE_H

#include <fstream>
#include <string>
#include <deque>
#include <map>
#include "util.h"

using std::ifstream;
using std::string;
using std::deque;
using std::map;

namespace KFS {

/*!
 * \brief state for restoring from a checkpoint file
 */
class Restorer {
	ifstream file;			//!< the CP file
public:
	bool rebuild(string cpname);	//!< process the CP file
};

extern bool restore_chunkVersionInc(deque <string> &c);

}
#endif // !defined(KFS_RESTORE_H)
