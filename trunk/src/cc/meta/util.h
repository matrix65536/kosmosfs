/*!
 * $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/util.h#3 $
 *
 * \file util.h
 * \brief miscellaneous metadata server code
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
#if !defined(KFS_UTIL_H)
#define KFS_UTIL_H

#include <string>
#include <deque>
#include <sstream>
#include "kfstypes.h"
#include "libkfsIO/IOBuffer.h"

using std::string;
using std::deque;
using std::ostringstream;

namespace KFS {

extern chunkOff_t chunkStartOffset(chunkOff_t offset);
extern int link_latest(const string real, const string alias);
extern const string toString(long long n);
extern long long toNumber(const string s);
extern string makename(const string dir, const string prefix, int number);
extern void split(deque <string> &component, const string path, char sep);
extern bool file_exists(string s);
extern void warn(const string s, bool use_perror);
extern void panic(const string s, bool use_perror);

extern void sendtime(ostringstream &os, const string &prefix, 
		     struct timeval &t, const string &suffix);

/// Is a message that ends with "\r\n\r\n" available in the
/// buffer.
/// @param[in] iobuf  An IO buffer stream with message
/// received from the chunk server.
/// @param[out] msgLen If a valid message is
/// available, length (in bytes) of the message.
/// @retval true if a message is available; false otherwise
///
extern bool IsMsgAvail(IOBuffer *iobuf, int *msgLen);

}
#endif // !defined(KFS_UTIL_H)
