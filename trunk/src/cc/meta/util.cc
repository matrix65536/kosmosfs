/*!
 * $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/util.cc#3 $
 *
 * \file util.cc
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

extern "C" {
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
}

#include <iostream>
#include <sstream>
#include <cstdlib>
#include <cerrno>
#include "util.h"

using namespace KFS;

/*!
 * \brief Find the chunk that contains the specified file offset.
 * \param[in] offset	offset in the file for which we need the chunk
 * \return		offset in the file that corresponds to the
 * start of the chunk.
 */
chunkOff_t 
KFS::chunkStartOffset(chunkOff_t offset)
{
	return (offset / CHUNKSIZE) * CHUNKSIZE;
}

/*!
 * \brief link to the latest version of a file
 *
 * \param[in] realname	name of the target file
 * \param[in] alias	name to link to target
 * \return		error code from link command
 *
 * Make a hard link with the name "alias" to the
 * given file "realname"; we use this to make it
 * easy to find the latest log and checkpoint files.
 */
int
KFS::link_latest(const string realname, const string alias)
{
	int status = 0;
	(void) unlink(alias.c_str());	// remove any previous link
	if (link(realname.c_str(), alias.c_str()) != 0)
		status = -errno;
	return status;
}

/*!
 * \brief convert a number to a string
 * \param[in] n	the number
 * \return	the string
 */
const string
KFS::toString(long long n)
{
	std::ostringstream s(std::ostringstream::out);

	s << n;
	return s.str();
}

/*!
 * \brief convert a string to a number
 * \param[in] s	the string
 * \return	the number
 */
long long
KFS::toNumber(string s)
{
	char *endptr;
	long long n = strtoll(s.c_str(), &endptr, 10);
	if (*endptr != '\0')
		n = -1;
	return n;
}

/*!
 * \brief paste together a pathname from its constituent parts
 * \param[in] dir	directory path
 * \param[in] prefix	beginning part of file name
 * \param[in] number	numeric suffix
 * \return		string "<dir>/<prefix>.<number>"
 */
string
KFS::makename(const string dir, const string prefix, int number)
{
	return dir + "/" + prefix + "." + toString(number);
}

/*!
 * \brief split a path name into its component parts
 * \param[out]	component	the list of components
 * \param[in]	path		the path name
 * \param[in]	sep		the component separator (e.g., '/')
 */
void
KFS::split(deque <string> &component, const string path, char sep)
{
	string::size_type start = 0;
	string::size_type slash = 0;

	while (slash != string::npos) {
		assert(slash == 0 || path[slash] == sep);
		slash = path.find(sep, start);
		string nextc = path.substr(start, slash - start);
		start = slash + 1;
		component.push_back(nextc);
	}
}

/*!
 * \brief check whether a file exists
 * \param[in]	name	path name of the file
 * \return		true if stat says it is a plain file
 */
bool
KFS::file_exists(string name)
{
	struct stat s;
	if (stat(name.c_str(), &s) == -1)
		return false;

	return S_ISREG(s.st_mode);
}

///
/// Return true if there is a sequence of "\r\n\r\n".
/// @param[in] iobuf: Buffer with data 
/// @param[out] msgLen: string length of the command in the buffer
/// @retval true if a command is present; false otherwise.
///
bool
KFS::IsMsgAvail(IOBuffer *iobuf,
                int *msgLen)
{
        char buf[MAX_RPC_HEADER_LEN];
        int nAvail, len = 0, i;

        nAvail = iobuf->BytesConsumable();
        if (nAvail > MAX_RPC_HEADER_LEN)
                nAvail = MAX_RPC_HEADER_LEN;

        len = iobuf->CopyOut(buf, nAvail);

        // Find the first occurence of "\r\n\r\n"
        for (i = 3; i < len; ++i) {
                if ((buf[i - 3] == '\r') &&
                    (buf[i - 2] == '\n') &&
                    (buf[i - 1] == '\r') &&
                    (buf[i] == '\n')) {
                        // The command we got is from 0..i.  The strlen of the
                        // command is i+1.
                        *msgLen = i + 1;
                        return true;
                }
        }
        return false;
}

/*!
 * A helper function to print out a timeval into a string buffer with
 * a prefix/suffix string around the time values.
 */
void
KFS::sendtime(ostringstream &os, const string &prefix, 
	      struct timeval &t, 
	      const string &suffix)
{
	os << prefix << " " << t.tv_sec << " " << t.tv_usec << suffix;
}

/*!
 * \brief print warning message on syscall failure
 * \param[in] msg	message text
 * \param[in] use_perror pass text to perror() if true
 */
void
KFS::warn(const string msg, bool use_perror)
{
	if (use_perror)
		perror(msg.c_str());
	else
		std::cerr << msg << '\n';
}

/*!
 * \brief bomb out on "impossible" error
 * \param[in] msg	panic text
 * \param[in] use_perror pass text to perror() if true
 */
void
KFS::panic(const string msg, bool use_perror)
{
	warn(msg, use_perror);
	abort();
}
