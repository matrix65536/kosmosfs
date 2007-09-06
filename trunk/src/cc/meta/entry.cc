/*
 * $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/entry.cc#3 $
 *
 * \file entry.cc
 * \brief parse checkpoint and log entries
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

#include "entry.h"
#include "util.h"

using namespace KFS;

bool
DiskEntry::parse(char *line)
{
	const string l(line);
	deque <string> component;

	if (l.empty())
		return true;

	split(component, l, SEPARATOR);
	parsetab::iterator c = table.find(component[0]);
	return (c != table.end() && c->second(component));
}

/*!
 * \brief remove a file name from the front of the deque
 * \param[out]	name	the returned name
 * \param[in]	tag	the keyword that precedes the name
 * \param[in]	c	the deque of components from the entry
 * \param[in]	ok	if false, do nothing and return false
 * \return		true if parse was successful
 *
 * The ok parameter short-circuits parsing if an error occurs.
 * This lets us do a series of steps without checking until the
 * end.
 */
bool
KFS::pop_name(string &name, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
		return false;

	c.pop_front();
	name = c.front();
	c.pop_front();
	if (!name.empty())
		return true;

	/*
	 * Special hack: the initial entry for "/" shows up
	 * as two empty components ("///"); I should probably
	 * come up with a more elegant way to do this.
	 */
	if (c.empty() || !c.front().empty())
		return false;

	c.pop_front();
	name = "/";
	return true;
}

/*!
 * \brief remove a path name from the front of the deque
 * \param[out]	path	the returned path
 * \param[in]	tag	the keyword that precedes the path
 * \param[in]	c	the deque of components from the entry
 * \param[in]	ok	if false, do nothing and return false
 * \return		true if parse was successful
 *
 * The ok parameter short-circuits parsing if an error occurs.
 * This lets us do a series of steps without checking until the
 * end.
 */
bool
KFS::pop_path(string &path, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
		return false;

	c.pop_front();
	/* Collect everything else in path with components separated by '/' */
	path = "";
	while (1) {
		path += c.front();
		c.pop_front();
		if (c.empty())
			break;
		path += "/";
	}
	return true;
}

/*!
 * \brief remove a file ID from the component deque
 */
bool
KFS::pop_fid(fid_t &fid, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
		return false;

	c.pop_front();
	fid = toNumber(c.front());
	c.pop_front();
	return (fid != -1);
}

/*!
 * \brief remove a size_t value from the component deque
 */
bool
KFS::pop_size(size_t &sz, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
		return false;

	c.pop_front();
	sz = toNumber(c.front());
	c.pop_front();
	return (sz != -1u);
}

/*!
 * \brief remove a short value from the component deque
 */
bool
KFS::pop_short(int16_t &num, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
		return false;

	c.pop_front();
	num = (int16_t) toNumber(c.front());
	c.pop_front();
	return (num != (int16_t) -1);
}

/*!
 * \brief remove a off_t value from the component deque
 */
bool
KFS::pop_offset(off_t &o, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
		return false;

	c.pop_front();
	o = toNumber(c.front());
	c.pop_front();
	return (o != -1);
}

/*!
 * \brief remove a file type from the component deque
 */
bool
KFS::pop_type(FileType &t, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
		return false;

	c.pop_front();
	string type = c.front();
	c.pop_front();
	if (type == "file") {
		t = KFS_FILE;
	} else if (type == "dir") {
		t = KFS_DIR;
	} else
		t = KFS_NONE;

	return (t != KFS_NONE);
}

/*!
 * \brief remove a time value from the component deque
 */
bool
KFS::pop_time(struct timeval &tv, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 3 || c.front() != tag)
		return false;

	c.pop_front();
	tv.tv_sec = toNumber(c.front());
	c.pop_front();
	tv.tv_usec = toNumber(c.front());
	c.pop_front();
	return (tv.tv_sec != -1 && tv.tv_usec != -1);
}
