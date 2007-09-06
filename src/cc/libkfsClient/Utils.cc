//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsClient/Utils.cc#3 $
//
// Created 2006/08/31
// Author: Sriram Rao (Kosmix Corp.) 
//
// Copyright (C) 2006 Kosmix Corp.
//
// This file is part of Kosmos File System (KFS).
//
// KFS is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation under version 3 of the License.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see
// <http://www.gnu.org/licenses/>.
//
// \brief Utilities for manipulating paths and other misc support.
//
//----------------------------------------------------------------------------

#include "Utils.h"

#include <cassert>
#include <vector>
using std::vector;
using std::string;

extern "C" {
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
}

using namespace KFS;

string
KFS::strip_dots(string path)
{
	vector <string> component;
	string result;
	string::size_type start = 0;

	while (start != string::npos) {
		assert(path[start] == '/');
		string::size_type slash = path.find('/', start + 1);
		string nextc(path, start, slash - start);
		start = slash;
		if (nextc.compare("/..") == 0) {
			if (!component.empty())
				component.pop_back();
		} else if (nextc.compare("/.") != 0)
			component.push_back(nextc);
	}

	if (component.empty())
		component.push_back(string("/"));

	for (vector <string>::iterator c = component.begin();
			c != component.end(); c++) {
		result += *c;
	}
	return result;
}

/*
 * Take a path name that was supplied as an argument for a KFS operation.
 * If it is not absolute, add the current directory to the front of it and
 * in either case, call strip_dots to strip out any "." and ".." components.
 */
string
KFS::build_path(string &cwd, const char *input)
{
	string tail(input);
	if (input[0] == '/')
		return strip_dots(tail);

	const char *c = cwd.c_str();
	bool is_root = (c[0] == '/' && c[1] == '\0');
	string head(c);
	if (!is_root)
		head.append("/");
	return strip_dots(head + tail);
}

void
KFS::Sleep(int nsecs)
{
	fd_set rfds;
	struct timeval timeout;

	FD_ZERO(&rfds);
	FD_SET(0, &rfds);
	timeout.tv_sec = nsecs;
	timeout.tv_usec = 0;
	select(1, &rfds, NULL, NULL, &timeout);
}
