//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsClient/Utils.h#3 $
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
// \brief Utils.h: Utilities for manipulating paths and other misc. support.
// 
//----------------------------------------------------------------------------

#ifndef LIBKFSCLIENT_UTILS_H
#define LIBKFSCLIENT_UTILS_H

#include <string>

namespace KFS {

// we call this function by creating temporaries on the stack. to let
// that thru, dont' stick in "&"
extern std::string strip_dots(std::string path);
extern std::string build_path(std::string &cwd, const char *input);

// Introduce a delay for nsecs...i.e., sleep
extern void Sleep(int nsecs);

}

#endif // LIBKFSCLIENT_UTILS_H
