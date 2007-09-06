
//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id:$
//
// Created 2006/08/23
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
//----------------------------------------------------------------------------

#ifndef CC_CONFIG_H
#define CC_CONFIG_H

//----------------------------------------------------------------------------
// Attribute to disable "unused variable" warnings
//
// Example:
//   int UNUSED_ATTR r = aFunctionThatAlwaysAlwaysAlwaysReturnsZero();
//   assert(r == 0);
//
// Note, this doesn't break the variable when it actually *is* used,
// as in a debug build.  It just makes the compiler keep quiet about
// not using it in release builds.
//----------------------------------------------------------------------------
#if !defined(UNUSED_ATTR)
#if defined(__GNUC__)
#define UNUSED_ATTR __attribute__((unused))
#else
#define UNUSED_ATTR
#endif
#endif


#endif // CC_CONFIG_H
