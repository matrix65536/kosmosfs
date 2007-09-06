//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/Utils.h#2 $
//
// Created 2006/09/27
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
// 
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_UTILS_H
#define CHUNKSERVER_UTILS_H

#include "libkfsIO/IOBuffer.h"
#include <string>
using std::string;

///
/// Given some data in a buffer, determine if we have a received a
/// valid op---one that ends with "\r\n\r\n".  
/// @param[in]  iobuf : buffer containing data
/// @param[out] msgLen : if we do have a valid command, return the length of
/// the command
/// @retval True if we have a valid command; False otherwise.
///
extern bool IsMsgAvail(IOBuffer *iobuf, int *msgLen);

///
/// \brief bomb out on "impossible" error
/// \param[in] msg       panic text
///
extern void die(const string &msg);

#endif // CHUNKSERVER_UTILS_H
