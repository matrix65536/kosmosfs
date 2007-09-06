//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/chunk/Utils.cc#2 $
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

#include "Utils.h"
#include "common/log.h"

///
/// Return true if there is a sequence of "\r\n\r\n".
/// @param[in] iobuf: Buffer with data sent by the client
/// @param[out] msgLen: string length of the command in the buffer
/// @retval true if a command is present; false otherwise.
///
bool IsMsgAvail(IOBuffer *iobuf, int *msgLen)
{
    char buf[1024];
    int nAvail, len = 0, i;

    nAvail = iobuf->BytesConsumable();
    if (nAvail > 1024)
        nAvail = 1024;
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

void
die(const string &msg)
{
    COSMIX_LOG_ERROR("Panic'ing: %s", msg.c_str());
    abort();
}

