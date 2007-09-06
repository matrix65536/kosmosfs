//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/BufferedSocket.cc#3 $
//
// Created 2006/07/03
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
// \brief Code for doing buffered reads from socket.
//
//----------------------------------------------------------------------------

#include <iostream>

#include "BufferedSocket.h"

int
BufferedSocket::ReadLine(string &result)
{
    int navail, nread = 0;
    char *lineEnd;

    lineEnd = index(mHead, '\n');
    if (lineEnd != NULL) {
        nread = lineEnd - mHead + 1;
        result.append(mHead, nread);
        Consume(nread);
        return nread;
    }
    
    // no line-end...so, copy out what is in the buffer
    if (mAvail > 0) {
        nread = mAvail;
        result.append(mHead, nread);
        Consume(nread);
    }
    
    // read until we get a new-line
    while (1) {
        navail = Recv(mBuffer, BUF_SIZE);
        if (navail == 0) {
            // socket is down
            return nread;
        }
        if ((navail < 0) && (errno == EAGAIN))
            continue;
        if (navail < 0)
            break;

        Fill(navail);

        lineEnd = index(mBuffer, '\n');
        if (lineEnd == NULL) {
            // haven't hit a line boundary...so, keep going
            result.append(mBuffer, navail);
            nread += navail;
            Consume(navail);
            continue;
        }
        navail = (lineEnd - mBuffer + 1);
        nread += navail;
        result.append(mBuffer, navail);
        Consume(navail);
        break;
    }
    return nread;
}

int
BufferedSocket::DoSynchRecv(char *buf, int bufLen, struct timeval &timeout)
{
    int nread = 0, res;

    // Copy out of the buffer and then, if needed, get from the socket.
    if (mAvail > 0) {
        nread = bufLen < mAvail ? bufLen : mAvail;
        memcpy(buf, mHead, nread);
        Consume(nread);
    }
    
    if ((bufLen - nread) <= 0)
        return nread;

    assert(mAvail == 0);

    res = TcpSocket::DoSynchRecv(buf + nread, bufLen - nread, timeout);
    if (res > 0)
        nread += res;
    return nread;
    
}

int
BufferedSocket::Recv(char *buf, int bufLen)
{
    int nread = 0, res;

    // Copy out of the buffer and then, if needed, get from the socket.
    if (mAvail > 0) {
        nread = bufLen < mAvail ? bufLen : mAvail;
        memcpy(buf, mHead, nread);
        Consume(nread);
    }
    
    if ((bufLen - nread) <= 0)
        return nread;

    assert(mAvail == 0);

    res = TcpSocket::Recv(buf + nread, bufLen - nread);
    if (res > 0) {
        nread += res;
        return nread;
    }
    if (nread == 0)
        return res;
    return nread;
}
