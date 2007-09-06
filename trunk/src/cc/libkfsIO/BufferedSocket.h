//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/BufferedSocket.h#3 $
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
// \brief Helper class that has a socket with a buffer.  This enables
// API such as readLine() where we read N bytes from the socket,
// return a line and leave the rest buffered for subsequent access.
// NOTE: This class only does buffered I/O for reads; for writes, it
// is all pass-thru to the TcpSocket class.
//----------------------------------------------------------------------------

#ifndef LIBKFSIO_BUFFEREDSOCKET_H
#define LIBKFSIO_BUFFEREDSOCKET_H

#include <cerrno>
#include <cassert>
#include "TcpSocket.h"
#include <string>
using std::string;

class BufferedSocket : public TcpSocket {
public:
    BufferedSocket() {
        Reset();
    }
    BufferedSocket(int fd) : TcpSocket(fd) {
        Reset();
    }
    /// Read a line of data which is terminated by a '\n' from the
    /// socket.
    /// @param[out] result   The string that corresponds to data read
    /// from the network
    /// @retval # of bytes read; -1 on error
    int ReadLine(string &result);

    /// Synchronously (blocking) receive for the desired # of bytes.
    /// Note that we first pull data out the buffer and if there is
    /// more to be read, we get it from the socket.
    /// @param[out] buf  The buffer to be filled with data from the
    /// socket.
    /// @param[in] bufLen  The desired # of bytes to be read in
    /// @param[in] timeout  The max amount of time to wait for data
    /// @retval # of bytes read; -1 on error; -ETIMEOUT if timeout
    /// expires and no data is read in
    int DoSynchRecv(char *buf, int bufLen, struct timeval &timeout);

    /// Read at most the specified # of bytes from the socket.
    /// Note that we first pull data out the buffer and if there is
    /// more to be read, we get it from the socket.  The read is
    /// non-blocking: if recv() returns EAGAIN (to indicate that no
    /// data is available), we return how much ever data we have read
    /// so far.
    /// @param[out] buf  The buffer to be filled with data from the
    /// socket.
    /// @param[in] bufLen  The desired # of bytes to be read in
    /// @retval # of bytes read; -1 on error
    int Recv(char *buf, int bufLen);

private:
    const static int BUF_SIZE = 4096;
    /// The buffer into which data has been read from the socket.
    char mBuffer[BUF_SIZE];
    /// Since we have read from the buffer, head tracks where the next
    /// character is available for read from mBuffer[]
    char *mHead;
    /// How much data is in the buffer
    int mAvail;
    void Reset() {
        mHead = mBuffer;
        mAvail = 0;
        memset(mBuffer, '\0', BUF_SIZE);
    }
    void Fill(int nbytes) {
        mAvail += nbytes;
        assert(mAvail <= BUF_SIZE);
    }
    void Consume(int nbytes) {
        mHead += nbytes;
        mAvail -= nbytes;
        if (mAvail == 0)
            Reset();
        assert(mAvail >= 0);
    }

};

#endif // LIBKFSIO_BUFFEREDSOCKET_H
