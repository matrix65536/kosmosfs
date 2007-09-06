//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/tools/MonUtils.cc#5 $
//
// Created 2006/07/18
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
// \brief Utilities for monitoring/stat extraction from meta/chunk servers.
//----------------------------------------------------------------------------

#include "MonUtils.h"
#include "common/kfstypes.h"

#include <cassert>
#include <cerrno>
#include <sstream>
using std::istringstream;

using namespace KFS_MON;

const char *KFS_VERSION_STR = "KFS/1.0";
const int CMD_BUF_SIZE = 1024;

void
KfsMonOp::ParseResponseCommon(string &resp, Properties &prop)
{
    istringstream ist(resp);
    kfsSeq_t resSeq;
    const char separator = ':';
    
    prop.loadProperties(ist, separator, false);
    resSeq = prop.getValue("Cseq", -1);
    this->status = prop.getValue("Status", -1);
}

void 
MetaPingOp::Request(ostringstream &os)
{
    os << "PING\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void
MetaPingOp::ParseResponse(const char *resp, int len)
{
    string respStr(resp, len);
    Properties prop;
    string serv;
    const char delim = ' ';
    string::size_type start, end;

    ParseResponseCommon(respStr, prop);
    serv = prop.getValue("Servers", "");
    start = serv.find_first_of("s=");
    if (start == string::npos) {
        return;
    }

    string serverInfo;

    while (start != string::npos) {
        end = serv.find_first_of(delim, start);

        if (end != string::npos)
            serverInfo.assign(serv, start, end - start);
        else
            serverInfo.assign(serv, start, serv.size() - start);

        this->servers.push_back(serverInfo);
        start = serv.find_first_of("s=", end);
    }
    
}

void 
ChunkPingOp::Request(ostringstream &os)
{
    os << "PING\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void
ChunkPingOp::ParseResponse(const char *resp, int len)
{
    string respStr(resp, len);
    Properties prop;

    ParseResponseCommon(respStr, prop);
    location.hostname = prop.getValue("Meta-server-host", "");
    location.port = prop.getValue("Meta-server-port", 0);
    totalSpace = prop.getValue("Total-space", (long long) 0);
    usedSpace = prop.getValue("Used-space", (long long) 0);
}

void 
MetaStatsOp::Request(ostringstream &os)
{
    os << "STATS\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void 
ChunkStatsOp::Request(ostringstream &os)
{
    os << "STATS\r\n";
    os << "Version: " << KFS_VERSION_STR << "\r\n";
    os << "Cseq: " << seq << "\r\n\r\n";
}

void
MetaStatsOp::ParseResponse(const char *resp, int len)
{
    string respStr(resp, len);

    ParseResponseCommon(respStr, stats);
}

void
ChunkStatsOp::ParseResponse(const char *resp, int len)
{
    string respStr(resp, len);

    ParseResponseCommon(respStr, stats);
}

int
KFS_MON::DoOpCommon(KfsMonOp *op, TcpSocket *sock)
{
    int numIO;
    char buf[CMD_BUF_SIZE];
    int len;
    ostringstream os;

    op->Request(os);
    numIO = sock->DoSynchSend(os.str().c_str(), os.str().length());
    if (numIO <= 0) {
        op->status = -1;
        return -1;
    }

    numIO = GetResponse(buf, CMD_BUF_SIZE, &len, sock);

    if (numIO <= 0) {
        op->status = -1;
        return -1;
    }

    assert(len > 0);

    op->ParseResponse(buf, len);

    return numIO;
}

///
/// Get a response from the server.  The response is assumed to
/// terminate with "\r\n\r\n".
/// @param[in/out] buf that should be filled with data from server
/// @param[in] bufSize size of the buffer
///
/// @param[out] delims the position in the buffer where "\r\n\r\n"
/// occurs; in particular, the length of the response string that ends
/// with last "\n" character.  If the buffer got full and we couldn't
/// find "\r\n\r\n", delims is set to -1.
///
/// @param[in] sock the socket from which data should be read
/// @retval # of bytes that were read; 0/-1 if there was an error
/// 
int
KFS_MON::GetResponse(char *buf, int bufSize,
                     int *delims,
                     TcpSocket *sock)
{
    int nread;
    int i;

    *delims = -1;
    while (1) {
        struct timeval timeout;

        timeout.tv_sec = 300;
        timeout.tv_usec = 0;

        nread = sock->DoSynchPeek(buf, bufSize, timeout);
        if (nread <= 0)
            return nread;
        for (i = 4; i <= nread; i++) {
            if (i < 4)
                break;
            if ((buf[i - 3] == '\r') &&
                (buf[i - 2] == '\n') &&
                (buf[i - 1] == '\r') &&
                (buf[i] == '\n')) {
                // valid stuff is from 0..i; so, length of resulting
                // string is i+1.
                memset(buf, '\0', bufSize);
                *delims = (i + 1);
                nread = sock->Recv(buf, *delims);
                return nread;
            }
        }
    }
    return -1;
}
