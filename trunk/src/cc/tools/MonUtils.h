//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/tools/MonUtils.h#4 $
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
// 
//----------------------------------------------------------------------------

#ifndef TOOLS_MONUTILS_H
#define TOOLS_MONUTILS_H

#include <vector>
#include <string>
#include <sstream>
using std::vector;
using std::string;
using std::ostringstream;

#include "libkfsIO/TcpSocket.h"
#include "common/properties.h"

namespace KFS_MON
{
    enum KfsMonOp_t {
        CMD_METAPING,
        CMD_CHUNKPING,
        CMD_METASTATS,
        CMD_CHUNKSTATS
    };

    struct KfsMonOp {
        KfsMonOp_t op;
        int32_t  seq;
        ssize_t  status;
        KfsMonOp(KfsMonOp_t o, int32_t s) :
            op(o), seq(s) { };
        virtual ~KfsMonOp() { };
        virtual void Request(ostringstream &os) = 0;
        virtual void ParseResponse(const char *resp, int len) = 0;
        void ParseResponseCommon(string &resp, Properties &prop);
    };

    struct MetaPingOp : public KfsMonOp {
        vector<string> servers; /// result
        MetaPingOp(int32_t s) :
            KfsMonOp(CMD_METAPING, s) { };
        void Request(ostringstream &os);
        void ParseResponse(const char *resp, int len);
    };

    struct ChunkPingOp : public KfsMonOp {
        ServerLocation location;
        size_t totalSpace;
        size_t usedSpace;
        ChunkPingOp(int32_t s) :
            KfsMonOp(CMD_CHUNKPING, s) { };
        void Request(ostringstream &os);
        void ParseResponse(const char *resp, int len);
    };

    struct MetaStatsOp : public KfsMonOp {
        Properties stats; // result
        MetaStatsOp(int32_t s) :
            KfsMonOp(CMD_METAPING, s) { };
        void Request(ostringstream &os);
        void ParseResponse(const char *resp, int len);
    };

    struct ChunkStatsOp : public KfsMonOp {
        Properties stats; // result
        ChunkStatsOp(int32_t s) :
            KfsMonOp(CMD_CHUNKPING, s) { };
        void Request(ostringstream &os);
        void ParseResponse(const char *resp, int len);
    };

    extern int DoOpCommon(KfsMonOp *op, TcpSocket *sock);
    extern int GetResponse(char *buf, int bufSize,
                           int *delims,
                           TcpSocket *sock);

}

#endif // TOOLS_MONUTILS_H
