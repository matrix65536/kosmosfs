//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/tools/KfsStats_main.cc#5 $
//
// Created 2006/07/20
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
// \brief Get the stats from chunk/meta servers.  Run in a loop until
// the user hits Ctrl-C.  Like iostat, results are refreshed every N secs
//----------------------------------------------------------------------------

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
};

#include <iostream>
#include <string>
using std::string;
using std::cout;
using std::endl;

#include "libkfsIO/TcpSocket.h"
#include "common/log.h"

#include "MonUtils.h"
using namespace KFS_MON;

static void
StatsMetaServer(const ServerLocation &location, bool rpcStats, int numSecs);

void
BasicStatsMetaServer(TcpSocket &metaServerSock, int numSecs);

void
RpcStatsMetaServer(TcpSocket &metaServerSock, int numSecs);

static void
StatsChunkServer(const ServerLocation &location, bool rpcStats, int numSecs);

static void
BasicStatsChunkServer(TcpSocket &chunkServerSock, int numSecs);

static void
RpcStatsChunkServer(TcpSocket &chunkServerSock, int numSecs);

static void
PrintChunkBasicStatsHeader();

static void
PrintMetaBasicStatsHeader();


int main(int argc, char **argv)
{
    char optchar;
    bool help = false, meta = false, chunk = false;
    bool rpcStats = false;
    const char *server = NULL;
    int port = -1, numSecs = 10;

    while ((optchar = getopt(argc, argv, "hcmn:p:s:t")) != -1) {
        switch (optchar) {
            case 'm': 
                meta = true;
                break;
            case 'c':
                chunk = true;
                break;
            case 's':
                server = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'n':
                numSecs = atoi(optarg);
                break;
            case 't':
                rpcStats = true;
                break;
            case 'h':
                help = true;
                break;
            default:
                COSMIX_LOG_ERROR("Unrecognized flag %c", optchar);
                help = true;
                break;
        }
    }

    help = help || (!meta && !chunk);

    if (help || (server == NULL) || (port < 0)) {
        cout << "Usage: " << argv[0] << " [-m|-c] -s <server name> -p <port>" 
             << " [-n <secs>] [-t]"  << endl;
        cout << "Use -m for metaserver and -c for chunk server" << endl;
        cout << "Use -t for RPC stats" << endl;
        exit(-1);
    }

    ServerLocation location(server, port);

    if (meta)
        StatsMetaServer(location, rpcStats, numSecs);
    else if (chunk)
        StatsChunkServer(location, rpcStats, numSecs);
}

static void
PrintRpcStat(const string &statName, Properties &prop)
{
    cout << statName << " = " << prop.getValue(statName, (long long) 0) << endl;
}


void
StatsMetaServer(const ServerLocation &location, bool rpcStats, int numSecs)
{
    TcpSocket metaServerSock;

    if (metaServerSock.Connect(location) < 0) {
        COSMIX_LOG_ERROR("Unable to connect to %s",
                         location.ToString().c_str());
        exit(0);
    }

    if (rpcStats) {
        RpcStatsMetaServer(metaServerSock, numSecs);
    } else {
        BasicStatsMetaServer(metaServerSock, numSecs);
    }
    metaServerSock.Close();
}

void
RpcStatsMetaServer(TcpSocket &metaServerSock, int numSecs)
{
    int numIO;
    int cmdSeqNum = 1;

    while (1) {
        MetaStatsOp op(cmdSeqNum);
        ++cmdSeqNum;
        numIO = DoOpCommon(&op, &metaServerSock);
        if (numIO < 0) {
            COSMIX_LOG_ERROR("Server isn't responding to stats");
            exit(0);
        }
        
        PrintRpcStat("Get alloc", op.stats);
        PrintRpcStat("Get layout", op.stats);
        PrintRpcStat("Lookup", op.stats);
        PrintRpcStat("Lookup Path", op.stats);
        PrintRpcStat("Allocate", op.stats);
        PrintRpcStat("Truncate", op.stats);
        PrintRpcStat("Create", op.stats);
        PrintRpcStat("Remove", op.stats);
        PrintRpcStat("Rename", op.stats);
        PrintRpcStat("Mkdir", op.stats);
        PrintRpcStat("Rmdir", op.stats);
        PrintRpcStat("Lease Acquire", op.stats);
        PrintRpcStat("Lease Renew", op.stats);
        PrintRpcStat("Lease Cleanup", op.stats);
        PrintRpcStat("Chunkserver Hello", op.stats);
        PrintRpcStat("Chunkserver Bye", op.stats);
        PrintRpcStat("Replication Checker", op.stats);
        PrintRpcStat("Chunkserver Done", op.stats);
        PrintRpcStat("Num Ongoing Replications", op.stats);
        PrintRpcStat("Num Failed Replications", op.stats);
        PrintRpcStat("Total Num Replications", op.stats);

        cout << "----------------------------------" << endl;
        sleep(numSecs);
    }
}

void
BasicStatsMetaServer(TcpSocket &metaServerSock, int numSecs)
{
    int numIO;
    int cmdSeqNum = 1;

    PrintMetaBasicStatsHeader();
    while (1) {
        MetaStatsOp op(cmdSeqNum);
        ++cmdSeqNum;
        numIO = DoOpCommon(&op, &metaServerSock);
        if (numIO < 0) {
            COSMIX_LOG_ERROR("Server isn't responding to stats");
            exit(0);
        }
        // useful things to have: # of connections handled
        if (cmdSeqNum % 10 == 0)
            PrintMetaBasicStatsHeader();

        cout << op.stats.getValue("Open network fds", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes read from network", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes written to network", (long long) 0) << endl;

        sleep(numSecs);
    }
}

static void
PrintMetaBasicStatsHeader()
{
    cout << "Net Fds" << '\t' << "N/w Bytes In" << '\t'
         << "N/w Bytes Out" << endl;
}

void
StatsChunkServer(const ServerLocation &location, bool rpcStats, int numSecs)
{
    TcpSocket chunkServerSock;

    if (chunkServerSock.Connect(location) < 0) {
        COSMIX_LOG_ERROR("Unable to connect to %s",
                         location.ToString().c_str());
        exit(0);
    }

    if (rpcStats) {
        RpcStatsChunkServer(chunkServerSock, numSecs);
    } else {
        BasicStatsChunkServer(chunkServerSock, numSecs);
    }

    chunkServerSock.Close();
}

void
RpcStatsChunkServer(TcpSocket &chunkServerSock, int numSecs)
{
    int numIO;
    int cmdSeqNum = 1;

    while (1) {
        ChunkStatsOp op(cmdSeqNum);
        ++cmdSeqNum;
        numIO = DoOpCommon(&op, &chunkServerSock);
        if (numIO < 0) {
            COSMIX_LOG_ERROR("Server isn't responding to stats");
            exit(0);
        }

        PrintRpcStat("Alloc", op.stats);
        PrintRpcStat("Size", op.stats);
        PrintRpcStat("Open", op.stats);
        PrintRpcStat("Read", op.stats);
        PrintRpcStat("Write Prepare", op.stats);
        PrintRpcStat("Write Commit", op.stats);
        PrintRpcStat("Write Sync", op.stats);
        PrintRpcStat("Write Master", op.stats);
        PrintRpcStat("Delete", op.stats);
        PrintRpcStat("Truncate", op.stats);
        PrintRpcStat("Heartbeat", op.stats);
        PrintRpcStat("Change Chunk Vers", op.stats);
        cout << "----------------------------------" << endl;
        sleep(numSecs);
    }
}

void
BasicStatsChunkServer(TcpSocket &chunkServerSock, int numSecs)
{
    int numIO;
    int cmdSeqNum = 1;

    PrintChunkBasicStatsHeader();
    while (1) {
        ChunkStatsOp op(cmdSeqNum);
        ++cmdSeqNum;
        numIO = DoOpCommon(&op, &chunkServerSock);
        if (numIO < 0) {
            COSMIX_LOG_ERROR("Server isn't responding to stats");
            exit(0);
        }

        if (cmdSeqNum % 10 == 0)
            PrintChunkBasicStatsHeader();

        cout << op.stats.getValue("Open network fds", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes read from network", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes written to network", (long long) 0) << '\t';
        cout << op.stats.getValue("Open disk fds", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes read from disk", (long long) 0) << '\t';
        cout << op.stats.getValue("Bytes written to disk", (long long) 0) << endl;

        sleep(numSecs);
    }
}

static void
PrintChunkBasicStatsHeader()
{
    cout << "Net Fds" << '\t' << "N/w Bytes In" << '\t'
         << "N/w Bytes Out" << '\t' 
         << "Disk Fds" << '\t' << "Disk Bytes In" << '\t' 
         << "Disk Bytes Out" << endl;
}
