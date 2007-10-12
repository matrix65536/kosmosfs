//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/03/10
// Author: Sriram Rao (Kosmix Corp.) 
//
// Copyright 2006 Kosmix Corp.
//
// This file is part of Kosmos File System (KFS).
//
// Licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// 
//----------------------------------------------------------------------------

#include "TcpSocket.h"
#include "common/log.h"

#include "Globals.h"

#include <cerrno>
#include <netdb.h>

using std::min;
using std::max;
using std::vector;

using namespace KFS;
using namespace KFS::libkfsio;

TcpSocket::~TcpSocket()
{
    Close();
}

int TcpSocket::Listen(int port)
{
    struct sockaddr_in	ourAddr;
    int reuseAddr = 1;

    mSockFd = socket(PF_INET, SOCK_STREAM, 0);
    if (mSockFd == -1) {
        perror("Socket: ");
        return -1;
    }

    memset(&ourAddr, 0, sizeof(struct sockaddr_in));
    ourAddr.sin_family = AF_INET;
    ourAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    ourAddr.sin_port = htons(port);

    /* 
     * A piece of magic here: Before we bind the fd to a port, setup
     * the fd to reuse the address. If we move this line to after the
     * bind call, then things don't work out.  That is, we bind the fd
     * to a port; we panic; on restart, bind will fail with the address
     * in use (i.e., need to wait 2MSL for TCP's time-wait).  By tagging
     * the fd to reuse an address, everything is happy.
     */
    if (setsockopt(mSockFd, SOL_SOCKET, SO_REUSEADDR, 
                   (char *) &reuseAddr, sizeof(reuseAddr)) < 0) {
        perror("Setsockopt: ");
    }

    if (bind(mSockFd, (struct sockaddr *) &ourAddr, sizeof(ourAddr)) < 0) {
        perror("Bind: ");
        close(mSockFd);
        mSockFd = -1;
        return -1;
    }
    
    if (listen(mSockFd, 5) < 0) {
        perror("listen: ");
    }

    globals().ctrOpenNetFds.Update(1);

    return 0;
    
}

TcpSocket* TcpSocket::Accept()
{
    int fd;
    struct sockaddr_in	cliAddr;    
    TcpSocket *accSock;
    socklen_t cliAddrLen = sizeof(cliAddr);

    if ((fd = accept(mSockFd, (struct sockaddr *) &cliAddr, &cliAddrLen)) < 0) {
        perror("Accept: ");
        return NULL;
    }
    accSock = new TcpSocket(fd);

    accSock->SetupSocket();

    globals().ctrOpenNetFds.Update(1);

    return accSock;
}

int TcpSocket::Connect(const struct sockaddr_in *remoteAddr)
{
    int res;

    Close();

    mSockFd = socket(PF_INET, SOCK_STREAM, 0);
    if (mSockFd == -1) {
        return -1;
    }

    res = connect(mSockFd, (struct sockaddr *) remoteAddr, sizeof(struct sockaddr_in));
    if (res < 0) {
        perror("Connect: ");
        close(mSockFd);
        mSockFd = -1;
        return -1;
    }

    SetupSocket();

    globals().ctrOpenNetFds.Update(1);

    return 0;
}

int TcpSocket::Connect(const ServerLocation &location)
{
    struct hostent *hostInfo;
    struct sockaddr_in remoteAddr;
    int res;

    hostInfo = gethostbyname(location.hostname.c_str());
    if (hostInfo == NULL) {
        return -1;
    }

    Close();

    mSockFd = socket(PF_INET, SOCK_STREAM, 0);
    if (mSockFd == -1) {
        return -1;
    }

    memcpy(&remoteAddr.sin_addr.s_addr, hostInfo->h_addr, sizeof(struct in_addr));
    remoteAddr.sin_port = htons(location.port);
    remoteAddr.sin_family = AF_INET;

    res = connect(mSockFd, (struct sockaddr *) &remoteAddr, sizeof(struct sockaddr_in));

    if (res < 0) {
        perror("Connect: ");
        close(mSockFd);
        mSockFd = -1;
        return -1;
    }

    SetupSocket();

    globals().ctrOpenNetFds.Update(1);

    return 0;
}

void TcpSocket::SetupSocket()
{
    int bufSize = 65536;

    // get big send/recv buffers and setup the socket for non-blocking I/O
    if (setsockopt(mSockFd, SOL_SOCKET, SO_SNDBUF, (char *) &bufSize, sizeof(bufSize)) < 0) {
        perror("Setsockopt: ");
    }
    if (setsockopt(mSockFd, SOL_SOCKET, SO_RCVBUF, (char *) &bufSize, sizeof(bufSize)) < 0) {
        perror("Setsockopt: ");
    }
    fcntl(mSockFd, F_SETFL, O_NONBLOCK);

}

int TcpSocket::GetRemoteName(struct sockaddr *remoteAddr, socklen_t *remoteLen)
{
    if (getpeername(mSockFd, remoteAddr, remoteLen) < 0) {
        perror("getpeername: ");
        return -1;
    }
    return 0;
}

int TcpSocket::Send(const char *buf, int bufLen)
{
    int nwrote;

    nwrote = send(mSockFd, buf, bufLen, 0);
    if (nwrote > 0) {
        globals().ctrNetBytesWritten.Update(nwrote);
    }
    return nwrote;
}

int TcpSocket::Recv(char *buf, int bufLen)
{
    int nread;

    nread = recv(mSockFd, buf, bufLen, 0);
    if (nread > 0) {
        globals().ctrNetBytesRead.Update(nread);
    }

    return nread;
}

int TcpSocket::Peek(char *buf, int bufLen)
{
    int nread;

    nread = recv(mSockFd, buf, bufLen, MSG_PEEK);
    return nread;
}

bool TcpSocket::IsGood()
{
    if (mSockFd < 0)
        return false;

    char c;
    
    // the socket could've been closed by the system because the peer
    // died.  so, tell if the socket is good, peek to see if any data
    // can be read; read returns 0 if the socket has been
    // closed. otherwise, will get -1 with errno=EAGAIN.
    
    if (Peek(&c, 1) == 0)
        return false;

    return true;
}


void TcpSocket::Close()
{
    if (mSockFd < 0) {
        return;
    }
    close(mSockFd);
    mSockFd = -1;
    globals().ctrOpenNetFds.Update(-1);
}

int TcpSocket::DoSynchSend(const char *buf, int bufLen)
{
    int numSent = 0;
    int res;

    while (numSent < bufLen) {
        res = Send(buf + numSent, bufLen - numSent);
        if (res == 0)
            return 0;
        if ((res < 0) && (errno == EAGAIN))
            continue;
        if (res < 0)
            break;
        numSent += res;
    }
    if (numSent > 0) {
        globals().ctrNetBytesWritten.Update(numSent);
    }
    return numSent;
}

// 
// Receive data within a certain amount of time.  If the server is too slow in responding, bail
//
int TcpSocket::DoSynchRecv(char *buf, int bufLen, struct timeval &timeout)
{
    int numRecd = 0;
    int res, nfds;
    fd_set fds;

    while (numRecd < bufLen) {
        FD_ZERO(&fds);
        FD_SET(mSockFd, &fds);
        nfds = select(mSockFd + 1, &fds, NULL, NULL, &timeout);
        if ((nfds == 0) && 
            ((timeout.tv_sec == 0) && (timeout.tv_usec == 0))) {
            KFS_LOG_DEBUG("Timeout in synch recv");
            return numRecd > 0 ? numRecd : -ETIMEDOUT;
        }

        res = Recv(buf + numRecd, bufLen - numRecd);
        if (res == 0)
            return 0;
        if ((res < 0) && (errno == EAGAIN))
            continue;
        if (res < 0)
            break;
        numRecd += res;
    }
    if (numRecd > 0) {
        globals().ctrNetBytesRead.Update(numRecd);
    }

    return numRecd;
}


// 
// Receive data within a certain amount of time and discard them.  If
// the server is too slow in responding, bail
//
int TcpSocket::DoSynchDiscard(int nbytes, struct timeval &timeout)
{
    int numRecd = 0, ntodo, res;
    const int bufSize = 4096;
    char buf[bufSize];

    while (numRecd < nbytes) {
        ntodo = min(nbytes - numRecd, bufSize);
        res = DoSynchRecv(buf, ntodo, timeout);
        if (res == -ETIMEDOUT)
            return numRecd;
        assert(numRecd >= 0);
        if (numRecd < 0)
            break;
        numRecd += res;
    }
    return numRecd;
}

// 
// Peek data within a certain amount of time.  If the server is too slow in responding, bail
//
int TcpSocket::DoSynchPeek(char *buf, int bufLen, struct timeval &timeout)
{
    int numRecd = 0;
    int res, nfds;
    fd_set fds;

    while (1) {
        FD_ZERO(&fds);
        FD_SET(mSockFd, &fds);
        nfds = select(mSockFd + 1, &fds, NULL, NULL, &timeout);
        if ((nfds == 0) && 
            ((timeout.tv_sec == 0) && (timeout.tv_usec == 0))) {
            return -ETIMEDOUT;
        }

        res = Peek(buf + numRecd, bufLen - numRecd);
        if (res == 0)
            return 0;
        if ((res < 0) && (errno == EAGAIN))
            continue;
        if (res < 0)
            break;
        numRecd += res;
        if (numRecd > 0)
            break;
    }
    return numRecd;
}


int KFS::Simulcast(const char *buf, int bufLen, vector<TcpSocket *> &targets)
{
    int res, nfds, fd, lastfd;
    fd_set writeSet;
    struct timeval timeout;
    vector<int> nsent;
    uint32_t i, ndone = 0;

    nsent.resize(targets.size());
    for (i = 0; i < nsent.size(); i++)
        nsent[i] = 0;

    while (1) {
        ndone = 0;
        for (i = 0; i < targets.size(); i++) {
            if (nsent[i] < bufLen)
                break;
            ndone++;
        }
        if (ndone == targets.size())
            break;

        // run in loops of 100 micro-seconds
        timeout.tv_sec = 0;
        timeout.tv_usec = 100;
        FD_ZERO(&writeSet);
        lastfd = 0;
        for (i = 0; i < targets.size(); i++) {
            if (nsent[i] >= bufLen)
                continue;

            if (!targets[i]->IsGood())
                return -1;
            fd = targets[i]->GetFd();
            lastfd = max(fd, lastfd);
            FD_SET(fd, &writeSet);
        }

        nfds = select(lastfd + 1, NULL, &writeSet, NULL, &timeout);
        if (nfds < 0) {
            perror("Select(): " );
            return -1;
        }
        for (i = 0; i < targets.size(); i++) {
            fd = targets[i]->GetFd();
            // push some data on the socket if any
            if ((FD_ISSET(fd, &writeSet)) && (nsent[i] < bufLen)) {
                res = targets[i]->Send(buf + nsent[i], bufLen - nsent[i]);
                if (res > 0)
                    nsent[i] += res;
            }
        }
    }
    return 0;
}
