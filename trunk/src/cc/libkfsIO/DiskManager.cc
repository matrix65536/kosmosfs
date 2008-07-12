//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/03/22
// Author: Sriram Rao
//
// Copyright 2008 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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

#include "DiskManager.h"
#include "Globals.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <aio.h>
#include <string.h>
#include <cerrno>
#include <algorithm>

#if defined (__sun__)
#include <signal.h>
#include <port.h>
#include "meta/thread.h"
#endif

#include "common/log.h"

using std::list;
using std::find_if;
using namespace KFS;
using namespace KFS::libkfsio;

///
/// \file DiskManager.cc
/// \brief Implements methods defined in DiskManager.h
///

static void
handleAIOCompletion(DiskEvent_t *event)
{
    event->aioStatus = aio_error(&(event->aio_cb));
    if (event->aioStatus == EINPROGRESS)
        return;

    if (event->aioStatus == ECANCELED) {
        event->status = EVENT_CANCELLED;
    } else {
        // this is a completion handler; record the return value and
        // put it in the completed queue; when the event is reaped,
        // its status will get updated
        event->retval = aio_return(&(event->aio_cb));
    }

    globals().diskManager.IOCompleted(event);
    globals().netKicker.Kick();
}

#if !defined (__sun__)
static void
aioCompletionHandler(sigval val)
{
    DiskEvent_t *event = (DiskEvent_t *) val.sival_ptr;

    handleAIOCompletion(event);
}
#endif

#if defined (__sun__)

static void* aioWorker(void *dummy);

// we use event port facility on Solaris to track AIO completion.
class AIOCompletion_t {
public:
    AIOCompletion_t() { }
    ~AIOCompletion_t() { 
        if (eventPort < 0)
            return;
        close(eventPort);
    }
    inline int GetCompletionPort() const {
        return eventPort;
    }
    void Init() {
        eventPort = port_create();
        if (eventPort < 0) {
            perror("port_create");
            return;
        }
        completionProcessor.start(aioWorker, NULL);
    }
    void MainLoop() {
        port_event_t pe;
        DiskEvent_t *event;
        int res;
        for (;;) {
            // poll for one event at a time
            res = port_get(eventPort, &pe, NULL);
            if (res < 0)
                continue;
            // notify
            event = (DiskEvent_t *) pe.portev_user;
            // assert(event->aio_cb == (struct aiocb *) pe->portev_object);
            handleAIOCompletion(event);
        }
    }
    
private:
    // the event port at which we get notified
    int eventPort;
    // the thread responsible for completion handling
    MetaThread completionProcessor;
};

AIOCompletion_t aioCompletionHandler;

static void*
aioWorker(void *dummy)
{
    aioCompletionHandler.MainLoop();
    return NULL;
}

#endif


DiskManager::DiskManager() : mOverloaded(false), mMaxOutstandingIOs(5000)
{

    // mDiskManagerTimeoutImpl = new DiskManagerTimeoutImpl(this);
}

DiskManager::~DiskManager()
{
    // globals().netManager.UnRegisterTimeoutHandler(mDiskManagerTimeoutImpl);

    // delete mDiskManagerTimeoutImpl;
}

void
DiskManager::Init()
{

}

void
DiskManager::InitForAIO()
{
#if defined (__sun__)
    // create a thread to handle AIO completions
    aioCompletionHandler.Init();
#endif
    // globals().netManager.RegisterTimeoutHandler(mDiskManagerTimeoutImpl);
}

class DiskEventMatcher {
    DiskEvent_t *event;
public:
    DiskEventMatcher(DiskEvent_t *e) : event(e) { }
    bool operator()(DiskEventPtr other) {
        return other.get() == event;
    }
};

///
/// For those events that have been completed, call back with the result of the event.
///

void
DiskManager::ReapCompletedIOs()
{
    DiskEvent_t *c;
    DiskEventPtr event;
    list<DiskEventPtr>::iterator iter;

    while ((c = mCompleted.dequeue_nowait()) != NULL) {
        iter = find_if(mDiskEvents.begin(), mDiskEvents.end(), DiskEventMatcher(c));
        if (iter == mDiskEvents.end())
            continue;
        event = *iter;
        if (event->status == EVENT_CANCELLED) {
            iter = mDiskEvents.erase(iter);
            continue;
        }
        event->status = EVENT_DONE;

        // we are at the event that has finished
        if ((event->op == OP_READ) && (event->retval > 0)) {
            // if the read finished successfully, event->retval
            // contains the # of bytes that were read
            event->data->Fill(event->retval);
        }
        event->conn->HandleDone(event, event->aioStatus);
        iter = mDiskEvents.erase(iter);
        IOCompleted();
    }
}

// static void
void
aioSetup(DiskEventPtr &event, int fd, off_t offset, int numBytes, char *buf)
{
    struct aiocb *aio_cb = &(event->aio_cb);

    aio_cb->aio_fildes = fd;
    aio_cb->aio_offset = offset;
    aio_cb->aio_nbytes = numBytes;
    aio_cb->aio_buf = buf;
    // get notified when the I/O finishes
#if defined (__sun__)
    // SIGEV_THREAD is not supported on solaris...so, event port
    // see the link at: http://developers.sun.com/solaris/articles/event_completion.html
    aio_cb->aio_sigevent.sigev_notify = SIGEV_PORT;
    event->port_notify.portnfy_port = aioCompletionHandler.GetCompletionPort();
    event->port_notify.portnfy_user = (void *) (event.get());
    aio_cb->aio_sigevent.sigev_value.sival_ptr = (void *) (&event->port_notify);
#else
    aio_cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    aio_cb->aio_sigevent.sigev_notify_function = aioCompletionHandler;
    aio_cb->aio_sigevent.sigev_value.sival_ptr = (void *) (event.get());
#endif
}

///
/// See the comments in DiskManager.h.  
///
int
DiskManager::Read(DiskConnection *conn, int fd,
                  IOBufferDataPtr &data,
                  off_t offset, int numBytes,
                  DiskEventPtr &resultEvent)
{
    DiskEventPtr event(new DiskEvent_t(conn->shared_from_this(), data, OP_READ));

    // schedule a read request
    aioSetup(event, fd, offset, numBytes, data->Producer());
    struct aiocb *aio_cb = &(event->aio_cb);

#if 0
    aio_cb->aio_fildes = fd;
    aio_cb->aio_offset = offset;
    aio_cb->aio_nbytes = numBytes;
    aio_cb->aio_buf = data->Producer();
    // get notified when the I/O finishes
    aio_cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    aio_cb->aio_sigevent.sigev_notify_function = aioCompletionHandler;
    aio_cb->aio_sigevent.sigev_value.sival_ptr = (void *) (event.get());
#endif

    // if (aio_read(&event->aio_cb) < 0) {
    if (aio_read(aio_cb) < 0) {
        perror("aio_read: ");
        return -1;
    }
    mDiskEvents.push_back(event);
    resultEvent = event;

    IOInitiated();

    return 0;
}

///
/// See the comments in DiskManager.h.  
///
int
DiskManager::Write(DiskConnection *conn, int fd,
                   IOBufferDataPtr &data,
                   off_t offset, int numBytes,
                   DiskEventPtr &resultEvent)
{
    DiskEventPtr event(new DiskEvent_t(conn->shared_from_this(), data, OP_WRITE));

    assert(numBytes <= data->BytesConsumable());

    assert(fd > 0);

    aioSetup(event, fd, offset, numBytes, data->Consumer());

    struct aiocb *aio_cb = &(event->aio_cb);

#if 0
    aio_cb->aio_fildes = fd;
    aio_cb->aio_offset = offset;
    aio_cb->aio_nbytes = numBytes;
    aio_cb->aio_buf = data->Consumer();
    // get notified when the I/O finishes
    aio_cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    aio_cb->aio_sigevent.sigev_notify_function = aioCompletionHandler;
    aio_cb->aio_sigevent.sigev_value.sival_ptr = (void *) (event.get());
#endif

    // if (aio_write(&event->aio_cb) < 0) {
    if (aio_write(aio_cb) < 0) {
        perror("aio_write: ");
        return -1;
    }
    mDiskEvents.push_back(event);
    resultEvent = event;

    IOInitiated();

    return 0;
}

///
/// For a sync, we use O_DSYNC.  This only sync's the data, but
/// doesn't update the associated inode information.  We do this to
/// save a disk I/O.  Should updating the inode information become
/// important, replace O_DSYNC in the aio_fsync() with O_SYNC.
///
int
DiskManager::Sync(DiskConnection *conn, int fd,
                  DiskEventPtr &resultEvent)
{
    DiskEventPtr event(new DiskEvent_t(conn->shared_from_this(), OP_SYNC));
    struct aiocb *aio_cb = &event->aio_cb;

    // KFS_LOG_VA_DEBUG("syncing fd = %d", fd);

    // schedule a datasync request
    aio_cb->aio_fildes = fd;
#if defined (__APPLE__)
    if (aio_fsync(O_SYNC, aio_cb) < 0) {
        perror("aio_sync: ");
        return -1;
    }
#else
    if (aio_fsync(O_DSYNC, aio_cb) < 0) {
        perror("aio_sync: ");
        return -1;
    }
#endif
    mDiskEvents.push_back(event);
    resultEvent = event;

    return 0;
}

void
DiskManager::IOInitiated()
{
    if (mDiskEvents.size() > mMaxOutstandingIOs) {
        KFS_LOG_VA_INFO("Too many disk IOs (%d) outstanding...overloaded", 
                        mDiskEvents.size());
        mOverloaded = true;
        globals().netManager.ChangeDiskOverloadState(true);
    }
}

void
DiskManager::IOCompleted()
{
    // either there is too much IO or we were overloaded and the load
    // hasn't dropped down significantly
    if ((mDiskEvents.size() > mMaxOutstandingIOs) ||
        (mOverloaded && (mDiskEvents.size() > mMaxOutstandingIOs / 2)))
        return;
    if (mOverloaded) {
        KFS_LOG_VA_INFO("# of disk I/Os outstanding (%d) is below limit...clearing overloaded",
                        mDiskEvents.size());
        mOverloaded = false;
        globals().netManager.ChangeDiskOverloadState(false);
    }
}

