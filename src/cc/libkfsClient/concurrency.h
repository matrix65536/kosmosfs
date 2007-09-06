//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: concurrency.h $
//
// \brief Provide class definitions relating to concurrency.
// 
// Created 2007/08/21
// Author: Wang Lam (Kosmix Corp.) 
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

#ifndef LIBKFSCLIENT_CONCURRENCY_H
#define LIBKFSCLIENT_CONCURRENCY_H

#include <cerrno>
#include <cassert>
extern "C" {
#include <pthread.h>
}

namespace KFS
{
    class MutexLock;
}

//
// The MutexLock class is a convenience class that facilitates
// locking a mutex on function entry and automatically unlocking
// it on function exit.
//
class KFS::MutexLock {
public:
    /**
     * Acquire the mutex
     */
    MutexLock( pthread_mutex_t *mutex ) : mMutex(mutex) 
    { pthread_mutex_lock(mMutex); }

    /**
     * Release the mutex automatically
     */
    ~MutexLock()
    { int rval = pthread_mutex_unlock(mMutex); assert(!rval); (void)rval; }

private:
    pthread_mutex_t *mMutex;
};

#endif // LIBKFSCLIENT_CONCURRENCY_H
