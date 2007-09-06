/*!
 * $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/thread.h#3 $
 *
 * Copyright (C) 2006 Kosmix Corp.
 * Author: Blake Lewis (Kosmix Corp.)
 *
 * This file is part of Kosmos File System (KFS).
 *
 * KFS is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation under version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 * \file thread.h
 * \brief thread control for KFS metadata server
 */
#if !defined(KFS_THREAD_H)
#define KFS_THREAD_H

#include <cassert>
#include "common/config.h"

extern "C" {
#include <pthread.h>
}

namespace KFS {

class MetaThread {
	pthread_mutex_t mutex;
	pthread_cond_t cv;
	pthread_t thread;
	static const pthread_t NO_THREAD = -1u;
public:
	typedef void *(*thread_start_t)(void *);
	MetaThread(): thread(NO_THREAD)
	{
		pthread_mutex_init(&mutex, NULL);
		pthread_cond_init(&cv, NULL);
	}
	~MetaThread()
	{
		pthread_mutex_destroy(&mutex);
                if (thread != NO_THREAD) {
                    int UNUSED_ATTR status = pthread_cancel(thread);
                    assert(status == 0);
                }
		pthread_cond_destroy(&cv);
	}
	void lock()
	{
		int UNUSED_ATTR status = pthread_mutex_lock(&mutex);
		assert(status == 0);
	}
	void unlock()
	{
		int UNUSED_ATTR status = pthread_mutex_unlock(&mutex);
		assert(status == 0);
       	}
	void wakeup()
	{
		int UNUSED_ATTR status = pthread_cond_broadcast(&cv);
		assert(status == 0);
	}
	void sleep()
	{
		int UNUSED_ATTR status = pthread_cond_wait(&cv, &mutex);
		assert(status == 0);
	}
	void start(thread_start_t func, void *arg)
	{
		int UNUSED_ATTR status;
		status = pthread_create(&thread, NULL, func, arg);
		assert(status == 0);
	}
	void stop()
	{
		int UNUSED_ATTR status = pthread_cancel(thread);
		assert(status == 0);
	}
};

}

#endif // !defined(KFS_THREAD_H)
