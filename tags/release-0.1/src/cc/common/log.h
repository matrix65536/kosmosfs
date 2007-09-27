//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// \brief Log library wrapper.
//
// Created 2005/03/01
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
//----------------------------------------------------------------------------

#ifndef COMMON_LOG_H
#define COMMON_LOG_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE // for asprintf; this #define must precede <stdio.h>
#endif

#ifdef __cplusplus
#include <cstdio>
#include <ctime>
#else
#include <stdio.h>
#include <time.h>
#endif
#include <pthread.h>

#ifndef _KFS_LOG_USE_NAMESPACE_STD
#ifdef __cplusplus
#define _KFS_LOG_USE_NAMESPACE_STD	using namespace std
#else
#define _KFS_LOG_USE_NAMESPACE_STD
#endif
#endif

/*
 * This file uses C99 variadic macros, which are not part of ISO C++ (1998)
 * but are supported in gcc as an extension.
 * To avoid a gcc warning to this effect, use -Wno-variadic-macros
 * when compiling code using this header file.
 */
#pragma GCC system_header

namespace KFS 
{

extern pthread_mutex_t _kosmosfs_log_internal_stdio_lock;

typedef int KosmosFSLogLevel;
extern const KosmosFSLogLevel KFS_LOG_ERROR_LEVEL;
extern const KosmosFSLogLevel KFS_LOG_WARN_LEVEL;
extern const KosmosFSLogLevel KFS_LOG_INFO_LEVEL;
extern const KosmosFSLogLevel KFS_LOG_DEBUG_LEVEL;
extern const KosmosFSLogLevel KFS_LOG_INSANE_LEVEL;
extern KosmosFSLogLevel kosmosfs_log_level;

/*
 * How logging systems handle log levels:
 * log4j's levels: TRACE DEBUG INFO WARN ERROR FATAL
 * RLog's levels: DEBUG INFO WARNING ERROR
 * ACE's levels: DEBUG INFO NOTICE WARNING ERROR CRITICAL ALERT EMERGENCY
 * syslog's levels: DEBUG INFO NOTICE WARNING ERR CRIT ALERT EMERG
*/

/** Internal placeholder log-to-stderr function. */
#ifndef _KFS_LOG_INTERNAL
#define _KFS_LOG_INTERNAL(level, ...) \
  { \
    _KFS_LOG_USE_NAMESPACE_STD; \
    time_t now = std::time(NULL); \
    char nowString[26]; \
    ctime_r(&now, &nowString[0]); \
    nowString[strlen(nowString)-1] = '\0'; \
    pthread_mutex_lock(&_kosmosfs_log_internal_stdio_lock); \
    fprintf(stderr, "%s (%lu) %s: %s:%d:%s: ", nowString, pthread_self(), level, \
      __FILE__, __LINE__, __func__); \
    fprintf(stderr, __VA_ARGS__); \
    fputc('\n', stderr); \
    pthread_mutex_unlock(&_kosmosfs_log_internal_stdio_lock); \
  }
#endif

// A logging library will have a macro wrapper, so
// these are macro wrappers too.
#ifndef KFS_LOG_INSANE
/** Print a insane message, using printf-style varargs. */
#define KFS_LOG_INSANE(...) \
	do { \
         if (kosmosfs_log_level >= KFS_LOG_INSANE_LEVEL) \
            _KFS_LOG_INTERNAL("INSANE", __VA_ARGS__) \
        } while(0)
#endif

#ifndef KFS_LOG_DEBUG
/** Print a debug message, using printf-style varargs. */
#define KFS_LOG_DEBUG(...) \
	do { \
         if (kosmosfs_log_level >= KFS_LOG_DEBUG_LEVEL) \
            _KFS_LOG_INTERNAL("DEBUG", __VA_ARGS__) \
        } while(0)
#endif

#ifndef KFS_LOG_INFO
/** Print an info message, using printf-style varargs. */
#define KFS_LOG_INFO(...) \
	do { \
         if (kosmosfs_log_level >= KFS_LOG_INFO_LEVEL) \
            _KFS_LOG_INTERNAL("INFO", __VA_ARGS__) \
        } while(0)
#endif

#ifndef KFS_LOG_WARN
/** Print a warn message, using printf-style varargs. */
#define KFS_LOG_WARN(...) \
	do { \
         if (kosmosfs_log_level >= KFS_LOG_WARN_LEVEL) \
            _KFS_LOG_INTERNAL("WARN", __VA_ARGS__) \
        } while(0)
#endif

#ifndef KFS_LOG_ERROR
/** Print an error message, using printf-style varargs. */
#define KFS_LOG_ERROR(...) \
	do { \
         if (kosmosfs_log_level >= KFS_LOG_ERROR_LEVEL) \
            _KFS_LOG_INTERNAL("ERROR", __VA_ARGS__) \
        } while(0)
#endif

#ifndef KFS_LOG_PERF
/** Print an perf log entry, using printf-style varargs. */
#define KFS_LOG_PERF(...) \
  { \
    _KFS_LOG_INTERNAL("PERF", __VA_ARGS__) \
  }
#endif

}

#endif // COMMON_LOG_H
