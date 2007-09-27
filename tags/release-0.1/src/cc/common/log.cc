//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
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

#include "log.h"
#include <stdlib.h>

using namespace KFS;

pthread_mutex_t KFS::_kosmosfs_log_internal_stdio_lock = PTHREAD_MUTEX_INITIALIZER;

const KosmosFSLogLevel KFS::KFS_LOG_INSANE_LEVEL = 5;
const KosmosFSLogLevel KFS::KFS_LOG_DEBUG_LEVEL = 4;
const KosmosFSLogLevel KFS::KFS_LOG_INFO_LEVEL = 3;
const KosmosFSLogLevel KFS::KFS_LOG_WARN_LEVEL = 2;
const KosmosFSLogLevel KFS::KFS_LOG_ERROR_LEVEL = 1;

class KosmosFSLog {
public:
    static KosmosFSLogLevel getloglevel() {

#ifdef NDEBUG
        // Omit KFS_LOG_DEBUG by default for NDEBUG code.
        KosmosFSLogLevel level = KFS_LOG_INFO_LEVEL;
#else
        KosmosFSLogLevel level = KFS_LOG_DEBUG_LEVEL;
#endif

        char *env = getenv("KFS_LOG_LEVEL");
        if (env == NULL) return level;
        level = (KosmosFSLogLevel)strtol(env, NULL, 0);
        if (level <= 0 || level > 5) return level;
        return level;
    }
};

KosmosFSLogLevel KFS::kosmosfs_log_level = KosmosFSLog::getloglevel();
