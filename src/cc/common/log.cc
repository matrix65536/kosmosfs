//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: log.cc $
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

pthread_mutex_t _cosmix_log_internal_stdio_lock = PTHREAD_MUTEX_INITIALIZER;

const CosmixLogLevel COSMIX_LOG_INSANE_LEVEL = 5;
const CosmixLogLevel COSMIX_LOG_DEBUG_LEVEL = 4;
const CosmixLogLevel COSMIX_LOG_INFO_LEVEL = 3;
const CosmixLogLevel COSMIX_LOG_WARN_LEVEL = 2;
const CosmixLogLevel COSMIX_LOG_ERROR_LEVEL = 1;

class CosmixLog {
public:
    static CosmixLogLevel getloglevel() {

#ifdef NDEBUG
        // Omit COSMIX_LOG_DEBUG by default for NDEBUG code.
        CosmixLogLevel level = COSMIX_LOG_INFO_LEVEL;
#else
        CosmixLogLevel level = COSMIX_LOG_DEBUG_LEVEL;
#endif

        char *env = getenv("COSMIX_LOG_LEVEL");
        if (env == NULL) return level;
        level = (CosmixLogLevel)strtol(env, NULL, 0);
        if (level <= 0 || level > 5) return level;
        return level;
    }
};

CosmixLogLevel cosmix_log_level = CosmixLog::getloglevel();
