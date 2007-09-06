//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: log.cc $
//
// Created 2005/03/01
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
