//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2007/09/26
//
// Author: Sriram Rao (Kosmix Corp.) 
//
// Copyright 2007 Kosmix Corp.
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
// \brief A simple shell that lets users navigate KFS directory hierarchy.
// 
//----------------------------------------------------------------------------

#ifndef TOOLS_KFSSHELL_H
#define TOOLS_KFSSHELL_H

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
}

#include <string>
#include <vector>

namespace KFS
{
    namespace tools
    {

        typedef void (*cmdHandler)(const std::vector<std::string> &args);

        // cmd handlers
        void handleCd(const std::vector<std::string> &args);
        void handleChangeReplication(const std::vector<std::string> &args);
        void handleCopy(const std::vector<std::string> &args);
        void handleLs(const std::vector<std::string> &args);
        void handleMkdirs(const std::vector<std::string> &args);
        void handleMv(const std::vector<std::string> &args);
        void handleRmdir(const std::vector<std::string> &args);
        void handlePing(const std::vector<std::string> &args);
        void handleRm(const std::vector<std::string> &args);    

        // utility functions
        bool doMkdirs(const char *path);
        bool doRmdir(const char *dirname);
    }
}

#endif // TOOLS_KFSSHELL_H
