//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/09/21
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
// \brief Tool that implements rmdir <path>
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
}

#include "libkfsClient/KfsClient.h"
#include "tools/KfsShell.h"

using std::cout;
using std::endl;
using std::vector;
using std::string;

using namespace KFS;
using namespace KFS::tools;

void
KFS::tools::handleRmdir(const vector<string> &args)
{
    if ((args.size() < 1) || (args[0] == "--help") || (args[0] == "")) {
        cout << "Usage: rmdir <dir> " << endl;
        return;
    }

     doRmdir(args[0].c_str());
}
