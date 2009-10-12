//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/10/28
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
// \brief Tool for listing directory contents (ala ls -l).
// 
//----------------------------------------------------------------------------

#include <iostream>    
#include <fstream>
#include <cerrno>
#include <ostream>

extern "C" {
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
}

#include "libkfsClient/KfsClient.h"
#include "tools/KfsShell.h"
#include "tools/KfsToolsCommon.h"

using std::cout;
using std::endl;
using std::ofstream;
using std::ostringstream;
using std::vector;

using namespace KFS;
using namespace KFS::tools;

// may want to do "ls -r"
int
KFS::tools::handleLs(const vector<string> &args)
{
    bool longMode = false, humanReadable = false, timeInSecs = false;
    vector<string>::size_type pathIndex = 0;

    if ((args.size() >= 1) && (args[0] == "--help")) {
        cout << "Usage: ls {-lht} {<dir>} " << endl;
        return 0;
    }
    
     KfsClientPtr kfsClient = getKfsClientFactory()->GetClient();

    if (!kfsClient)
    {
	std::cerr << "Error: No default KFS client!\n";
	return 0;
    }

    if (args.size() >= 1) {
        if (args[0][0] == '-') {
            pathIndex = 1;
            for (uint32_t i = 1; i < args[0].size(); i++) {
                switch (args[0][i]) {
                    case 'l':
                        longMode = true;
                        break;
                    case 'h':
                        humanReadable = true;
                        break;
                    case 't':
                        timeInSecs = true;
                        break;
                }
            }
        }
    }

    if (args.size() > pathIndex)
        return DirList(kfsClient, args[pathIndex], longMode, humanReadable, timeInSecs);
    else
        return DirList(kfsClient, ".", longMode, humanReadable, timeInSecs);
}
