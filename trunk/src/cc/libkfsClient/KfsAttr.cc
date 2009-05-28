//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/04/23
// Author: Jakub Schmidtke
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

#include "KfsAttr.h"

#include <cstring>
#include <algorithm>

using namespace KFS;

void
ChunkAttr::AvoidServer ( ServerLocation &loc )
{
    vector<ServerLocation>::iterator iter;

    iter = std::find ( chunkServerLoc.begin(), chunkServerLoc.end(), loc );

    if ( iter != chunkServerLoc.end() )
        chunkServerLoc.erase ( iter );

    if ( chunkServerLoc.size() == 0 )
    {
        // all the servers we could talk to are gone; so, we need
        // to re-figure where the data is.
        chunkId = -1;
    }
}

void
KfsFileStat::safeConvert(struct stat & std_stat, kfsOff_t & std_size)
{
    memset ( &std_stat, 0, sizeof ( std_stat ) );

    std_stat.st_mode = mode;
    std_stat.st_atime = atime;
    std_stat.st_mtime = mtime;
    std_stat.st_ctime = ctime;

    std_size = size;
}
