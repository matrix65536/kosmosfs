//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/meta/ChunkServerFactory.cc#3 $
//
// Created 2006/09/29
// Author: Sriram Rao (Kosmix Corp.)
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
// \brief ChunkServerFactory code to remove a dead server.
//----------------------------------------------------------------------------

#include "ChunkServerFactory.h"
#include <algorithm>
using std::find_if;

using namespace KFS;

void
ChunkServerFactory::RemoveServer(const ChunkServer *target)
{
	list<ChunkServerPtr>::iterator i;

	i = find_if(mChunkServers.begin(), mChunkServers.end(),
			ChunkServerMatcher(target));
	if (i != mChunkServers.end()) {
		mChunkServers.erase(i);
	}
}

