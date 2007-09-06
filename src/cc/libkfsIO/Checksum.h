//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/Checksum.h#3 $
//
// Created 2006/09/12
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
// Code for computing 32-bit Adler checksums
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_CHECKSUM_H
#define CHUNKSERVER_CHECKSUM_H

#include <vector>
#include "libkfsIO/IOBuffer.h"

/// Checksums are computed on 64KB block boundaries.  We use the
/// "rolling" 32-bit Adler checksum algorithm
const uint32_t CHECKSUM_BLOCKSIZE = 65536;

const uint32_t MOD_ADLER = 65521;

extern uint32_t OffsetToChecksumBlockNum(off_t offset);

extern uint32_t OffsetToChecksumBlockStart(off_t offset);

extern uint32_t OffsetToChecksumBlockEnd(off_t offset);

/// Call this function if you want checksum computed over CHECKSUM_BLOCKSIZE bytes
extern uint32_t ComputeBlockChecksum(IOBuffer *data, size_t len);

/// Call this function if you want a checksums for a sequence of CHECKSUM_BLOCKSIZE bytes
extern std::vector<uint32_t> ComputeChecksums(IOBuffer *data, size_t len);


#endif // CHUNKSERVER_CHECKSUM_H
