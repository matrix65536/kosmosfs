//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/09/12
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
// An adaptation of the 32-bit Adler checksum algorithm
//
//----------------------------------------------------------------------------

#include "libkfsIO/Chunk.h"
#include "Checksum.h"

#include <algorithm>
#include <vector>
using std::min;
using std::vector;

uint32_t
OffsetToChecksumBlockNum(off_t offset)
{
    return offset / CHECKSUM_BLOCKSIZE;
}

uint32_t
OffsetToChecksumBlockStart(off_t offset)
{
    return (offset / CHECKSUM_BLOCKSIZE) *
        CHECKSUM_BLOCKSIZE;
}

uint32_t
OffsetToChecksumBlockEnd(off_t offset)
{
    return ((offset / CHECKSUM_BLOCKSIZE) + 1) *
        CHECKSUM_BLOCKSIZE;
}

///
/// The checksum computation code is adapted from Wikipedia.  
/// See: http://en.wikipedia.org/wiki/Adler-32
/// Given a block of data, compute its checksum.
///
uint32_t
ComputeBlockChecksum(IOBuffer *data, size_t len)
{
    uint32_t a = 1, b = 0;
    int count = 0;

    for (list<IOBufferDataPtr>::iterator iter = data->mBuf.begin();
         len > 0 && (iter != data->mBuf.end()); ++iter) {
        IOBufferDataPtr blk = *iter;
        char *buf = blk->Consumer();
        unsigned tlen = min((size_t) blk->BytesConsumable(), len);

        if (tlen == 0)
            continue;

        len -= tlen;

        do {
            a += *buf++;
            b += a;
            ++count;
            // to avoid overflows, compute a/b values over a 4k range
            if ((count % 4096) == 0) {
                a = (a & 0xffff) + (a >> 16) * (65536-MOD_ADLER);
                b = (b & 0xffff) + (b >> 16) * (65536-MOD_ADLER);
                count = 0;
            }
        } while (--tlen);
    }
    if (count != 0) {
        // spill...
        a = (a & 0xffff) + (a >> 16) * (65536-MOD_ADLER);
        b = (b & 0xffff) + (b >> 16) * (65536-MOD_ADLER);
    }

    if (a >= MOD_ADLER)
        a -= MOD_ADLER;
    b = (b & 0xffff) + (b >> 16) * (65536-MOD_ADLER);
    if (b >= MOD_ADLER)
        b -= MOD_ADLER;
    return (b << 16) | a;
}

///
/// Given a sequence of CHECKSUM_BLOCKSIZE blocks, compute checksum on
/// individual CHECKSUM_BLOCKSIZE's and return them.
///
vector<uint32_t>
ComputeChecksums(IOBuffer *data, size_t len)
{
    vector<uint32_t> cksums;
    list<IOBufferDataPtr>::iterator iter = data->mBuf.begin();

    assert(len >= CHECKSUM_BLOCKSIZE);

    if (iter == data->mBuf.end())
        return cksums;

    IOBufferDataPtr blk = *iter;
    char *buf = blk->Consumer();

    /// Compute checksum block by block
    while ((len > 0) && (iter != data->mBuf.end())) {
        uint32_t a = 1, b = 0;
        int count = 0;
        size_t currLen = 0;

        while (currLen < CHECKSUM_BLOCKSIZE) {
            unsigned navail = min((size_t) (blk->Producer() - buf), len);
            if (currLen + navail > CHECKSUM_BLOCKSIZE)
                navail = CHECKSUM_BLOCKSIZE - currLen;

            if (navail == 0) {
                iter++;
                if (iter == data->mBuf.end())
                    break;
                blk = *iter;
                buf = blk->Consumer();
                continue;
            }

            currLen += navail;
            len -= navail;

            do {
                a += *buf++;
                b += a;
                ++count;
                // to avoid overflows, compute a/b values over a 4k range
                if ((count % 4096) == 0) {
                    a = (a & 0xffff) + (a >> 16) * (65536-MOD_ADLER);
                    b = (b & 0xffff) + (b >> 16) * (65536-MOD_ADLER);
                    count = 0;
                }
            } while (--navail);
        }
        if (count != 0) {
            // spill...
            a = (a & 0xffff) + (a >> 16) * (65536-MOD_ADLER);
            b = (b & 0xffff) + (b >> 16) * (65536-MOD_ADLER);
        }

        if (a >= MOD_ADLER)
            a -= MOD_ADLER;
        b = (b & 0xffff) + (b >> 16) * (65536-MOD_ADLER);
        if (b >= MOD_ADLER)
            b -= MOD_ADLER;
        cksums.push_back((b << 16) | a);
    }
    return cksums;
}
