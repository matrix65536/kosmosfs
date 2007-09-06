//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/libkfsIO/IOBuffer.cc#3 $
//
// Created 2006/03/15
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
// 
//----------------------------------------------------------------------------

#include <cerrno>
#include <unistd.h>

#include <iostream>
#include <algorithm>
using std::min;

#include "IOBuffer.h"
#include "Globals.h"
using namespace libkfsio;

IOBufferData::IOBufferData()
{
    // cout << "Allocating: " << this << endl;

    mData = new char [IOBUFSIZE];
    mStart = mData;
    mEnd = mStart + IOBUFSIZE;
    mProducer = mConsumer = mData;
}

IOBufferData::~IOBufferData()
{
    // cout << "Deleting: " << this << endl;

    delete [] mData;
    mProducer = mConsumer = NULL;
}

int IOBufferData::ZeroFill(int nbytes)
{
    int fillAvail = mEnd - mProducer;

    if (fillAvail < nbytes)
        nbytes = fillAvail;

    memset(mProducer, '\0', nbytes);
    return Fill(nbytes);
}

int IOBufferData::Fill(int nbytes)
{
    int fillAvail = mEnd - mProducer;

    if (nbytes > fillAvail) {
        mProducer = mEnd;
        return fillAvail;
    }
    mProducer += nbytes; 
    assert(mProducer <= mEnd);
    return nbytes;
}

int IOBufferData::Consume(int nbytes)
{
    int consumeAvail = mProducer - mConsumer;

    if (nbytes > consumeAvail) {
        mConsumer = mProducer;
        return consumeAvail;
    }
    mConsumer += nbytes; 
    assert(mConsumer <= mProducer);
    return nbytes;
}

int IOBufferData::Trim(int nbytes)
{
    int bytesAvail = mProducer - mConsumer;

    // you can't trim and grow the data in the buffer
    if (bytesAvail < nbytes)
        return bytesAvail;

    mProducer = mConsumer + nbytes;
    return nbytes;
}

int IOBufferData::Read(int fd)
{
    int numBytes = mEnd - mProducer;
    int nread;

    assert(numBytes > 0);

    if (numBytes <= 0)
        return 0;

    nread = read(fd, mProducer, numBytes);
    
    if (nread > 0) {
        mProducer += nread;
        globals().ctrNetBytesRead.Update(nread);
    }
    return nread;
}

int IOBufferData::Write(int fd)
{
    int numBytes = mProducer - mConsumer;
    int nwrote;

    assert(numBytes > 0);

    if (numBytes <= 0)
        return 0;

    nwrote = write(fd, mConsumer, numBytes);

    if (nwrote > 0) {
        mConsumer += nwrote;
        globals().ctrNetBytesWritten.Update(nwrote);
    }

    return nwrote;
}

int IOBufferData::CopyIn(const char *buf, int numBytes)
{
    int bytesToCopy = mEnd - mProducer;

    if (bytesToCopy < numBytes) {
        memcpy(mProducer, buf, bytesToCopy);
        Fill(bytesToCopy);
        return bytesToCopy;
    } else {
        memcpy(mProducer, buf, numBytes);
        Fill(numBytes);
        return numBytes;
    }
}

int IOBufferData::CopyIn(const IOBufferData *other, int numBytes)
{
    int bytesToCopy = mEnd - mProducer;

    if (bytesToCopy < numBytes) {
        memcpy(mProducer, other->mConsumer, bytesToCopy);
        Fill(bytesToCopy);
        return bytesToCopy;
    } else {
        memcpy(mProducer, other->mConsumer, numBytes);
        Fill(numBytes);
        return numBytes;
    }
}

int IOBufferData::CopyOut(char *buf, int numBytes)
{
    int bytesToCopy = mProducer - mConsumer;

    assert(bytesToCopy >= 0);

    if (bytesToCopy <= 0) {
        return 0;
    }

    if (bytesToCopy > numBytes)
        bytesToCopy = numBytes;

    memcpy(buf, mConsumer, bytesToCopy);
    return bytesToCopy;
}

IOBuffer::IOBuffer()
{

}

IOBuffer::~IOBuffer()
{

}

void IOBuffer::Append(IOBufferDataPtr &buf)
{
    mBuf.push_back(buf);
}

void IOBuffer::Append(IOBuffer *ioBuf)
{
    list<IOBufferDataPtr>::iterator iter;
    IOBufferDataPtr data;

    for (iter = ioBuf->mBuf.begin(); iter != ioBuf->mBuf.end(); iter++) {
        data = *iter;
        mBuf.push_back(data);
    }
    ioBuf->mBuf.clear();
}

void IOBuffer::Move(IOBuffer *other, int numBytes)
{
    list<IOBufferDataPtr>::iterator iter;
    IOBufferDataPtr data, dataCopy;
    int bytesMoved = 0;

    assert(other->BytesConsumable() >= numBytes);

    iter = other->mBuf.begin();
    while ((iter != other->mBuf.end()) &&
           (bytesMoved < numBytes)) {
        data = *iter;
        if (data->BytesConsumable() + bytesMoved < numBytes) {
            other->mBuf.pop_front();
            bytesMoved += data->BytesConsumable();
            mBuf.push_back(data);
        } else {
            // this is the last buffer being moved; only partial data
            // from the buffer needs to be moved and so copy it over.
            int bytesToCopy = numBytes - bytesMoved;
            dataCopy.reset(new IOBufferData());
            dataCopy->CopyIn(data.get(), bytesToCopy);
            mBuf.push_back(dataCopy);
            other->Consume(bytesToCopy);
            bytesMoved += bytesToCopy;
            assert(bytesMoved >= numBytes);
        }
        iter = other->mBuf.begin();
    }
}

void IOBuffer::Splice(IOBuffer *other, int offset, int numBytes)
{
    list<IOBufferDataPtr>::iterator iter, insertPt = mBuf.begin();
    IOBufferDataPtr data, dataCopy;
    int startPos = 0, extra;

    extra = offset - BytesConsumable();
    while (extra > 0) {
        int zeroed = min(IOBUFSIZE, extra);
        data.reset(new IOBufferData());
        data->ZeroFill(zeroed);
        extra -= zeroed;
        mBuf.push_back(data);
    }
    assert(BytesConsumable() >= offset);

    assert(other->BytesConsumable() >= numBytes);

    // find the insertion point
    iter = mBuf.begin();
    while ((iter != mBuf.end()) &&
           (startPos < offset)) {
        data = *iter;
        if (data->BytesConsumable() + startPos > offset) {
            int bytesToCopy = offset - startPos;

            dataCopy.reset(new IOBufferData());

            dataCopy->CopyIn(data.get(), bytesToCopy);
            data->Consume(bytesToCopy);
            mBuf.insert(iter, dataCopy);
            startPos += dataCopy->BytesConsumable();
        } else {
            startPos += data->BytesConsumable();
            ++iter;
        }
        insertPt = iter;
    }

    // get rid of stuff between [offset...offset+numBytes]
    while ((iter != mBuf.end()) &&
           (startPos < offset + numBytes)) {
        data = *iter;
        extra = data->BytesConsumable();
        if (startPos + extra > offset + numBytes) {
            extra = offset + numBytes - startPos;
        }
        data->Consume(extra);
        startPos += extra;
        ++iter;
    }

    // now, put the thing at insertPt
    if (insertPt != mBuf.end())
        mBuf.splice(insertPt, other->mBuf);
    else {
        iter = other->mBuf.begin();
        while (iter != other->mBuf.end()) {
            data = *iter;
            mBuf.push_back(data);
            other->mBuf.pop_front();
            iter = other->mBuf.begin();            
        }
    }
}

void IOBuffer::ZeroFill(int numBytes)
{
    IOBufferDataPtr data;

    while (numBytes > 0) {
        int zeroed = min(IOBUFSIZE, numBytes);
        data.reset(new IOBufferData());
        data->ZeroFill(zeroed);
        numBytes -= zeroed;
        mBuf.push_back(data);
    }

}

int IOBuffer::Read(int fd)
{
    IOBufferDataPtr data;
    int numRead = 0, res;

    if (mBuf.empty()) {
        data.reset(new IOBufferData());
        mBuf.push_back(data);
    }

    while (1) {
        data = mBuf.back();
        
        if (data->IsFull()) {
            data.reset(new IOBufferData());
            mBuf.push_back(data);
        }
        res = data->Read(fd);
        if (res <= 0)
            break;
        numRead += res;
    }
    return numRead;
}


int IOBuffer::Write(int fd)
{
    int res, numWrote = 0;
    IOBufferDataPtr data;

    while (!mBuf.empty()) {
        data = mBuf.front();
        if (data->IsEmpty()) {
            mBuf.pop_front();
	    continue;
        }
        assert(data->BytesConsumable() > 0);

        res = data->Write(fd);
        if (res <= 0)
            break;

        numWrote += res;
    }
    return numWrote;
}

int
IOBuffer::BytesConsumable()
{
    list<IOBufferDataPtr>::iterator iter;
    IOBufferDataPtr data;
    int numBytes = 0;

    for (iter = mBuf.begin(); iter != mBuf.end(); iter++) {
        data = *iter;
        numBytes += data->BytesConsumable();
    }
    return numBytes;
}

void IOBuffer::Consume(int nbytes)
{
    list<IOBufferDataPtr>::iterator iter;
    IOBufferDataPtr data;
    int bytesConsumed;

    assert(nbytes >= 0);
    iter = mBuf.begin();
    while (iter != mBuf.end()) {
        data = *iter;
        bytesConsumed = data->Consume(nbytes);
        nbytes -= bytesConsumed;
        if (nbytes <= 0)
            break;
        if (data->IsEmpty())
            mBuf.pop_front();
        iter = mBuf.begin();
    }
    assert(nbytes == 0);
}

void IOBuffer::Trim(int nbytes)
{
    list<IOBufferDataPtr>::iterator iter;
    IOBufferDataPtr data;
    int bytesAvail, totBytes = 0;

    if (nbytes <= 0)
        return;

    iter = mBuf.begin();
    while (iter != mBuf.end()) {
        data = *iter;
        bytesAvail = data->BytesConsumable();
        if (bytesAvail + totBytes <= nbytes) {
            totBytes += bytesAvail;
            ++iter;
            continue;
        }
        if (totBytes == nbytes)
            break;

        data->Trim(nbytes - totBytes);
        ++iter;
        break;
    }
    
    while (iter != mBuf.end()) {
        data = *iter;
        data->Consume(data->BytesConsumable());
        ++iter;
    }
    assert(BytesConsumable() == nbytes);
}

int IOBuffer::CopyIn(const char *buf, int numBytes)
{
    IOBufferDataPtr data;
    int numCopied = 0, bytesCopied;

    if (mBuf.empty()) {
    	data.reset(new IOBufferData());
	mBuf.push_back(data);
    } else {
        data = mBuf.back();
    }

    while (numCopied < numBytes) {
        bytesCopied = data->CopyIn(buf + numCopied, 
                                   numBytes - numCopied);
        numCopied += bytesCopied;
        if (numCopied >= numBytes)
            break;
    	data.reset(new IOBufferData());
	mBuf.push_back(data);
    }

    return numCopied;
}

int IOBuffer::CopyOut(char *buf, int numBytes)
{
    list<IOBufferDataPtr>::iterator iter;
    IOBufferDataPtr data;
    char *curr = buf;
    int nread = 0, copied;

    buf[0] = '\0';
    for (iter = mBuf.begin(); iter != mBuf.end(); iter++) {
        data = *iter;
        copied = data->CopyOut(curr, numBytes - nread);
        assert(copied >= 0);
        curr += copied;
        nread += copied;
        assert(curr <= buf + numBytes);

        if (nread >= numBytes)
            break;
    }

    return nread;
}
