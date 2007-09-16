//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2006/06/20
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
// \file Logger.h
// \brief Code for handling logging between checkpoints
//
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_LOGGER_H
#define CHUNKSERVER_LOGGER_H

#include <fstream>
#include <string>
using std::ofstream;
using std::string;

#include "libkfsIO/ITimeout.h"
#include "libkfsIO/Chunk.h"
#include "libkfsIO/NetManager.h"
#include "KfsOps.h"

#include "meta/queue.h"
#include "meta/thread.h"
using namespace KFS;

class LoggerTimeoutImpl;

///
/// Between a pair of checkpoints, the operations at the chunk server
/// relating to allocate/delete chunks as well as writes to chunks are
/// logged.  The logs are stored at: <logDir>/logs
///
class Logger {
public:
    Logger();
    ~Logger();

    void Init(const char *logDir);

    /// Set up for logging
    void Start();

    /// The main loop for the logger thread.  It pulls requests that
    /// have been submitted for logging and logs them.
    void MainLoop();

    /// Submit a request for logging.  This is called by the main
    /// thread and the request is sent down to the logger thread.
    /// @param[in] op  The op that needs to be logged
    void Submit(KfsOp *op);

    /// This is called by the main thread to pull requests that have
    /// been logged.  Processing for the logged requests resumes.
    void Dispatch();


    /// Restore state from checkpoint/log after a shutdown
    void Restore();


private:
    /// Version # to be written out in the ckpt file
    static const int KFS_VERSION = 1;

    /// The path to the directory for writing out logs
    const char *mLogDir;
    /// The name of the log file
    string mLogFilename;
    /// counter that tracks the generation # of the log file
    long long mLogGenNum;

    /// The handle to the log file
    ofstream mFile;
    /// pending ops that need to be logged
    MetaQueue<KfsOp> mPending;
    /// ops for which logging is done
    MetaQueue<KfsOp> mLogged;
    /// thread that does the logging and flushes the log file to disk
    MetaThread mWorker;
    /// Timer object to pull out logged requests and dispatch them
    LoggerTimeoutImpl *mLoggerTimeoutImpl;

    /// Write out a checkpoint and then rotate the logs.
    /// @param[in] op  The checkpoint op that contains data to be
    /// written out.
    void Checkpoint(KfsOp *op);

    /// Rotate the logs whenever the system takes a checkpoint
    void RotateLog();

    /// Helper function that builds the log file's name using the generation #
    /// @retval The name of the log file that includes the generation #
    string MakeLogFilename();

    /// Helper function that builds the ckpt file's name using the generation #
    /// @retval The name of the ckpt file that includes the generation #
    string MakeCkptFilename();

    /// Helper function that builds the "latest" ckpt file's name
    /// @retval The name of the "latest" ckpt file
    string MakeLatestCkptFilename();

    /// Given a line from a checkpoint file, parse out the ChunkInfo_t
    /// structure from it.
    /// @param[in] line  The line read from the checkpoint file
    /// @param[out] entry  The fields extracted from the line
    /// @retval true if parse is successful; false otherwise
    ///
    bool ParseCkptEntry(const char *line, ChunkInfo_t &entry);

    /// Replay the log after a dirty shutdown
    void ReplayLog();

};

/// A Timeout interface object for pulling out the logged requests and
/// dispatching them
class LoggerTimeoutImpl : public ITimeout {
public:
    LoggerTimeoutImpl(Logger *log) {
        mLogger = log;
        // set a checkpoint once every min.
        // SetTimeoutInterval(60*1000);
    };
    /// On a timeout, pull out whatever is logged
    void Timeout() {
        mLogger->Dispatch();
    };
private:
    /// Owning logger object
    Logger	*mLogger;
};

extern Logger gLogger;

#endif // CHUNKSERVER_LOGGER_H
