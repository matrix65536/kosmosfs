//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// Created 2005/03/01
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
//----------------------------------------------------------------------------

#include "log.h"
#include <stdlib.h>
#include <log4cpp/RollingFileAppender.hh>
#include <log4cpp/OstreamAppender.hh>
#include <log4cpp/SimpleLayout.hh>

using namespace KFS;

log4cpp::Category* KFS::MsgLogger::logger = NULL;

void
MsgLogger::Init(const char *filename, log4cpp::Priority::Value priority)
{
    log4cpp::Appender* appender;
    log4cpp::Layout* layout = new log4cpp::SimpleLayout();

    if (filename != NULL)
        appender = new log4cpp::RollingFileAppender("default", std::string(filename));
    else
        appender = new log4cpp::OstreamAppender("default", &std::cout);

    appender->setLayout(layout);

    logger = &(log4cpp::Category::getInstance(std::string("kfs")));
    logger->addAppender(appender);
    logger->setAdditivity(false);
    logger->setPriority(priority);
}

void MsgLogger::SetLevel(log4cpp::Priority::Value priority) {
    logger->setPriority(priority);
}
