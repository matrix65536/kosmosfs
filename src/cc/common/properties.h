//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: properties.h $
//
// \brief Properties file similar to java.util.Properties
//
// Created 2004/05/05
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
//----------------------------------------------------------------------------

#ifndef COMMON_PROPERTIES_H
#define COMMON_PROPERTIES_H
  
#include <istream>
#include <string>
#include <map>

namespace KFS
{

class Properties {

  private :
    //Map that holds the (key,value) pairs
    std::map<std::string, std::string> * propmap; 
    std::string removeLTSpaces(std::string);

  public  :
    // load the properties from a file
    int loadProperties(const char* fileName, char delimiter, bool verbose, bool multiline = false);
    // load the properties from an in-core buffer
    int loadProperties(std::istream &ist, char delimiter, bool verbose, bool multiline = false);
    std::string getValue(std::string key, std::string def) const;
    const char* getValue(std::string key, const char* def) const;
    int getValue(std::string key, int def);
    long getValue(std::string key, long def);
    long long getValue(std::string key, long long def);
    uint64_t getValue(std::string key, uint64_t def);
    double getValue(std::string key, double def);   
    void setValue(const std::string key, const std::string value);
    void getList(std::string &outBuf, std::string linePrefix) const;
    Properties();
    Properties(const Properties &p);
    ~Properties();

};

}

#endif // COMMON_PROPERTIES_H
