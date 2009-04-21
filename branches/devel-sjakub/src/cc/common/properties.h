//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// \brief Properties file similar to java.util.Properties
//
// Created 2004/05/05
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
//----------------------------------------------------------------------------

#ifndef COMMON_PROPERTIES_H
#define COMMON_PROPERTIES_H
  
#include <istream>
#include <string>
#include <map>
#include <typeinfo>
#include <sstream>

namespace KFS
{

class Properties {

  private :
    //Map that holds the (key,value) pairs
    std::map<std::string, std::string> * propmap; 
    std::string removeLTSpaces(std::string);

  public  :
    
    Properties();
    Properties(const Properties &p);
    ~Properties();
    
    // load the properties from a file
    int loadProperties(const char * fileName, char delimiter, bool verbose, bool multiline = false);
    
    // load the properties from an in-core buffer
    int loadProperties(std::istream &ist, char delimiter, bool verbose, bool multiline = false);
    
    void getList(std::string &outBuf, const std::string & linePrefix) const;
    
    template<typename T> T getValue ( const std::string & key, const T & defaultValue ) const
    {
      std::map<std::string, std::string>::const_iterator it = propmap->find ( key );

      if ( it == propmap->end() ) return defaultValue;

      std::istringstream i ( it->second );

      T ret;

      char c;

      if ( ! ( i >> ret ) || i.get ( c ) )
      {
        return defaultValue;
      }

      return ret;
    }

    template<typename T> bool setValue ( const std::string & key, const T & value )
    {
      std::ostringstream o;

      if ( ! ( o << value ) )
      {
        return false;
      }

      ( *propmap ) [key] = o.str();

      return true;
    }
    
    std::string getValue ( const std::string & key, const std::string & def ) const;
    const char* getValue ( const std::string & key, const char* defaultValue ) const;
};

}

#endif // COMMON_PROPERTIES_H
