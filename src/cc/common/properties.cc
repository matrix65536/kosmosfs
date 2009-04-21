//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$ 
//
// \brief Properties implementation.
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

#include <iostream>
#include <fstream>
#include <cstdlib>
#include "properties.h"

using std::string;
using namespace KFS;

Properties::Properties()
{
    propmap = new std::map<string, string>;
}

Properties::Properties(const Properties &p)
{
    propmap = new std::map<string, string>(*(p.propmap));
}

Properties::~Properties()
{
    delete propmap;
}

int Properties::loadProperties(const char * fileName, char delimiter, bool verbose, bool multiline /*=false*/)
{
    std::ifstream input(fileName);
    string line;

    if(!input.is_open()) 
    {
        std::cerr <<  "Properties::loadProperties() Could not open the file:" << fileName << std::endl;
        return(-1);
    }
    loadProperties(input, delimiter, verbose, multiline);
    input.close();
    return 0;
}

int Properties::loadProperties(std::istream &ist, char delimiter, bool verbose, bool multiline /*=false*/)
{
    string line;

    while(ist)
    {
        getline(ist, line);                       //read one line at a time
        if  (line.find('#') == 0)
            continue;                               //ignore comments
        string::size_type pos =
            line.find(delimiter);                   //find the delimiter
        
        if( pos == line.npos )
            continue;                               //ignore if no delimiter is found
        string key = line.substr(0,pos);       // get the key
        key = removeLTSpaces(key);
        string value = line.substr(pos+1);     //get the value
        value = removeLTSpaces(value);

		if (multiline)
        	(*propmap)[key] += value;					// allow properties to be spread across multiple lines
        else
        	(*propmap)[key] = value;
        
        if( verbose)
            std::cout << "Loading key " << key  << " with value " << (*propmap)[key] << std::endl;
    }
    return 0;
}

string Properties::removeLTSpaces(string str){

    char const* delims = " \t\r\n";

    // trim leading whitespace
    string::size_type  notwhite = str.find_first_not_of(delims);
    str.erase(0,notwhite);

   // trim trailing whitespace
   notwhite = str.find_last_not_of(delims);
   str.erase(notwhite+1);
   return(str);
}

void Properties::getList(string &outBuf, const string & linePrefix) const {
  std::map<string, string>::iterator iter;

  for (iter = propmap->begin(); iter != propmap->end(); iter++) {
    if ((*iter).first.size() > 0) {
      outBuf += linePrefix;
      outBuf += (*iter).first;
      outBuf += '=';
      outBuf += (*iter).second;
      outBuf += '\n';
    }
  }

  return;
}

string Properties::getValue ( const std::string & key, const std::string & defaultValue ) const
{
  std::map<std::string, std::string>::const_iterator it = propmap->find ( key );

  if ( it == propmap->end() ) return defaultValue;

  return it->second;
}

const char* Properties::getValue ( const std::string & key, const char* defaultValue ) const
{
  std::map<std::string, std::string>::const_iterator it = propmap->find ( key );

  if ( it == propmap->end() ) return defaultValue;

  return ( it->second ).c_str();
}
