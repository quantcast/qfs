//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// \brief Properties file similar to java.util.Properties
//
// Created 2004/05/05
//
// Copyright 2008-2012 Quantcast Corp.
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

#include "StdAllocator.h"

namespace KFS
{

using std::map;
using std::string;
using std::istream;

// Key: value properties.
// Can be used to parse rfc822 style request headers, or configuration files.
class Properties
{
private :
    int intbase;
    //Map that holds the (key,value) pairs
    typedef map<string, string,
        std::less<string>,
        StdFastAllocator<std::pair<const string, string> >
    > PropMap;
    PropMap propmap;
    inline PropMap::const_iterator find(const string& key) const;

public:
    static string AsciiToLower(const string& str);

    typedef PropMap::const_iterator iterator;
    iterator begin() const { return propmap.begin(); }
    iterator end() const { return propmap.end(); }
    // load the properties from a file
    int loadProperties(const char* fileName, char delimiter,
        bool verbose, bool multiline = false, bool keysAsciiToLower = false);
    // load the properties from an in-core buffer
    int loadProperties(istream &ist, char delimiter,
        bool verbose, bool multiline = false, bool keysAsciiToLower = false);
    string getValue(const string& key, const string& def) const;
    const char* getValue(const string& key, const char* def) const;
    int getValue(const string& key, int def) const;
    unsigned int getValue(const string& key, unsigned int def) const;
    long getValue(const string& key, long def) const;
    unsigned long getValue(const string& key, unsigned long def) const;
    long long getValue(const string& key, long long def) const;
    unsigned long long getValue(const string& key, unsigned long long def) const;
    double getValue(const string& key, double def) const;
    void setValue(const string& key, const string& value);
    void getList(string &outBuf, const string& linePrefix,
        const string& lineSuffix = string("\n")) const;
    void clear() { propmap.clear(); }
    bool empty() const { return propmap.empty(); }
    size_t size() const { return propmap.size(); }
    void copyWithPrefix(const string& prefix, Properties& props) const;
    void swap(Properties& props)
        { propmap.swap(props.propmap); }
    void setIntBase(int base)
        { intbase = base; }
    Properties(int base = 10);
    Properties(const Properties &p);
    ~Properties();

};

}

#endif // COMMON_PROPERTIES_H
