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

#include <iosfwd>
#include <string>
#include <map>

#include "StdAllocator.h"
#include "StBuffer.h"

namespace KFS
{

using std::map;
using std::string;
using std::istream;
using std::ostream;

// Key: value properties.
// Can be used to parse rfc822 style request headers, or configuration files.
class Properties
{
public:
    enum { kStringBufSize = 32 };
    // with 32 bytes the size of RB tree node: (32+8+8)*2+3*8 < 128 bytes, gnu
    // pool allocator (StdFastAllocator) threshold.
    typedef StringBufT<kStringBufSize> String;
private:
    int intbase;
    //Map that holds the (key,value) pairs
    typedef map<String, String,
        std::less<String>,
        StdFastAllocator<std::pair<const String, String> >
    > PropMap;
    PropMap propmap;
    template<typename T> bool Parse(
        const Properties::String& str, const T& def, T& out) const;
    inline PropMap::const_iterator find(const String& key) const;
    string getValueSelf(const String& key, const string& def) const;
    const char* getValueSelf(const String& key, const char* def) const;
    int getValueSelf(const String& key, int def) const;
    unsigned int getValueSelf(const String& key, unsigned int def) const;
    long getValueSelf(const String& key, long def) const;
    unsigned long getValueSelf(const String& key, unsigned long def) const;
    long long getValueSelf(const String& key, long long def) const;
    unsigned long long getValueSelf(const String& key, unsigned long long def)
        const;
    double getValueSelf(const String& key, double def) const;
    char getValueSelf(const String& key, char def) const;
    signed char getValueSelf(const String& key, signed char def) const;
    unsigned char getValueSelf(const String& key, unsigned char def) const;

public:
    static string AsciiToLower(const string& str);

    typedef PropMap::const_iterator iterator;
    iterator begin() const { return propmap.begin(); }
    iterator end() const { return propmap.end(); }
    // load the properties from a file
    int loadProperties(const char* fileName, char delimiter,
        ostream* verbose = 0, bool multiline = false, bool keysAsciiToLower = false);
    // load the properties from an in-core buffer
    int loadProperties(istream& ist, char delimiter,
        ostream* verbose = 0, bool multiline = false, bool keysAsciiToLower = false);
    int loadProperties(const char* buf, size_t len, char delimiter,
        ostream* verbose = 0, bool multiline = false,
        bool keysAsciiToLower = false);
    template<typename TValue>
    TValue getValue(const String& key, const TValue& def) const
        { return getValueSelf(key, def); }
    const char* getValue(const String& key, const char* def) const
        { return getValueSelf(key, def); }
    template<typename TKey, typename TValue>
    TValue getValue(const TKey& key, const TValue& def) const
        { return getValueSelf(String(key), def); }
    template<typename TKey>
    const char* getValue(const TKey& key, const char* def) const
        { return getValueSelf(String(key), def); }
    const String* getValue(const Properties::String& key) const
    {
        PropMap::const_iterator const it = propmap.find(key);
        return (it != propmap.end() ? &(it->second) : 0);
    }
    const String* getValue(const char* key) const
        { return getValue(String(key)); }
    void setValue(const string& key, const string& value);
    void setValue(const String& key, const string& value);
    template<typename TKey>
    void setValue(const TKey& key, const string& value)
        { setValue(String(key), value); }
    void setValue(const String& key, const String& value)
        { propmap[key] = value; }
    void getList(string &outBuf, const string& linePrefix,
        const string& lineSuffix = string("\n")) const;
    bool remove(const String& key);
    template<typename T>
    bool remove(const T& key)
        { return remove(String(key)); }
    bool remove(const char* key)
        { return remove(String(key)); }
    void clear() { propmap.clear(); }
    bool empty() const { return propmap.empty(); }
    size_t size() const { return propmap.size(); }
    size_t copyWithPrefix(const char* prefix, size_t prefixLen,
        Properties& props) const;
    size_t copyWithPrefix(const char* prefix, Properties& props) const;
    size_t copyWithPrefix(const string& prefix, Properties& props) const
        { return copyWithPrefix(prefix.data(), prefix.size(), props); }
    void swap(Properties& props)
    {
        propmap.swap(props.propmap);
        const int tmp = intbase;
        intbase = props.intbase;
        props.intbase = tmp;
    }
    void setIntBase(int base)
        { intbase = base; }
    bool operator==(const Properties& p) const
        { return (intbase == p.intbase && propmap == p.propmap); }
    bool operator!=(const Properties& p) const
        { return (! (*this == p)); }
    bool equalsWithPrefix(const char* prefix, size_t prefixLen,
        const Properties& props) const;
    Properties(int base = 10);
    Properties(const Properties& p);
    ~Properties();
};

}

#endif // COMMON_PROPERTIES_H
