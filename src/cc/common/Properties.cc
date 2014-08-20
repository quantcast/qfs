//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// \brief Properties implementation.
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

#include "Properties.h"
#include "RequestParser.h"

#include <iostream>
#include <fstream>
#include <cstdlib>
#include <string.h>

namespace KFS
{

using std::string;
using std::istream;
using std::ifstream;
using std::cerr;
using std::cout;
using std::endl;
using std::pair;

inline static int
AsciiCharToLower(int c)
{
    return ((c >= 'A' && c <= 'Z') ? 'a' + (c - 'A') : c);
}

template<typename T> inline bool
Properties::Parse(const Properties::String& str, const T& def, T& out) const
{
    const char*  ptr  = str.GetPtr();
    const size_t size = str.GetSize();
    if (intbase == 10) {
        if (! DecIntParser::Parse(ptr, size, out)) {
            out = def;
        }
        return true;
    }
    if (intbase == 16) {
        if (! HexIntParser::Parse(ptr, size, out)) {
            out = def;
        }
        return true;
    }
    return false;
}

template<typename T> inline static void
removeLTSpaces(const string& str, string::size_type start,
    string::size_type end, T& outStr, bool asciiToLower = false)
{
    char const* const delims = " \t\r\n";

    if (start >= str.length()) {
        outStr.clear();
        return;
    }
    string::size_type const first = str.find_first_not_of(delims, start);
    if (end <= first || first == string::npos) {
        outStr.clear();
        return;
    }
    string::size_type const last = str.find_last_not_of(
        delims, end == string::npos ? string::npos : end - 1);
    if (asciiToLower) {
        outStr.clear();
        for (const char* p = str.data() + first,
                * e = str.data() +
                    (last == string::npos ? str.size() : last + 1);
                p < e;
                ++p) {
            outStr.Append(AsciiCharToLower(*p & 0xFF));
        }
        return;
    }
    outStr.Copy(str.c_str() + first,
        (last == string::npos ? str.size() : last + 1) - first);
}

/* static */ string
Properties::AsciiToLower(const string& str)
{
    string s(str);
    for (string::iterator i = s.begin(); i != s.end(); ++i) {
        const int c = AsciiCharToLower(*i & 0xFF);
        if (c != *i) {
            *i = c;
        }
    }
    return s;
}

inline Properties::iterator
Properties::find(const Properties::String& key) const
{
    return propmap.find(key);
}

Properties::Properties(int base)
    : intbase(base),
      propmap()
{
}

Properties::Properties(const Properties &p)
    : intbase(p.intbase),
      propmap(p.propmap)
{
}

Properties::~Properties()
{
}

int
Properties::loadProperties(
    const char* fileName,
    char        delimiter,
    ostream*    verbose   /* = 0 */,
    bool        multiline /* = false */,
    bool        keysAsciiToLower /* = false */)
{
    ifstream input(fileName);
    if(! input.is_open()) {
        cerr <<  "Properties::loadProperties() failed to open the file:" <<
            fileName << endl;
        return(-1);
    }
    loadProperties(input, delimiter, verbose, multiline, keysAsciiToLower);
    input.close();
    return 0;
}

int
Properties::loadProperties(
    istream& ist,
    char     delimiter,
    ostream* verbose,
    bool     multiline        /* = false */,
    bool     keysAsciiToLower /* = false */)
{
    string line;
    String key;
    String val;
    if (ist) {
        line.reserve(512);
    }
    while (ist) {
        getline(ist, line); //read one line at a time
        if (line.empty() || line[0] == '#') {
            continue; // ignore comments
        }
        // find the delimiter
        string::size_type const pos = line.find(delimiter);
        if (pos == string::npos) {
            continue; // ignore if no delimiter is found
        }
        removeLTSpaces(line, 0, pos, key, keysAsciiToLower);
        removeLTSpaces(line, pos + 1, string::npos, val);
        if (multiline) {
            // allow properties to span across multiple lines
            propmap[key].Append(val);
        } else {
            propmap[key] = val;
        }
        if (verbose) {
            (*verbose) << "Loading key " << key  <<
                " with value " << propmap[key] << endl;
        }
    }
    return 0;
}

int
Properties::loadProperties(
    const char* buf,
    size_t      len,
    char        delimiter,
    ostream*    verbose   /* = 0 */,
    bool        multiline /* = false */,
    bool        keysAsciiToLower /* = false */)
{
    PropertiesTokenizer tokenizer(buf, len);
    if (keysAsciiToLower) {
        String lkey;
        while (tokenizer.Next(delimiter)) {
            const PropertiesTokenizer::Token& key = tokenizer.GetKey();
            const PropertiesTokenizer::Token& val = tokenizer.GetValue();
            lkey.clear();
            for (const char* p = key.mPtr, * e = p + key.mLen; p < e; ++p) {
                lkey.Append(AsciiCharToLower(*p & 0xFF));
            }
            if (multiline) {
                propmap[lkey].Append(val.mPtr, val.mLen);
            } else {
                propmap[lkey].Copy(val.mPtr, val.mLen);
            }
            if (verbose) {
                (*verbose) << "Loading key ";
                verbose->write(key.mPtr, key.mLen);
                (*verbose) << " with value ";
                verbose->write(val.mPtr, val.mLen);
                (*verbose) << endl;
            }
        }
    } else {
        while (tokenizer.Next(delimiter)) {
            const PropertiesTokenizer::Token& key = tokenizer.GetKey();
            const PropertiesTokenizer::Token& val = tokenizer.GetValue();
            if (multiline) {
                propmap[String(key.mPtr, key.mLen)].Append(val.mPtr, val.mLen);
            } else {
                propmap[String(key.mPtr, key.mLen)].Copy(val.mPtr, val.mLen);
            }
            if (verbose) {
                (*verbose) << "Loading key ";
                verbose->write(key.mPtr, key.mLen);
                (*verbose) << " with value ";
                verbose->write(val.mPtr, val.mLen);
                (*verbose) << endl;
            }
        }
    }
    return 0;
}

void
Properties::setValue(const string& key, const string& value)
{
    String kstr;
    if (key.length() > size_t(kStringBufSize)) {
        kstr = key;
    } else {
        kstr.Copy(key.data(), key.size());
    }
    if (value.length() > size_t(kStringBufSize)) {
        propmap[kstr] = value;
    } else {
        propmap[kstr].Copy(value.data(), value.size());
    }
}

void
Properties::setValue(const Properties::String& key, const string& value)
{
    if (value.length() > size_t(kStringBufSize)) {
        propmap[key] = value;
    } else {
        propmap[key].Copy(value.data(), value.size());
    }
}

string
Properties::getValueSelf(const Properties::String& key, const string& def) const
{
    PropMap::const_iterator const i = find(key);
    if (i == propmap.end()) {
        return def;
    }
    if (i->second.size() > size_t(kStringBufSize)) {
        return i->second.GetStr();
    }
    return string(i->second.data(), i->second.size());
}

const char*
Properties::getValueSelf(const Properties::String& key, const char* def) const
{
    PropMap::const_iterator const i = find(key);
    return (i == propmap.end() ? def : i->second.c_str());
}

int
Properties::getValueSelf(const Properties::String& key, int def) const
{
    PropMap::const_iterator const i = find(key);
    int ret = def;
    return (i == propmap.end() ? def : (Parse(i->second, def, ret) ? ret :
        (int)strtol(i->second.c_str(), 0, intbase)));
}

unsigned int
Properties::getValueSelf(const Properties::String& key, unsigned int def) const
{
    PropMap::const_iterator const i = find(key);
    unsigned int ret = def;
    return (i == propmap.end() ? def : (Parse(i->second, def, ret) ? ret :
        (unsigned int)strtoul(i->second.c_str(), 0, intbase)));
}

long
Properties::getValueSelf(const Properties::String& key, long def) const
{
    PropMap::const_iterator const i = find(key);
    long ret = def;
    return (i == propmap.end() ? def : (Parse(i->second, def, ret) ? ret :
        strtol(i->second.c_str(), 0, intbase)));
}

unsigned long
Properties::getValueSelf(const Properties::String& key, unsigned long def) const
{
    PropMap::const_iterator const i = find(key);
    unsigned long ret = def;
    return (i == propmap.end() ? def : (Parse(i->second, def, ret) ? ret :
        strtoul(i->second.c_str(), 0, intbase)));
}

long long
Properties::getValueSelf(const Properties::String& key, long long def) const
{
    PropMap::const_iterator const i = find(key);
    long long ret = def;
    return (i == propmap.end() ? def : (Parse(i->second, def, ret) ? ret :
        strtoll(i->second.c_str(), 0, intbase)));
}

unsigned long long
Properties::getValueSelf(const Properties::String& key, unsigned long long def)
    const
{
    PropMap::const_iterator const i = find(key);
    unsigned long long ret = def;
    return (i == propmap.end() ? def : (Parse(i->second, def, ret) ? ret :
        strtoull(i->second.c_str(), 0, intbase)));
}

double
Properties::getValueSelf(const Properties::String& key, double def) const
{
    PropMap::const_iterator const i = find(key);
    if (i == propmap.end()) {
        return def;
    }
    char*             e   = 0;
    const char* const p   = i->second.c_str();
    const double      ret = strtod(p, &e);
    return ((p < e && *e <= ' ') ? ret : def);
}

signed char
Properties::getValueSelf(const Properties::String& key, signed char def) const
{
    PropMap::const_iterator const i = find(key);
    signed char ret = def;
    return (i == propmap.end() ? def : (Parse(i->second, def, ret) ? ret :
        (signed char)strtol(i->second.c_str(), 0, intbase)));
}

unsigned char
Properties::getValueSelf(const Properties::String& key, unsigned char def) const
{
    PropMap::const_iterator const i = find(key);
    unsigned char ret = def;
    return (i == propmap.end() ? def : (Parse(i->second, def, ret) ? ret :
        (unsigned char)strtoul(i->second.c_str(), 0, intbase)));
}

char
Properties::getValueSelf(const Properties::String& key, char def) const
{
    PropMap::const_iterator const i = find(key);
    char ret = def;
    return (i == propmap.end() ? def : (Parse(i->second, def, ret) ? ret :
        (char)strtol(i->second.c_str(), 0, intbase)));
}

bool
Properties::remove(const Properties::String& key)
{
    return (propmap.erase(key) > 0);
}

void
Properties::getList(string& outBuf,
    const string& linePrefix, const string& lineSuffix) const
{
    PropMap::const_iterator iter;
    for (iter = propmap.begin(); iter != propmap.end(); iter++) {
        if (iter->first.size() > 0) {
            outBuf += linePrefix;
            outBuf.append(iter->first.data(), iter->first.size());
            outBuf += '=';
            outBuf.append(iter->second.data(), iter->second.size());
            outBuf += lineSuffix;
        }
  }
  return;
}

size_t
Properties::copyWithPrefix(const char* prefix, Properties& props) const
{
    return copyWithPrefix(prefix, prefix ? strlen(prefix) : size_t(0), props);
}

inline static bool
KeyStartsWith(const Properties::String& key,
    const char* prefix, size_t prefixLen)
{
    return (key.size() >= prefixLen &&
        memcmp(key.data(), prefix, prefixLen) == 0);
}

size_t
Properties::copyWithPrefix(const char* prefix, size_t prefixLen,
    Properties& props) const
{
    size_t ret = 0;
    if (prefix && 0 < prefixLen) {
        for (PropMap::const_iterator it = propmap.lower_bound(
                    String(prefix, prefixLen));
                it != propmap.end(); it++) {
            const String& key = it->first;
            if (! KeyStartsWith(key, prefix, prefixLen)) {
                break;
            }
            pair<PropMap::iterator, bool> res = props.propmap.insert(
                make_pair(key, it->second));
            if (res.second) {
                ret++;
            } else if (res.first->second != it->second) {
                res.first->second = it->second;
                ret++;
            }
        }
        return ret;
    }
    for (PropMap::const_iterator it = propmap.begin();
            it != propmap.end();
            ++it) {
        const String& key = it->first;
        if (prefixLen <= key.size()) {
            pair<PropMap::iterator, bool> res = props.propmap.insert(
                make_pair(key, it->second));
            if (res.second) {
                ret++;
            } else if (res.first->second != it->second) {
                res.first->second = it->second;
                ret++;
            }
        }
    }
    return ret;
}

bool
Properties::equalsWithPrefix(const char* prefix, size_t prefixLen,
    const Properties& props) const
{
    if (prefixLen <= 0) {
        return (propmap == props.propmap);
    }
    if (prefix) {
        const String pref(prefix, prefixLen);
        for (PropMap::const_iterator
                it  = propmap.lower_bound(pref),
                    oit = props.propmap.lower_bound(pref);
                ;
                ++it, ++oit) {
            if (it == propmap.end() ||
                    ! KeyStartsWith(it->first, prefix, prefixLen)) {
                return (oit == props.propmap.end() ||
                    ! KeyStartsWith(oit->first, prefix, prefixLen));
            }
            if (oit == props.propmap.end() || *it != *oit) {
                break;
            }
        }
        return false;
    }
    for (PropMap::const_iterator
            it  = propmap.begin(), oit = props.propmap.begin(); ;) {
        while (it != propmap.end() && it->first.size() < prefixLen) {
            ++it;
        }
        while (oit != props.propmap.end() && oit->first.size() < prefixLen) {
            ++oit;
        }
        if (it == propmap.end()) {
            return (oit == props.propmap.end());
        }
        if (oit == props.propmap.end() || *it != *oit) {
            break;
        }
        ++it;
        ++oit;
    }
    return false;
}

} // namespace KFS
