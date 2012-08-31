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

#include <iostream>
#include <fstream>
#include <cstdlib>
#include "Properties.h"

namespace KFS
{

using std::string;
using std::istream;
using std::ifstream;
using std::cerr;
using std::cout;
using std::endl;

inline static int
AsciiCharToLower(int c)
{
    return ((c >= 'A' && c <= 'Z') ? 'a' + (c - 'A') : c);
}

inline string
removeLTSpaces(const string& str, string::size_type start, string::size_type end,
    bool asciiToLower = false)
{
    char const* const delims = " \t\r\n";

    if (start >= str.length()) {
        return string();
    }
    string::size_type const first = str.find_first_not_of(delims, start);
    if (end <= first || first == string::npos) {
        return string();
    }
    string::size_type const last = str.find_last_not_of(
        delims, end == string::npos ? string::npos : end - 1);
    return (
        asciiToLower ?
            Properties::AsciiToLower(str.substr(first, last - first + 1)) :
            str.substr(first, last - first + 1)
    );
}

/* static */ string
Properties::AsciiToLower(const string& str)
{
    string s(str);
    for (string::iterator i = s.begin(); i != s.end(); ++i) {
        const int c = AsciiCharToLower(*i);
        if (c != *i) {
            *i = c;
        }
    }
    return s;
}

inline
Properties::iterator Properties::find(const string& key) const
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
Properties::loadProperties(const char* fileName, char delimiter,
    bool verbose /* = false */, bool multiline /* =false */,
    bool keysAsciiToLower /* = false */)
{
    ifstream input(fileName);
    if(! input.is_open()) {
        cerr <<  "Properties::loadProperties() Could not open the file:" <<
            fileName << endl;
        return(-1);
    }
    loadProperties(input, delimiter, verbose, multiline, keysAsciiToLower);
    input.close();
    return 0;
}

int
Properties::loadProperties(istream &ist, char delimiter,
    bool verbose /* = false */, bool multiline /* = false */,
    bool keysAsciiToLower /* = false */)
{
    string line;
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
        const string key(removeLTSpaces(line, 0, pos, keysAsciiToLower));
        const string val(removeLTSpaces(line, pos + 1, string::npos));
        if (multiline) {
            // allow properties to span across multiple lines
            propmap[key] += val;
        } else {
            propmap[key] = val;
        }
        if (verbose) {
            cout << "Loading key " << key  <<
                " with value " << propmap[key] << endl;
        }
    }
    return 0;
}

void
Properties::setValue(const string& key, const string& value)
{
    propmap[key] = value;
    return;
}

string
Properties::getValue(const string& key, const string& def) const
{
    PropMap::const_iterator const i = find(key);
    return (i == propmap.end() ? def : i->second);
}

const char*
Properties::getValue(const string& key, const char* def) const
{
    PropMap::const_iterator const i = find(key);
    return (i == propmap.end() ? def : i->second.c_str());
}

int
Properties::getValue(const string& key, int def) const
{
    PropMap::const_iterator const i = find(key);
    return (i == propmap.end() ? def :
        (int)strtol(i->second.c_str(), 0, intbase));
}

unsigned int
Properties::getValue(const string& key, unsigned int def) const
{
    PropMap::const_iterator const i = find(key);
    return (i == propmap.end() ? def :
        (unsigned int)strtoul(i->second.c_str(), 0, intbase));
}

long
Properties::getValue(const string& key, long def) const
{
    PropMap::const_iterator const i = find(key);
    return (i == propmap.end() ? def : strtol(i->second.c_str(), 0, intbase));
}

unsigned long
Properties::getValue(const string& key, unsigned long def) const
{
    PropMap::const_iterator const i = find(key);
    return (i == propmap.end() ? def : strtoul(i->second.c_str(), 0, intbase));
}

long long
Properties::getValue(const string& key, long long def) const
{
    PropMap::const_iterator const i = find(key);
    return (i == propmap.end() ? def : strtoll(i->second.c_str(), 0, intbase));
}

unsigned long long
Properties::getValue(const string& key, unsigned long long def) const
{
    PropMap::const_iterator const i = find(key);
    return (i == propmap.end() ? def : strtoull(i->second.c_str(), 0, intbase));
}

double
Properties::getValue(const string& key, double def) const
{
    PropMap::const_iterator const i = find(key);
    return (i == propmap.end() ? def : atof(i->second.c_str()));
}

void
Properties::getList(string &outBuf,
    const string& linePrefix, const string& lineSuffix) const
{
  PropMap::const_iterator iter;
  for (iter = propmap.begin(); iter != propmap.end(); iter++) {
    if (iter->first.size() > 0) {
      outBuf += linePrefix;
      outBuf += iter->first;
      outBuf += '=';
      outBuf += iter->second;
      outBuf += lineSuffix;
    }
  }
  return;
}

void
Properties::copyWithPrefix(const string& prefix, Properties& props) const
{
    PropMap::const_iterator iter;
    for (iter = propmap.begin(); iter != propmap.end(); iter++) {
        const string& key = iter->first;
        if (key.compare(0, prefix.length(), prefix) == 0) {
            props.propmap[key] = iter->second;
        }
    }
}

} // namespace KFS
