/*!
 * $Id$
 *
 * \file DiskEntry.h
 * \brief process entries from the checkpoint and log files
 * \author Blake Lewis (Kosmix Corp.)
 *         Mike Ovsiannikov implement DETokenizer to speed up parsing / load by
 *         making it more cpu efficient.
 *
 * Copyright 2008-2012 Quantcast Corp.
 * Copyright 2006-2008 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
#if !defined(KFS_ENTRY_H)
#define KFS_ENTRY_H

#include "kfstypes.h"

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include <vector>
#include <iostream>
#include <string>
#include <map>

namespace KFS
{
using std::vector;
using std::map;
using std::istream;
using std::ostream;
using std::string;

class DETokenizer
{
public:
    struct Token
    {
        Token()
            : ptr(0),
              len(0)
            {}
        Token(const char* p)
            : ptr(p),
              len(strlen(p))
            {}
        Token(const char* p, size_t l)
            : ptr(p),
              len(l)
            {}
        bool operator==(const Token& other) const {
            return (len == other.len &&
                memcmp(ptr, other.ptr, len) == 0);
        }
        bool operator!=(const Token& other) const {
            return ! (*this == other);
        }
        bool operator==(const char* str) const {
            const char*       c = ptr;
            const char* const e = c + len;
            const char*       p = str;
            // Token should have no 0 characters.
            while (c < e) {
                if (*p++ != *c++) {
                    return false;
                }
            }
            return (*p == 0);
        }
        bool operator!=(const char* str) const {
            return ! (*this == str);
        }
        // Not lexicographic order, only intended for std::map.
        bool operator<(const Token& other) const {
            return (len < other.len || (len == other.len &&
                memcmp(ptr, other.ptr, len) < 0));
        }
        operator string () const {
            return string(ptr, len);
        }
        bool empty() const {
            return len <= 0;
        }
        const char* ptr;
        size_t      len;
    };
    DETokenizer(istream& in)
        : tokens(new Token[kMaxEntryTokens]),
          cur(tokens),
          end(tokens),
          is(in),
          entryCount(0),
          buffer(new char [kMaxEntrySize + 3]),
          bend(buffer + kMaxEntrySize),
          nextEnt(bend),
          prevStart(nextEnt),
          base(10),
          lastOk(true)
        { MarkEnd(); }
    ~DETokenizer() {
        delete [] tokens;
        delete [] buffer;
    }
    void pop_front() {
        assert(cur < end);
        ++cur;
    }
    const Token& front() const {
        assert(cur < end);
        return *cur;
    }
    size_t size() const {
        return (end - cur);
    }
    bool empty() const {
        return (cur >= end);
    }
    bool next(ostream* os = 0);
    size_t getEntryCount() const {
        return entryCount;
    }
    string getEntry() const {
        const char* const p = end == tokens ? nextEnt : tokens->ptr;
        const char* const e = strchr(p, '\n');
        return (e ? string(p, e - p) : string(p));
    }
    void popPath(string& path) {
        assert(cur != end);
        const char* const s = cur->ptr;
        const char*       p;
        do {
            p = cur->ptr + cur->len;
            ++cur;
        } while (cur != end && ! cur->empty());
        path.assign(s, p - s);
    }
    int64_t toNumber() {
        assert(cur < end);
        if (cur->len <= 0) {
            return -1;
        }
        if (base == 16) {
            return hexToNumber();
        }
        char* end;
        const int64_t ret = strtoll(cur->ptr, &end, base);
        lastOk = end == cur->ptr + cur->len;
        return (lastOk ? ret : (int64_t)-1);
    }
    void setIntBase(int b) {
        base = b;
    }
    int getIntBase() const {
        return base;
    }
    bool isLastOk() const {
        return lastOk;
    }
private:
    enum { kMaxEntrySize   = 512 << 10 };
    enum { kMaxEntryTokens = 1   << 10 };
    Token*      tokens;
    Token*      cur;
    Token*      end;
    istream&    is;
    size_t      entryCount;
    char* const buffer;
    char*       bend;
    char*       nextEnt;
    const char* prevStart;
    int         base;
    bool        lastOk;
    static const unsigned char* const c2hex;

    void MarkEnd() {
        // sentinel for next()
        assert(bend <= buffer + kMaxEntrySize);
        bend[0] = '\n';
        bend[1] = 0;
        bend[2] = '\n';
    }
    int64_t hexToNumber() {
        if (cur->len <= 0) {
            return -1;
        }
        const unsigned char* p =
            reinterpret_cast<const unsigned char*>(cur->ptr);
        const unsigned char* const e = p + cur->len;
        const bool minus = *p == '-';
        if (minus || *p == '+') {
            ++p;
        }
        int64_t ret = 0;
        if (p + sizeof(ret) * 2 < e) {
            lastOk = false;
            return -1;
        }
        while (p < e) {
            const unsigned char h = c2hex[*p++];
            if (h == (unsigned char)0xFF) {
                lastOk = false;
                return -1;
            }
            ret = (ret << 4) | h;
        }
        return (minus ? -ret : ret);
    }
private:
    DETokenizer(const DETokenizer&);
    DETokenizer& operator=(const DETokenizer&);
};

inline static bool operator==(const char* str, const DETokenizer::Token& token) {
    return (token == str);
}

inline static bool operator!=(const char* str, const DETokenizer::Token& token) {
    return ! (token == str);
}

inline static ostream& operator<<(ostream& os, const DETokenizer::Token& token) {
    return os.write(token.ptr, token.len);
}

/*!
 * \brief a checkpoint or log entry read back from disk
 *
 * This class represents lines that have been read back from either
 * the checkpoint or log file during KFS startup.  Each entry in
 * these files is in the form
 *
 *  <keyword>/<data1>/<data2>/...
 *
 * where <keyword> represents a type of metatree node in the case
 * of checkpoint, or an update request, in a log file.  The basic
 * processing is to split the line into ts component parts, then
 * use the keyword to look up a function that validates the remaining
 * data and performs whatever action is appropriate.  In the case
 * of checkpoints, this will be to insert the specified node into
 * the tree, while for log entries, we redo the update (taking care
 * to specify any new file ID's so that they remain the same as
 * they were before the restart).
 */
class DiskEntry {
public:
    typedef DETokenizer::Token Token;
    typedef bool (*parser)(DETokenizer &c); //!< a parsing function
private:
    typedef map <Token, parser> parsetab;   //!< map type to parser
    parsetab table;
public:
    void add_parser(const Token& k, parser f) { table[k] = f; }
    bool parse(DETokenizer& tonenizer); //!< look up parser and call it
};


/*!
 * \brief parser helper routines
 * These functions remove items of the specified kind from the deque
 * of components.  The item will be preceded by an identifying keyword,
 * which is passed in as "tag".
 */
extern bool pop_name(
    string &name, const char* tag, DETokenizer &c, bool ok);
extern bool pop_path(
    string &path, const char* tag, DETokenizer &c, bool ok);
extern bool pop_fid(fid_t &fid, const char* tag, DETokenizer &c, bool ok);
extern bool pop_size(size_t &sz, const char* tag, DETokenizer &c, bool ok);
extern bool pop_offset(chunkOff_t &o, const char* tag, DETokenizer &c, bool ok);
extern bool pop_short(int16_t &n, const char* tag, DETokenizer &c, bool ok);
extern bool pop_type(
    FileType &t, const char* tag, DETokenizer &c, bool ok);
extern bool pop_time(
    int64_t &tv, const char* tag, DETokenizer &c, bool ok);
extern bool pop_num(int64_t &n, const char* tag, DETokenizer& c, bool ok);

}
#endif // !defined(KFS_ENTRY_H)
