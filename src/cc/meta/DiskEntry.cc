/*
 * $Id$
 *
 * \file DiskEntry.cc
 * \brief parse checkpoint and log entries
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

#include "DiskEntry.h"
#include "util.h"

namespace KFS
{
using std::istream;
using std::streamsize;

bool
DiskEntry::parse(DETokenizer& tokenizer)
{
    if (tokenizer.empty())
        return true;

    parsetab::const_iterator c = table.find(tokenizer.front());
    return (c != table.end() && c->second(tokenizer));
}

bool
DETokenizer::next(ostream* os)
{
    Token* const tend = tokens + kMaxEntryTokens;
    cur = tokens;
    end = tokens;
    if (os && prevStart < nextEnt) {
        os->write(prevStart, nextEnt - prevStart);
        prevStart = nextEnt;
    }
    for (; ;) {
        while (*nextEnt == '\n') {
            ++nextEnt;
        }
        char* s = nextEnt;
        char* p = s;
        while (*p != '\n') {
            if (*p == '/') {
                end->ptr = s;
                end->len = p - s;
                s = ++p;
                if (++end >= tend) {
                    end = tokens;
                    return false;
                }
            } else {
                ++p;
            }
        }
        if (p < bend) {
            end->ptr = s;
            end->len = p - s;
            ++end;
            nextEnt  = p + 1;
            entryCount++;
            break;
        }
        end = tokens;
        const size_t size = nextEnt >= bend ? 0 :
            (p < bend ? p : bend) - nextEnt;
        if (kMaxEntrySize <= size) {
            return false;
        }
        if (os && prevStart < nextEnt) {
            const size_t sz =
                (nextEnt < bend ? nextEnt : bend) - prevStart;
            if (sz > 0) {
                os->write(prevStart, sz);
            }
        }
        memmove(buffer, nextEnt, size);
        nextEnt = buffer;
        prevStart = nextEnt;
        if (! is.read(buffer + size, kMaxEntrySize - size)) {
            bend = buffer + size;
            if (! is.eof()) {
                MarkEnd();
                return false;
            }
            streamsize const cnt = is.gcount();
            if (cnt <= 0) {
                MarkEnd();
                return false;
            }
            bend += cnt;
            MarkEnd();
        }
    }
    return true;
}

const unsigned char* const DETokenizer::c2hex = char2HexTable();

/*!
 * \brief remove a file name from the front of the deque
 * \param[out]  name    the returned name
 * \param[in]   tag the keyword that precedes the name
 * \param[in]   c   the deque of components from the entry
 * \param[in]   ok  if false, do nothing and return false
 * \return      true if parse was successful
 *
 * The ok parameter short-circuits parsing if an error occurs.
 * This lets us do a series of steps without checking until the
 * end.
 */
bool
pop_name(string &name, const char* tag, DETokenizer& c, bool ok)
{
    if (!ok || c.size() < 2 || c.front() != tag)
        return false;

    c.pop_front();
    name = c.front();
    c.pop_front();
    if (!name.empty())
        return true;

    /*
     * Special hack: the initial entry for "/" shows up
     * as two empty components ("///"); I should probably
     * come up with a more elegant way to do this.
     */
    if (c.empty() || !c.front().empty())
        return false;

    c.pop_front();
    name = "/";
    return true;
}

/*!
 * \brief remove a path name from the front of the deque
 * \param[out]  path    the returned path
 * \param[in]   tag the keyword that precedes the path
 * \param[in]   c   the deque of components from the entry
 * \param[in]   ok  if false, do nothing and return false
 * \return      true if parse was successful
 *
 * The ok parameter short-circuits parsing if an error occurs.
 * This lets us do a series of steps without checking until the
 * end.
 */
bool
pop_path(string &path, const char* tag, DETokenizer& c, bool ok)
{
    if (!ok || c.size() < 2 || c.front() != tag)
        return false;

    c.pop_front();
    /* Collect everything else in path with components separated by '/' */
    c.popPath(path);
    return ! path.empty();
}

/*!
 * \brief remove a file ID from the component deque
 */
bool
pop_fid(fid_t &fid, const char* tag, DETokenizer& c, bool ok)
{
    if (!ok || c.size() < 2 || c.front() != tag)
        return false;

    c.pop_front();
    fid = c.toNumber();
    c.pop_front();
    return (fid != -1);
}

/*!
 * \brief remove a size_t value from the component deque
 */
bool
pop_size(size_t &sz, const char* tag, DETokenizer& c, bool ok)
{
    if (!ok || c.size() < 2 || c.front() != tag)
        return false;

    c.pop_front();
    sz = c.toNumber();
    c.pop_front();
    return (sz != -1u);
}

/*!
 * \brief remove a short value from the component deque
 */
bool
pop_short(int16_t &num, const char* tag, DETokenizer& c, bool ok)
{
    if (!ok || c.size() < 2 || c.front() != tag)
        return false;

    c.pop_front();
    num = (int16_t) c.toNumber();
    c.pop_front();
    return (num != (int16_t) -1);
}

/*!
 * \brief remove a chunkOff_t value from the component deque
 */
bool
pop_offset(chunkOff_t &o, const char* tag, DETokenizer& c, bool ok)
{
    if (!ok || c.size() < 2 || c.front() != tag)
        return false;

    c.pop_front();
    o = c.toNumber();
    c.pop_front();
    return (o != -1);
}

/*!
 * \brief remove a file type from the component deque
 */
bool
pop_type(FileType& t, const char* tag, DETokenizer& c, bool ok)
{
    if (!ok || c.size() < 2 || c.front() != tag)
        return false;

    c.pop_front();
    string type = c.front();
    c.pop_front();
    if (type == "file") {
        t = KFS_FILE;
    } else if (type == "dir") {
        t = KFS_DIR;
    } else
        t = KFS_NONE;

    return (t != KFS_NONE);
}

/*!
 * \brief remove a time value from the component deque
 */
bool
pop_time(int64_t& tv, const char* tag, DETokenizer& c, bool ok)
{
    if (!ok || c.size() < 3 || c.front() != tag)
        return false;

    c.pop_front();
    int64_t sec = c.toNumber();
    c.pop_front();
    int64_t usec = c.toNumber();
    c.pop_front();
    const int64_t kMicroSec = 1000 * 1000;
    tv = sec * kMicroSec + usec;
    return (sec != -1 && usec >= 0 && usec <= kMicroSec);
}

/*!
 * \brief remove a int64_t value from the component deque
 */
bool
pop_num(int64_t& n, const char* tag, DETokenizer& c, bool ok)
{
    if (!ok || c.size() < 2 || c.front() != tag)
        return false;

    c.pop_front();
    n = c.toNumber();
    c.pop_front();
    return c.isLastOk();
}

} // Namespace KFS
