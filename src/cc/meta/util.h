/*!
 * $Id$
 *
 * \file util.h
 * \brief miscellaneous metadata server code
 * \author Blake Lewis (Kosmix Corp.)
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
#if !defined(KFS_UTIL_H)
#define KFS_UTIL_H

#include <string>
#include <ostream>
#include <inttypes.h>

#include "kfstypes.h"
#include "kfsio/IOBuffer.h"
#include "kfsio/IOBufferWriter.h"
#include "common/time.h"
#include "common/IntToString.h"

namespace KFS
{

using std::string;
using std::ostream;

extern chunkOff_t chunkStartOffset(chunkOff_t offset);
extern int link_latest(const string& real, const string& alias);
extern string toString(int64_t n);
extern int64_t toNumber(const char* str);
static inline int64_t toNumber(const string& s) { return toNumber(s.c_str()); }
extern string makename(const string& dir, const string& prefix, int64_t number);
extern bool file_exists(const string& s);
extern void warn(const string& s, bool use_perror = false);
extern void panic(const string& s, bool use_perror = false);
extern const unsigned char* char2HexTable();

extern void sendtime(ostream &os, const string &prefix,
             int64_t t, const string &suffix);
extern ostream& resetOStream(ostream& os);

class DisplayDateTime
{
public:
    explicit DisplayDateTime(int64_t usec, bool isoFlag = false,
            bool usecFlag = false)
        : mTimeUsec(usec),
          mIsoFlag(isoFlag),
          mDisplayUsecsFlag(usecFlag)
        {}
    DisplayDateTime()
        : mTimeUsec(microseconds()),
          mIsoFlag(false),
          mDisplayUsecsFlag(false)
        {}
    ostream& display(ostream& os) const;
private:
    const int64_t mTimeUsec;
    const bool    mIsoFlag;
    const bool    mDisplayUsecsFlag;
};

inline ostream& operator<<(ostream& os, const DisplayDateTime& dt)
    { return dt.display(os); }

class DisplayIsoDateTime : public DisplayDateTime
{
public:
    explicit DisplayIsoDateTime(int64_t usec, bool usecFlag = true)
        : DisplayDateTime(usec, true, usecFlag)
        {}
    explicit DisplayIsoDateTime(bool usecFlag = true)
        : DisplayDateTime(microseconds(), true, usecFlag)
        {}
};

inline ostream& operator<<(ostream& os, const DisplayIsoDateTime& dt)
    { return dt.display(os); }
    
class IntIOBufferWriter : public IOBufferWriter
{
public:
    IntIOBufferWriter(
        IOBuffer& buf)
        : IOBufferWriter(buf),
          mBufEnd(mBuf + kBufSize)
        {}
    template<typename T>
    void WriteInt(T val)
    {
        const char* const p = IntToDecString(val, mBufEnd);
        Write(p, mBufEnd - p);
    }
    template<typename T>
    void WriteHexInt(T val)
    {
        const char* const p = IntToHexString(val, mBufEnd);
        Write(p, mBufEnd - p);
    }
private:
    enum { kBufSize = 32 };

    char        mBuf[kBufSize];
    char* const mBufEnd;
private:
    IntIOBufferWriter(const IntIOBufferWriter&);
    IntIOBufferWriter& operator=(const IntIOBufferWriter&);
};

/// Is a message that ends with "\r\n\r\n" available in the
/// buffer.
/// @param[in] iobuf  An IO buffer stream with message
/// received from the chunk server.
/// @param[out] msgLen If a valid message is
/// available, length (in bytes) of the message.
/// @retval true if a message is available; false otherwise
///
extern bool IsMsgAvail(IOBuffer *iobuf, int *msgLen);
extern void setAbortOnPanic(bool flag);


}
#endif // !defined(KFS_UTIL_H)
