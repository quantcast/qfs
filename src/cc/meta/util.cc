/*!
 * $Id$
 *
 * \file util.cc
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

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>

#include <ostream>
#include <sstream>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include "util.h"
#include "common/MsgLogger.h"
#include "common/RequestParser.h"
#include "common/IntToString.h"
#include "qcdio/QCUtils.h"

namespace KFS
{

/*!
 * \brief Find the chunk that contains the specified file offset.
 * \param[in] offset    offset in the file for which we need the chunk
 * \return      offset in the file that corresponds to the
 * start of the chunk.
 */
chunkOff_t
chunkStartOffset(chunkOff_t offset)
{
    return (offset / CHUNKSIZE) * CHUNKSIZE;
}

/*!
 * \brief link to the latest version of a file
 *
 * \param[in] realname  name of the target file
 * \param[in] alias name to link to target
 * \return      error code from link command
 *
 * Make a hard link with the name "alias" to the
 * given file "realname"; we use this to make it
 * easy to find the latest log and checkpoint files.
 */
int
link_latest(const string& realname, const string& alias)
{
    int status = 0;
    for (int64_t i = getpid() + time(0), e = i + 5; i < e; i++) {
        const string tmp = alias + "." + toString(i) + ".tmp";
        if (link(realname.c_str(), tmp.c_str())) {
            status = -errno;
        } else {
            if (rename(tmp.c_str(), alias.c_str())) {
                status = -errno;
            } else {
                status = 0;
            }
            break;
        }
    }
    return status;
}

/*!
 * \brief convert a number to a string
 * \param[in] n the number
 * \return  the string
 */
string
toString(int64_t n)
{
    char buf[32];
    char* const e = buf + sizeof(buf) / sizeof(buf[0]);
    char* const p = IntToDecString(n, e);
    return string(p, e - p);
}

/*!
 * \brief convert a string to a number
 * \param[in] s the string
 * \return  the number
 */
int64_t
toNumber(const char* str)
{
    char* endptr;
    const int64_t n = strtoll(str, &endptr, 10);
    return (*endptr != '\0' ? int64_t(-1) : n);
}

/*!
 * \brief paste together a pathname from its constituent parts
 * \param[in] dir   directory path
 * \param[in] prefix    beginning part of file name
 * \param[in] number    numeric suffix
 * \return      string "<dir>/<prefix>.<number>"
 */
string
makename(const string& dir, const string& prefix, int64_t number)
{
    return dir + "/" + prefix + "." + toString(number);
}

/*!
 * \brief check whether a file exists
 * \param[in]   name    path name of the file
 * \return      true if stat says it is a plain file
 */
bool
file_exists(const string& name)
{
    struct stat s;
    if (stat(name.c_str(), &s) == -1) {
        return false;
    }
    return S_ISREG(s.st_mode);
}

///
/// Return true if there is a sequence of "\r\n\r\n".
/// @param[in] iobuf: Buffer with data
/// @param[out] msgLen: string length of the command in the buffer
/// @retval true if a command is present; false otherwise.
///
bool
IsMsgAvail(IOBuffer* iobuf, int* msgLen)
{
    const int idx = iobuf->IndexOf(0, "\r\n\r\n");
    if (idx < 0) {
        return false;
    }
    *msgLen = idx + 4; // including terminating seq. length.
    return true;
}

/*!
 * A helper function to print out a timeval into a string buffer with
 * a prefix/suffix string around the time values.
 */
void
sendtime(ostream& os, const string& prefix, int64_t t, const string& suffix)
{
    const int64_t kMicroseconds = 1000 * 1000;
    os << prefix << (t / kMicroseconds) <<
        " " <<  (t % kMicroseconds) << suffix;
}

ostream&
DisplayDateTime::display(ostream& os) const
{
    char          tbuf[128];
    struct tm     gtm    = { 0 };
    const int64_t kUsecs = 1000 * 1000;
    const time_t  time   = (time_t)(mTimeUsec / kUsecs);
    if (mIsoFlag) {
        // iso 8601.
        if (strftime(tbuf, sizeof(tbuf),
            mDisplayUsecsFlag ?
                "%Y-%m-%dT%H:%M:%S." :
                "%Y-%m-%dT%H:%M:%SZ",
                gmtime_r(&time, &gtm)) <= 0) {
            tbuf[0] = '?';
            tbuf[1] = 0;
        }
    } else {
        if (strftime(tbuf, sizeof(tbuf),
            mDisplayUsecsFlag ?
                "%a %d %b %Y %H:%M:%S" :
                "%a %b %d %H:%M:%S %Y",
                localtime_r(&time, &gtm)) <= 0) {
            tbuf[0] = '?';
            tbuf[1] = 0;
        }
    }
    os << tbuf;
    if (mDisplayUsecsFlag) {
        const char              fill  = os.fill();
        const std::streamsize   width = os.width();
        const ostream::fmtflags flags = os.flags();
        os.flags(ostream::right | ostream::dec);
        os.width(6);
        os.fill(char('0'));
        os << (mTimeUsec - int64_t(time) * kUsecs);
        if (mIsoFlag) {
            os << "Z";
        }
        os.fill(fill);
        os.width(width);
        os.flags(flags);
    }
    return os;
}

/*!
 * \brief print warning message on syscall failure
 * \param[in] msg   message text
 * \param[in] use_perror pass text to perror() if true
 */
void
warn(const string& msg, bool use_perror)
{
    const int err = use_perror ? errno : 0;
    KFS_LOG_STREAM_WARN << msg <<
        (use_perror ? QCUtils::SysError(err) : string()) <<
    KFS_LOG_EOM;
}

static bool sAbortOnPanicFlag = true;

void
setAbortOnPanic(bool flag)
{
    sAbortOnPanicFlag = flag;
}

/*!
 * \brief bomb out on "impossible" error
 * \param[in] msg   panic text
 * \param[in] use_perror pass text to perror() if true
 */
void
panic(const string& msg, bool use_perror)
{
    const int err = use_perror ? errno : 0;
    KFS_LOG_STREAM_FATAL << msg <<
        (use_perror ? QCUtils::SysError(err, " ") : string()) <<
    KFS_LOG_EOM;
    MsgLogger::Stop(); // Flush log.
    if (sAbortOnPanicFlag) {
        abort();
    } else {
        _exit(1);
    }
}

const unsigned char*
char2HexTable()
{
    return HexIntParser::GetChar2Hex();
}

ostream&
resetOStream(ostream& os)
{
    os.clear();
    os.flags(ostream::dec | ostream::skipws);
    os.precision(6);
    os.width(0);
    os.fill(' ');
    os.tie(0);
    return os;
}

} // namespace KFS
