/*!
 * $Id$
 *
 * \file Key.h
 * \brief Base class for KFS metadata key operations.
 * \author Blake Lewis (Kosmix Corp.)
 *         Mike Ovsiannikov pack keys into 128 bits by  using the fact that
 *         that the chunk offset is 64MB aligned.
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
#if !defined(META_KEY_H)
#define META_KEY_H

#include "kfstypes.h"

namespace KFS {

typedef int64_t KeyData;    //!< "opaque" key data

/*!
 * \brief search key
 *
 * Key values for tree operations.
 */
class Key
{
public:
    Key(MetaType k, KeyData d1, KeyData d2 = 0) {
        hi = uint64_t(k & 0x30) << (62 - 4);
        const uint64_t dh = (uint64_t)(d1 - (int64_t(1) << 63));
        hi |= (dh >> 2) & ~(uint64_t(3) << 62);
        lo = dh << 62;
        const uint64_t dl = (uint64_t)(d2 - (int64_t(1) << 63));
        lo |= (dl >> 2) & (~uint64_t(3));
        lo |= uint64_t(k & 0x3);
    }
    Key() {
        *this = Key(KFS_UNINIT, 0, 0);
    }
    bool operator < (const Key &test) const
        { return (hi < test.hi || (hi == test.hi && lo < test.lo)); }
    bool operator == (const Key &test) const
        { return (((hi ^ test.hi) | (lo ^ test.lo)) == 0); }
    bool operator != (const Key &test) const
        { return ! (*this == test); }
    bool operator > (const Key &test) const
        { return (test < *this); }
    bool operator <= (const Key &test) const
        { return ! (*this > test); }
    bool operator >= (const Key &test) const
        { return ! (*this < test); }
private:
    uint64_t hi;
    uint64_t lo;
    friend class PartialMatch;
};

class PartialMatch
{
private:
    static const uint64_t mask = (uint64_t(3) << 62) | uint64_t(3);
    Key key;
public:
    PartialMatch(MetaType k, KeyData d1)
        : key(k, d1)
        {}
    bool operator < (const Key &test) const {
        return (key.hi < test.hi || (key.hi == test.hi &&
            (key.lo & mask) < (test.lo & mask)));
    }
    bool operator > (const Key &test) const {
        return (key.hi > test.hi || (key.hi == test.hi &&
            (key.lo & mask) > (test.lo & mask)));
    }
    bool operator == (const Key &test) const {
        return (((key.hi ^ test.hi) |
            ((key.lo ^ test.lo) & mask)) == 0);
    }
    bool operator != (const Key &test) const
        { return ! (*this == test); }
    bool operator <= (const Key &test) const
        { return ! (*this > test); }
    bool operator >= (const Key &test) const
        { return ! (*this < test); }
};

inline bool operator < (const Key &l, const PartialMatch &r) {
    return r > l;
}
inline bool operator > (const Key &l, const PartialMatch &r) {
    return r < l;
}
inline bool operator == (const Key &l, const PartialMatch &r) {
    return r == l;
}
inline bool operator != (const Key &l, const PartialMatch &r) {
    return r != l;
}
inline bool operator <= (const Key &l, const PartialMatch &r) {
    return r >= l;
}
inline bool operator >= (const Key &l, const PartialMatch &r) {
    return r <= l;
}

}

#endif  // !defined(META_KEY_H)
