//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/07/20
// Author: Sriram Rao
//
// Copyright 2008-2011 Quantcast Corp.
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
//
// \brief Counter for statistics gathering.
//
//----------------------------------------------------------------------------

#ifndef LIBKFSIO_COUNTER_H
#define LIBKFSIO_COUNTER_H

#include <stdint.h>

#include <algorithm>
#include <string>
#include <ostream>
#include <map>

#include "common/kfsatomic.h"

namespace KFS
{
using std::ostream;
using std::string;
using std::for_each;
using std::map;

/// Counters in KFS are currently setup to track a single "thing".
/// If we need to track multiple related items (such as, network
/// connections and how much I/O is done on them), then we need to
/// have multiple counters one for each and then display the
/// accumulated statistics in some way.
class Counter {
public:
    // XXX: add threshold values for counts

    Counter() : mName(""), mCount(0), mTimeSpent(0) { }
    Counter(const char *name) : mName(name), mCount(0), mTimeSpent(0) { }
    virtual ~Counter() { }

    /// Print out some information about this counter
    virtual void Show(ostream &os) {
        os << mName << ": " << mCount << "," << (mTimeSpent * 1e-6) << "\r\n";
    }

    void SetName(const char *name) {
        mName = name;
    }

    /// Update the counter
    virtual void Update(int64_t amount) {
        SyncAddAndFetch(mCount, amount);
    }

    virtual void UpdateTime(int64_t timeSpentMicroSec) {
        SyncAddAndFetch(mTimeSpent, timeSpentMicroSec);
    }

    virtual void Set(int64_t c) { mCount = c; }

    /// Reset the state of this counter
    virtual void Reset() { mCount = 0; mTimeSpent = 0; }

    const string& GetName() const {
        return mName;
    }
    int64_t GetValue() const {
        return mCount;
    }
protected:
    /// Name of this counter object
    string mName;
    /// Value of this counter
    volatile int64_t mCount;
    /// time related statistics
    volatile int64_t mTimeSpent;
};

class ShowCounter {
    ostream &os;
public:
    ShowCounter(ostream &o) : os(o) { }
    template<typename T>
    void operator() (const T& v) {
        v.second->Show(os);
    }
};

///
/// Counter manager that tracks all the counters in the system.  The
/// manager can be queried for statistics.
///
class CounterManager {
    typedef map<string, Counter*> CounterMap;
public:
    CounterManager()
        : mCounters()
        {}
    ~CounterManager()
        {}

    /// Add a counter object
    /// @param[in] counter   The counter to be added
    void AddCounter(Counter *counter) {
        mCounters[counter->GetName()] = counter;
    }

    /// Remove a counter object
    /// @param[in] counter   The counter to be removed
    void RemoveCounter(Counter *counter) {
        CounterMap::iterator const it = mCounters.find(counter->GetName());
        if (it != mCounters.end() && it->second == counter) {
            mCounters.erase(it);
        }
    }

    /// Given a counter's name, retrieve the associated counter
    /// object.
    /// @param[in] name   Name of the counter to be retrieved
    /// @retval The associated counter object if one exists; NULL
    /// otherwise.
    Counter *GetCounter(const string &name) {
        CounterMap::iterator const it = mCounters.find(name);
        return (it == mCounters.end() ? 0 : it->second);
    }

    /// Print out all the counters in the system, one per line.  Each
    /// line is terminated with a "\r\n".  If there are no counters,
    /// then we print "\r\n".
    void Show(ostream &os) {
        if (mCounters.empty()) {
            os << "\r\n";
            return;
        }
        for_each(mCounters.begin(), mCounters.end(), ShowCounter(os));
    }

private:
    /// Map that tracks all the counters in the system
    CounterMap  mCounters;
};

} // namespace KFS

#endif // LIBKFSIO_COUNTER_H
