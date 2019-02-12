//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2019/02/09
// Author: Mike Ovsiannikov
//
// Copyright 2019 Quantcast Corporation. All rights reserved.
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
// Net manager watchdog interface implementation.
//
//----------------------------------------------------------------------------

#ifndef KFS_KFSIO_NET_MANAGER_WATCHER_H
#define KFS_KFSIO_NET_MANAGER_WATCHER_H

#include "common/Watchdog.h"
#include "NetManager.h"

namespace KFS
{

class NetManagerWatcher : public Watchdog::Watched
{
public:
    NetManagerWatcher(
        const char*       inNamePtr,
        const NetManager& inNetManager)
        : Watchdog::Watched(inNamePtr),
          mNetManager(inNetManager)
        {}
    virtual ~NetManagerWatcher()
        {}
    virtual uint64_t Poll() const
        { return (uint64_t)mNetManager.NowUsec(); }
private:
    const NetManager& mNetManager;
private:
    NetManagerWatcher(
        const NetManagerWatcher& inNetManagerWatcher);
    NetManagerWatcher& operator=(
        const NetManagerWatcher& inNetManagerWatcher);
};

}
#endif /* KFS_KFSIO_NET_MANAGER_WATCHER_H */
