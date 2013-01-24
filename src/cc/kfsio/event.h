//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/22
// Author: Sriram Rao
//
// Copyright 2008-2010 Quantcast Corp.
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
//----------------------------------------------------------------------------

#ifndef _LIBKFSIO_EVENT_H
#define _LIBKFSIO_EVENT_H

#include "KfsCallbackObj.h"

namespace KFS
{
///
/// \enum EventCode_t
/// Various event codes that a KfsCallbackObj is notified with when
/// events occur.
///
enum EventCode_t {
    EVENT_NEW_CONNECTION,
    EVENT_NET_READ,
    EVENT_NET_WROTE,
    EVENT_NET_ERROR,
    EVENT_DISK_READ,
    EVENT_DISK_WROTE,
    EVENT_DISK_ERROR,
    EVENT_UNUSED_0,
    EVENT_CMD_DONE,
    EVENT_INACTIVITY_TIMEOUT,
    EVENT_TIMEOUT,
    EVENT_DISK_DELETE_DONE,
    EVENT_DISK_RENAME_DONE,
    EVENT_DISK_GET_FS_SPACE_AVAIL_DONE,
    EVENT_DISK_CHECK_DIR_READABLE_DONE
};

}

#endif // _LIBKFSIO_EVENT_H
