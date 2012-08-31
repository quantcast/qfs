//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/10/03
// Author: Mike Ovsiannikov
//
// Copyright 2009 Quantcast Corp.
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

#ifndef NET_ERROR_SIMULATOR_H
#define NET_ERROR_SIMULATOR_H

namespace KFS
{
// Network error simulator. Used for testing and debugging. It is instrumental in
// discovering protocol and implementation bugs, and in particular "timing holes".
// Use in "endurance test".
//
// Error simulation spec syntax:
// sn=^[^:]*:30000$,pn=^[^:]*:20000$,a=rn+rd+wd+err+rst+rand+log,int=1,sleep|rsleep=2;<spec>;...
// sn -- perl regex to match <ip>:<port> socket name returned by getsockname()
// pn -- perl regex to match <ip>:<port> peer name returned by getpeername()
// a  -- action:
//  rn    -- read none, disable read
//  rd    -- read discard, discard input buffer data, doesn't disable read
//  wd    -- write discard, disable write, discard output buffer data
//  err   -- set poll error flag, reset all other flags
//  erd   -- when read flag set, set poll error flag, reset all other flags
//  ewr   -- when write flag set, set poll error flag, reset all other flags
//  rst   -- close connection, call NetConnection::Close()
//  rand  -- use random inteval from 0 to "int="
//  exit  -- call _exit(1)
//  abort -- call abort()
//  log   -- emit log message when action performed
// int=x    -- action interval
// sleep=x  -- sleep for x seconds
// rsleep=x -- sleep for random # of seconds in the range [0:x]
//
class NetManager;
bool NetErrorSimulatorConfigure(NetManager& inNetManager, const char* inConfigPtr = 0);
}

#endif /* NET_ERROR_SIMULATOR_H */
