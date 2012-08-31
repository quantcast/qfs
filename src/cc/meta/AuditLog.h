//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/04/10
// Author: Mike Ovsiannikov.
//
// Copyright 2012 Quantcast Corp.
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
// \file AuditLog.h
// \brief Kfs meta server audit log interface. Writes every client request into
// audit log file.
//
//----------------------------------------------------------------------------

#ifndef AUDIT_LOG_H
#define AUDIT_LOG_H

namespace KFS
{

struct MetaRequest;
class Properties;

class AuditLog
{
public:
    static void Log(const MetaRequest& inOp);
    static void SetParameters(const Properties& inProps);
    static void Stop();
    static void PrepareToFork();
    static void ForkDone();
    static void ChildAtFork();
};

};

#endif /* AUDIT_LOG_H */
