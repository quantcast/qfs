//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/07/26
// Author: Mike Ovsiannikov
//
// Copyright 2013 Quantcast Corp.
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
// \file AuthContext.h
//
//----------------------------------------------------------------------------

#ifndef AUTH_CONTEXT_H
#define AUTH_CONTEXT_H

#include <string>

namespace KFS
{
using std::string;

struct MetaAuthenticate;
class Properties;

class AuthContext
{
public:
    AuthContext();
    ~AuthContext();
    bool Validate(
        MetaAuthenticate& inOp);
    bool Authenticate(
        MetaAuthenticate& inOp);
    bool RemapAndValidate(
        string& ioAuthName) const;
    bool SetParameters(
        const char*       inParamNamePrefixPtr,
        const Properties& inParameters);
private:
    class Impl;
    Impl& mImpl;

private:
    AuthContext(
        const AuthContext& inCtx);
    AuthContext& operator=(
        const AuthContext& inCtx);
};

}

#endif /* AUTH_CONTEXT_H */
