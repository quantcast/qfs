//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/12/22
// Author: Mike Ovsiannikov
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
// \brief Zlib inflate.
//
//----------------------------------------------------------------------------

#ifndef KFSIO_ZLIBINFLATE_H
#define KFSIO_ZLIBINFLATE_H

#include <stddef.h>

namespace KFS
{

class ZlibInflate
{
public:
    class Output
    {
    public:
        virtual int GetBuffer(
            char*&  outBufferPtr,
            size_t& outBufferSize) = 0;
        virtual int Write(
            const char* inBufferPtr,
            size_t      inBufferSize) = 0;
    protected:
        Output()
            {}
        virtual ~Output()
            {}
    };
    ZlibInflate();
    ~ZlibInflate();
    int Run(
        const char* inBufferPtr,
        size_t      inBufferSize,
        Output&     inOutput,
        bool&       outDoneFlag);
    void Reset();
    const char* StrError(
        int inStatus);
private:
    class Impl;

    Impl& mImpl;
private:
    ZlibInflate(
        const ZlibInflate& inInflate);
    ZlibInflate& operator=(
        const ZlibInflate& inInflate);
};

}
#endif /* KFSIO_ZLIBINFLATE_H */
