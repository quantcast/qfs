//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/07/14
// Author: Mike Ovsiannikov
//
// Copyright 2016 Quantcast Corp.
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
// Viewstamped replication transaction log sequence number.
//
//----------------------------------------------------------------------------

#ifndef KFS_META_VR_LOG_SEQ_H
#define KFS_META_VR_LOG_SEQ_H

#include "common/kfstypes.h"

namespace KFS
{

class MetaVrLogSeq
{
public:
    MetaVrLogSeq()
        : mEpochSeq(-1),
          mViewSeq(-1),
          mLogSeq(-1)
        {}
    MetaVrLogSeq(
        seq_t inEpochSeq,
        seq_t inViewSeq,
        seq_t inLogSeq)
        : mEpochSeq(inEpochSeq),
          mViewSeq(inViewSeq),
          mLogSeq(inLogSeq)
        {}
    bool IsValid() const
    {
        return (0 <= mEpochSeq && 0 <= mViewSeq && 0 <= mLogSeq);
    }
    bool operator<(
        const MetaVrLogSeq& inRhs) const
    {
        return (
            mEpochSeq < inRhs.mEpochSeq || (
            mEpochSeq == inRhs.mEpochSeq && (
                mViewSeq < inRhs.mViewSeq || (
                mViewSeq == inRhs.mViewSeq &&
                mLogSeq < inRhs.mLogSeq)
        )));
    }
    bool operator<=(
        const MetaVrLogSeq& inRhs) const
    {
        return ! (inRhs < *this);
    }
    bool operator>(
        const MetaVrLogSeq& inRhs) const
    {
        return (inRhs < *this);
    }
    bool operator>=(
        const MetaVrLogSeq& inRhs) const
    {
        return ! (*this < inRhs);
    }
    bool operator==(
        const MetaVrLogSeq& inRhs) const
    {
        return (
            mEpochSeq == inRhs.mEpochSeq &&
            mViewSeq  == inRhs.mViewSeq &&
            mLogSeq   == inRhs.mLogSeq
        );
    }
    bool operator!=(
        const MetaVrLogSeq& inRhs) const
    {
        return ! (*this == inRhs);
    }
    template<typename OST>
    OST& Display(
        OST& inStream) const
    {
        return (inStream << mEpochSeq << " " << mViewSeq << " " << mLogSeq);
    }
    template<typename T>
    bool Parse(
        const char*& ioPtr,
        size_t       inLen,
        T*           inParserTypePtr = 0)
    {
        const char* const theEndPtr = ioPtr + inLen;
        return (
            T::Parse(ioPtr, theEndPtr - ioPtr, mEpochSeq) &&
            T::Parse(ioPtr, theEndPtr - ioPtr, mViewSeq)  &&
            T::Parse(ioPtr, theEndPtr - ioPtr, mLogSeq)
        );
    }

    seq_t mEpochSeq;
    seq_t mViewSeq;
    seq_t mLogSeq;
};

template<typename OST>
    OST&
operator<<(
    OST&                inStream,
    const MetaVrLogSeq& inSeq)
{
    return inSeq.Display(inStream);
}

} // namespace KFS

#endif /* KFS_META_VR_LOG_SEQ_H */
