//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/06/21
// Author: Mike Ovsiannikov
//
// Copyright 2011 Quantcast Corp.
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

#ifndef VALUE_SAMPLER_H
#define VALUE_SAMPLER_H

#include <inttypes.h>

namespace KFS
{

// Sample / history buffer. Put puts current value and the corresponding time
// stamp into the buffer.
// Can be used to implement filters, compute average, etc.
class ValueSampler
{
public:
    typedef int64_t Sample;
    typedef int64_t Time;

    ValueSampler(
        int    inMaxSamples,
        Sample inSample,
        Time   inTime)
        : mMaxSamples(0),
          mCurIdx(0),
          mSamplesPtr(0),
          mTotal(0)
        { ValueSampler::SetMaxSamples(inMaxSamples, inSample, inTime); }
    ~ValueSampler()
        { delete [] mSamplesPtr; }
    void Put(
        Sample inSample,
        Time   inTime)
    {
        SampleEntry& theEntry = mSamplesPtr[mCurIdx];
        mTotal -= theEntry.mSample;
        mTotal += inSample;
        theEntry.mSample = inSample;
        theEntry.mTime   = inTime;
        if (++mCurIdx >= mMaxSamples) {
            mCurIdx = 0;
        }
    }
    int GetMaxSamples() const
        { return mMaxSamples; }
    Time GetTimeIterval() const
    {
        return (mSamplesPtr[GetLastIdx()].mTime -
            mSamplesPtr[GetFirstIdx()].mTime);
    }
    Sample GetTotal() const
        { return mTotal; }
    Sample GetLastFirstDiffByTime() const
    {
        SampleEntry& theFirst = mSamplesPtr[GetFirstIdx()];
        SampleEntry& theLast  = mSamplesPtr[GetLastIdx()];
        const Time theTime = theLast.mTime - theFirst.mTime;
        if (theTime <= 0) {
            return 0;
        }
        return ((theLast.mSample - theFirst.mSample) / theTime);
    }
    void SetMaxSamples(
        int    inMaxSamples,
        Sample inSample,
        Time   inTime)
    {
        if (inMaxSamples == mMaxSamples && mMaxSamples > 0) {
            return;
        }
        mMaxSamples = inMaxSamples > 0 ? inMaxSamples : 1;
        delete [] mSamplesPtr;
        mSamplesPtr = new SampleEntry[mMaxSamples];
        Reset(inSample, inTime);
    }
    void Reset(
        Sample inSample,
        Time   inTime)
    {
        for (int i = 0; i < mMaxSamples; i++) {
            mSamplesPtr[i].mSample = inSample;
            mSamplesPtr[i].mTime   = inTime;
            mTotal += inSample;
        }
    }
    void GetLastFirstDiff(
        Sample& outSampleDiff,
        Time&   outTimeDiff) const
    {
        SampleEntry& theFirst = mSamplesPtr[GetFirstIdx()];
        SampleEntry& theLast  = mSamplesPtr[GetLastIdx()];
        outTimeDiff   = theLast.mTime   - theFirst.mTime;
        outSampleDiff = theLast.mSample - theFirst.mSample;
    }
private:
    struct SampleEntry
    {
        SampleEntry()
            : mSample(),
              mTime()
            {}
        Sample mSample;
        Time   mTime;
    };

    int          mMaxSamples;
    int          mCurIdx;
    SampleEntry* mSamplesPtr;
    Sample       mTotal;

    int GetFirstIdx() const
        { return mCurIdx; }
    int GetLastIdx() const
        { return ((mCurIdx > 0 ? mCurIdx : mMaxSamples) - 1); }
private:
    ValueSampler(
        const ValueSampler& inSampler);
    ValueSampler& operator=(
        const ValueSampler& inSampler);
};

} // namespace KFS

#endif /* VALUE_SAMPLER_H */
