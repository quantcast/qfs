//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/05/11
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
// Veiwstamped replications RPCs..
//
//
//----------------------------------------------------------------------------

#ifndef META_VROPS_H
#define META_VROPS_H

#include "common/kfstypes.h"

#include "MetaRequest.h"
#include "MetaVrSM.h"
#include "util.h"

#include <errno.h>

namespace KFS
{
class Properties;

class MetaVrRequest : public MetaRequest
{
public:
    MetaVrRequest(
        MetaOp    inOpType,
        LogAction inLogAction,
        seq_t     inOpSequence = -1)
        : MetaRequest(inOpType, inLogAction, inOpSequence),
          mEpochSeq(-1),
          mViewSeq(-1),
          mCommitSeq(-1),
          mVrSmPtr(0),
          mRefCount(0)
    {
        MetaVrRequest::Ref();
        shortRpcFormatFlag = false;
    }

    seq_t mEpochSeq;
    seq_t mViewSeq;
    seq_t mCommitSeq;

    bool Validate() const
    {
        return (0 <= mEpochSeq && 0 <= mViewSeq);
    }
    template<typename T>
    static T& ParserDef(
        T& inParser)
    {
        return MetaRequest::ParserDef(inParser)
        .Def("E", &MetaVrRequest::mEpochSeq,  seq_t(-1))
        .Def("V", &MetaVrRequest::mViewSeq,   seq_t(-1))
        .Def("C", &MetaVrRequest::mCommitSeq, seq_t(-1))
        ;
    }
    template<typename T>
    static T& LogIoDef(
        T& inParser)
    {
        return MetaRequest::LogIoDef(inParser)
        .Def("E", &MetaVrRequest::mEpochSeq,  seq_t(-1))
        .Def("V", &MetaVrRequest::mViewSeq,   seq_t(-1))
        .Def("C", &MetaVrRequest::mCommitSeq, seq_t(-1))
        ;
    }
    virtual bool start()
    {
        if (0 == status && ! Validate()) {
            status = -EINVAL;
        }
        return (0 == status);
    }
    virtual void HandleResponse(
        seq_t             inSeq,
        const Properties& inProps) = 0;
    void Ref()
        { mRefCount++; }
    void Unref()
    {
        if (--mRefCount <= 0) {
            delete this;
        }
    }
    virtual void handle()
        { /* nothing */ }
protected:
    MetaVrSM* mVrSmPtr;
    int       mRefCount;

    virtual ~MetaVrRequest()
    {
        if (0 != mRefCount) {
            panic("~MetaVrRequest: invalid ref count");
        }
    }
    bool ResponseHeader(
        ReqOstream& inOs);
    virtual void response(
        ReqOstream& inOs)
    {
        ResponseHeader(inOs);
        inOs << "\r\n";
    }
    template<typename T>
    void HandleReply(
        T&                inReq,
        seq_t             inSeq,
        const Properties& inProps)
    {
        if (mVrSmPtr) {
            mVrSmPtr->HandleReply(inReq, inSeq, inProps);
        }
    }
    virtual void ReleaseSelf()
        { Unref(); }
private:
    MetaVrRequest(
        const MetaVrRequest& inRequest);
    MetaVrRequest& operator=(
        const MetaVrRequest& inRequest);
};

class MetaVrStartViewChange : public MetaVrRequest
{
public:
    MetaVrStartViewChange()
        : MetaVrRequest(META_VR_START_VIEW_CHANGE, kLogIfOk)
        {}
    virtual ostream& ShowSelf(
        ostream& inOs) const
    {
        return (inOs <<
            "vr-start-view-change" <<
            " epoch: "  << mEpochSeq <<
            " view: "   << mViewSeq <<
            " commit: " << mCommitSeq
        );
    }
    virtual void HandleResponse(
        seq_t             inSeq,
        const Properties& inProps)
        { HandleReply(*this, inSeq, inProps); }
protected:
    virtual ~MetaVrStartViewChange()
        {}
};

class MetaVrDoViewChange : public MetaVrRequest
{
public:
    MetaVrDoViewChange()
        : MetaVrRequest(META_VR_DO_VIEW_CHANGE, kLogIfOk)
        {}
    virtual ostream& ShowSelf(
        ostream& inOs) const
    {
        return (inOs <<
            "vr-do-view-change" <<
            " epoch: "  << mEpochSeq <<
            " view: "   << mViewSeq <<
            " commit: " << mCommitSeq
        );
    }
    virtual void HandleResponse(
        seq_t             inSeq,
        const Properties& inProps)
        { HandleReply(*this, inSeq, inProps); }
protected:
    virtual ~MetaVrDoViewChange()
        {}
};

class MetaVrStartView : public MetaVrRequest
{
public:
    MetaVrStartView()
        : MetaVrRequest(META_VR_START_VIEW, kLogIfOk)
        {}
    virtual ostream& ShowSelf(
        ostream& inOs) const
    {
        return (inOs <<
            "vr-start-view" <<
            " epoch: "  << mEpochSeq <<
            " view: "   << mViewSeq <<
            " commit: " << mCommitSeq
        );
    }
    virtual void HandleResponse(
        seq_t             inSeq,
        const Properties& inProps)
        { HandleReply(*this, inSeq, inProps); }
protected:
    virtual ~MetaVrStartView()
        {}
};

class MetaVrReconfiguration : public MetaVrRequest
{
public:
    MetaVrReconfiguration()
        : MetaVrRequest(META_VR_RECONFIGURATION, kLogIfOk)
        {}
    virtual ostream& ShowSelf(
        ostream& inOs) const
    {
        return (inOs <<
            "vr-reconfiguration" <<
            " epoch: "  << mEpochSeq <<
            " view: "   << mViewSeq <<
            " commit: " << mCommitSeq
        );
    }
    virtual void HandleResponse(
        seq_t             inSeq,
        const Properties& inProps)
        { HandleReply(*this, inSeq, inProps); }
protected:
    virtual ~MetaVrReconfiguration()
        {}
};

class MetaVrStartEpoch : public MetaVrRequest
{
public:
    MetaVrStartEpoch()
        : MetaVrRequest(META_VR_START_EPOCH, kLogIfOk)
        {}
    virtual ostream& ShowSelf(
        ostream& inOs) const
    {
        return (inOs <<
            "vr-start-epoch" <<
            " epoch: "  << mEpochSeq <<
            " view: "   << mViewSeq <<
            " commit: " << mCommitSeq
        );
    }
    virtual void HandleResponse(
        seq_t             inSeq,
        const Properties& inProps)
        { HandleReply(*this, inSeq, inProps); }
protected:
    virtual ~MetaVrStartEpoch()
        {}
};

} // namespace KFS

#endif /* META_VROPS_H */
