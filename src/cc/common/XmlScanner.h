//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/10/24
// Author: Mike Ovsainnikov
//
// Copyright 2015,2016 Quantcast Corporation. All rights reserved.
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
// Minimalistic xml scanner, intended for "values" extraction, and very basic /
// minimalistic validation.
// CDATA is not supported,
//
//----------------------------------------------------------------------------

#ifndef KFS_COMMON_XML_SCANNER_H
#define KFS_COMMON_XML_SCANNER_H

namespace KFS
{

class XmlScanner
{
public:
    enum State
    {
        kStateNone,
        kStateTag,
        kStateTagName,
        kStateTagNameEnd,
        kStateAttr,
        kStateEmptyTag,
        kStateValue,
        kStateCloseTag,
        kStateCloseTagEnd,
        kStateSkipStart,
        kStateCommentStart,
        kStateComment,
        kStateCommentEnd,
        kStateCommentEnd1,
        kStateProlog,
        kStatePrologEnd,
        kStateSkipEnd,
        kStateSkipCloseBracket,
        kStateSkipCloseBracketEndTag,
    };
    template<typename IT, typename FT>
    static bool Scan(
        IT& inIterator,
        FT& inFunc)
    {
        const int kSpace     = ' ';
        int       thePrevSym = -1;
        State     theState   = kStateNone;
        State     theFState  = kStateNone;
        int       theSym;
        while (0 <= (theSym = inIterator.Next())) {
            switch (theState) {
                case kStateNone:
                    if (theSym <= kSpace) {
                        break;
                    }
                    if ('<' == theSym) {
                        theState = kStateTag;
                        break;
                    }
                    return false;
                case kStateTag:
                    if ('!' == theSym) {
                        theState = kStateSkipStart;
                        break;
                    }
                    theFState = kStateNone;
                    if ('/' == theSym) {
                        theState = kStateCloseTag;
                        break;
                    }
                    if ('?' == theSym) {
                        theState = kStateProlog;
                        break;
                    }
                    theState = kStateTagName;
                    // Fall through
                case kStateTagName:
                    if (theSym <= kSpace || '/' == theSym) {
                        theState = '/' == theSym ? kStateEmptyTag : kStateAttr;
                        if (inFunc(kStateTagNameEnd, theSym)) {
                            break;
                        }
                        return false;
                    }
                    // Fall through
                case kStateAttr:
                    if ('>' == theSym) {
                        if ('/' == thePrevSym) {
                            theState  = kStateNone;
                            theFState = kStateEmptyTag;
                        } else {
                            theState  = kStateValue;
                            theFState = kStateTagNameEnd;
                        }
                    } else {
                        theFState = theState;
                    }
                    if (inFunc(theFState, theSym)) {
                        break;
                    }
                    return false;
                case kStateEmptyTag:
                    if ('>' != theSym) {
                        return false;
                    }
                    theState = kStateNone;
                    if (inFunc(kStateEmptyTag, theSym)) {
                        break;
                    }
                    return false;
                case kStateValue:
                    if ('<' == theSym) {
                        theState  = kStateTag;
                        break;
                    }
                    theFState = theState;
                    if (inFunc(theFState, theSym)) {
                        break;
                    }
                    return false;
                case kStateCloseTag:
                    if ('>' == theSym) {
                        theState  = kStateNone;
                        theFState = kStateCloseTagEnd;
                    } else {
                        if (theSym <= kSpace) {
                            break;
                        }
                        if (thePrevSym <= kSpace) {
                            return false;
                        }
                        theFState = theState;
                    }
                    if (inFunc(theFState, theSym)) {
                        break;
                    }
                    return false;
                case kStateSkipStart:
                    if ('-' == theSym) {
                        theState = kStateCommentStart;
                    } else {
                        theState  = kStateSkipEnd;
                        theFState = kStateNone;
                    }
                    break;
                case kStateCommentStart:
                    if ('-' != theSym) {
                        return false;
                    }
                    theState = kStateComment;
                    break;
                case kStateComment:
                    if ('-' == theSym) {
                        theState = kStateCommentEnd;
                    }
                    break;
                case kStateCommentEnd:
                    if ('-' == theSym) {
                        theState = kStateCommentEnd1;
                        break;
                    }
                    theState = kStateComment;
                    break;
                case kStateCommentEnd1:
                    if ('>' != theSym) {
                        return false;
                    }
                    theState = kStateValue == theFState ?
                            kStateValue : kStateNone;
                    break;
                case kStateProlog:
                    if ('?' == theSym) {
                        theState = kStatePrologEnd;
                    }
                    if (inFunc(theState, theSym)) {
                        break;
                    }
                    break;
                case kStatePrologEnd:
                    if ('>' != theSym) {
                        return false;
                    }
                    theState = kStateNone;
                    break;
                case kStateSkipEnd:
                    if ('[' == theSym) {
                        theState = kStateSkipCloseBracket;
                        break;
                    }
                    if ('>' == theSym) {
                        theState = kStateNone;
                    }
                    break;
                case kStateSkipCloseBracket:
                    if (']' == theSym) {
                        theState = kStateSkipCloseBracketEndTag;
                    }
                    break;
                case kStateSkipCloseBracketEndTag:
                    if ('>' == theSym) {
                        theState = kStateNone;
                        break;
                    }
                    if (kSpace < theSym) {
                        return false;
                    }
                    break;
                default:
                    return false;
            }
            thePrevSym = theSym;
        }
        return (kStateNone == theState);
    }
    template<typename StringT, typename FuncT, char PathSeparator = '/'>
    class KeyValueFunc
    {
    public:
        typedef StringT                     String;
        typedef typename StringT::size_type Size;

        KeyValueFunc(
            FuncT&  inFunc,
            String& inKeyBuf,
            String& inValueBuf,
            Size    inMaxKeySize,
            Size    inMaxValueSize)
            : mFunc(inFunc),
              mCloseTagPos(String::npos),
              mCurCloseTagPos(String::npos),
              mMaxKeySize(inMaxKeySize),
              mMaxValueSize(inMaxValueSize),
              mPrevState(kStateNone),
              mLeafFlag(false),
              mKey(inKeyBuf),
              mValue(inValueBuf)
        {
            mKey.clear();
            mValue.clear();
        }
        void Reset()
        {
            mKey.clear();
            mValue.clear();
            mCloseTagPos    = String::npos;
            mCurCloseTagPos = String::npos;
            mPrevState      = kStateNone;
            mLeafFlag       = false;
        }
        template<typename IT>
        bool Scan(
            IT& inIterator)
            { return XmlScanner::Scan(inIterator, *this); }
        bool operator()(
            State inState,
            int   inSym)
        {
            const State thePrevState = mPrevState;
            mPrevState = inState;
            if (kStateCloseTag != inState &&
                    String::npos != mCurCloseTagPos &&
                    ! CloseTag()) {
                return false;
            }
            switch (inState) {
                case kStateEmptyTag:
                case kStateCloseTag:
                    if (String::npos == mCloseTagPos) {
                        if (mKey.empty()) {
                            return false;
                        }
                        mCloseTagPos    = mKey.rfind(PathSeparator);
                        mCurCloseTagPos = mCloseTagPos;
                        if (String::npos == mCloseTagPos) {
                            return false;
                        }
                    }
                    if (kStateEmptyTag == inState) {
                        if (! CloseEmptyTag()) {
                            return false;
                        }
                        break;
                    }
                    mCurCloseTagPos++;
                    if (mKey.size() <= mCloseTagPos) {
                        return false;
                    }
                    if (inSym != mKey[mCurCloseTagPos]) {
                        return false;
                    }
                    break;
                case kStateTagName:
                    if (mMaxKeySize <= mKey.size()) {
                        return false;
                    }
                    if (thePrevState != inState) {
                        mKey     += PathSeparator;
                        mLeafFlag = true;
                    }
                    mKey += (char)inSym;
                    break;
                case kStateValue:
                    if (thePrevState != inState) {
                        mValue.clear();
                    }
                    if (mMaxValueSize <= mKey.size()) {
                        return false;
                    }
                    mValue += (char)inSym;
                    break;
                case kStateProlog:
                    if (! mKey.empty()) {
                        return false;
                    }
                    break;
                default:
                    break;
            }
            return true;
        }
        const String& GetKey() const
            { return mKey; }
        const String& GetValue() const
            { return mValue; }
        bool IsLeaf() const
            { return mLeafFlag; }
    protected:
        FuncT&  mFunc;
        Size    mCloseTagPos;
        Size    mCurCloseTagPos;
        Size    mMaxKeySize;
        Size    mMaxValueSize;
        State   mPrevState;
        bool    mLeafFlag;
        String& mKey;
        String& mValue;

        bool CloseEmptyTag()
        {
            if (mLeafFlag && ! mFunc(mKey, mValue)) {
                return false;
            }
            mValue.clear();
            mKey.erase(mCloseTagPos);
            mCurCloseTagPos = String::npos;
            mCloseTagPos    = String::npos;
            mLeafFlag       = false;
            return true;
        }
        bool CloseTag()
            { return (mCurCloseTagPos + 1 == mKey.size() && CloseEmptyTag()); }
    private:
        KeyValueFunc(
            const KeyValueFunc& inFunc);
        KeyValueFunc& operator=(
            const KeyValueFunc& inFunc);
    };
};

} // namespace KFS

#endif /* KFS_COMMON_XML_SCANNER_H */
