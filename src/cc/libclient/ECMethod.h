//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/08/09
// Author: Mike Ovsiannikov
//
// Copyright 2014 Quantcast Corp.
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
// Erasure code method interface.
//
//----------------------------------------------------------------------------

#ifndef KFS_LIBCLIENT_ECMETHOD_H
#define KFS_LIBCLIENT_ECMETHOD_H

#include <string>

namespace KFS
{
namespace client
{
using std::string;

class ECMethod
{
public:
    class Encoder
    {
    public:
        virtual int Encode(
            int    inStripeCount,
            int    inRecoveryStripeCount,
            int    inLength,
            void** inBuffersPtr) = 0;
        virtual void Release() = 0;
    protected:
        Encoder()
            {}
        Encoder(
            const Encoder& /* inEncoder */)
            {}
        virtual ~Encoder()
            {}
        Encoder& operator=(
            const Encoder& /* inEncoder */)
            { return *this; }
    };

    class Decoder
    {
    public:
        virtual int Decode(
            int        inStripeCount,
            int        inRecoveryStripeCount,
            int        inLength,
            void**     inBuffersPtr,
            int const* inMissingStripesIdx) = 0;
        virtual void Release() = 0;
        virtual bool SupportsOneRecoveryStripeRebuild() const = 0;
    protected:
        Decoder()
            {}
        Decoder(
            const Encoder& /* inEncoder */)
            {}
        virtual ~Decoder()
            {}
        Decoder& operator=(
            const Encoder& /* inEncoder */)
            { return *this; }
    };

    static int InitAll(); // Returns number of methods.
    static bool IsValid(
        int     inMethodType,
        int     inStripeCount,
        int     inRecoveryStripeCount,
        string* outErrMsgPtr);
    static Encoder* FindEncoder(
        int     inMethodType,
        int     inStripeCount,
        int     inRecoveryStripeCount,
        string* outErrMsgPtr);
    static Decoder* FindDecoder(
        int     inMethodType,
        int     inStripeCount,
        int     inRecoveryStripeCount,
        string* outErrMsgPtr);
    static string FindDescription(
        int     inMethodType,
        string* outErrMsgPtr);
protected:
    ECMethod();
    virtual ~ECMethod();
    bool Register(
        int inMethodType);
    void Unregister(
        int inMethodType);
    virtual bool Init(
        int inMethodType) = 0;
    virtual void Release(
        int inMethodType) = 0;
    virtual string GetDescription() const = 0;
    virtual bool Validate(
        int     inMethodType,
        int     inStripeCount,
        int     inRecoveryStripeCount,
        string* outErrMsgPtr) = 0;
    virtual Encoder* GetEncoder(
        int     inMethodType,
        int     inStripeCount,
        int     inRecoveryStripeCount,
        string* outErrMsgPtr) = 0;
    virtual Decoder* GetDecoder(
        int     inMethodType,
        int     inStripeCount,
        int     inRecoveryStripeCount,
        string* outErrMsgPtr) = 0;
protected:
    ECMethod(
        const ECMethod& /* inMethod */)
        {}
    ECMethod& operator=(
        const ECMethod& /* inMethod */)
        { return* this; }
private:
    static ECMethod* Find(
        int     inMethodType,
        string* outErrMsgPtr);
    inline static int RegisterAllMethods();
    static int InitAllSelf();
};

}} /* namespace client KFS */

#endif /* KFS_LIBCLIENT_ECMETHOD_H */
