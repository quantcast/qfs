//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/14
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
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

#ifndef _LIBIO_KFSCALLBACKOBJ_H
#define _LIBIO_KFSCALLBACKOBJ_H

#include <boost/static_assert.hpp>

namespace KFS
{
///
/// \file KfsCallbackObj.h
/// \brief Callback/Continuations based programming model
///
/// A KfsCallback object is based on a Continuation programming
/// model: The object executes until it makes a blocking call, at
/// which point control switches over to another object.
///
/// A continuation consists of two parts: (1) state, (2) an event
/// handler that will be called when an event occurs.  The
/// KfsCallbackObj class defined here is only a base class.
///

// abstract base class for ObjectMethod template
class ObjectMethodBase {
public:
    virtual ~ObjectMethodBase() {}
    virtual int execute(int code, void* data) const = 0;
};

//
// A derived sub-class of the KfsCallbackObj class defines its own event
// handlers.  We need to store a pointer to such an handler so that
// the callback can be invoked.  This is an implementation problem
// because: we can store a pointer in a derived class to something in
// the base class, but not vice-versa.
//
// SOOO..., create an object that holds two things: (1) the object on
// which a callback is defined, and (2) a pointer to the method in
// that object.  By doing this with templates, we preserve type-safety
// and work the magic without using any type-casting.
//
template<typename T>
class ObjectMethod : public ObjectMethodBase
{
public:
    typedef int (T::*MethodPtr)(int code, void* data);

    // save pointer to object and method
    ObjectMethod(T* optr, MethodPtr mptr)
        : mOptr(optr), mMptr(mptr)
        {}
    virtual int execute(int code, void* data) const
        { return (mOptr->*mMptr)(code, data); }
private:
    T*        const mOptr;    // pointer to the object
    MethodPtr const mMptr;    // pointer to the method
private:
    ObjectMethod(const ObjectMethod&);
    ObjectMethod& operator=(const ObjectMethod&);
};

///
/// \brief Sets the event handler for a callback object.
/// @param pobj Pointer to the KfsCallback object
/// @param meth Pointer to the handler method in the KfsCallbackObj
///
template<typename T>
void SET_HANDLER( T* pobj, typename ObjectMethod<T>::MethodPtr meth )
{
   pobj->SetHandler(pobj, meth);
}

///
/// \class KfsCallbackObj
/// A callback object has state and an event handler that will be invoked
/// whenever an event occurs for this callback object.
///
class KfsCallbackObj
{
public:
    KfsCallbackObj()
        : mObjMeth(0)
        {}
    virtual ~KfsCallbackObj();
    ///
    /// Signature for an event handler:
    /// @param code An integer about the event that occurred
    /// @param data A pointer to the data associated with the event
    ///
    int HandleEvent(int code, void *data)
        { return mObjMeth->execute(code, data); }

    template<typename T>
    void SetHandler(T* pobj, typename ObjectMethod<T>::MethodPtr meth)
    {
        BOOST_STATIC_ASSERT(sizeof(ObjectMethod<T>) <= sizeof(mObjMethodStorage));
        if (mObjMeth) {
            mObjMeth->~ObjectMethodBase();
        }
        mObjMeth = ::new (&mObjMethodStorage) ObjectMethod<T>(pobj, meth);
    }
private:
    struct {
        size_t mStorage[
            (sizeof(ObjectMethod<ObjectMethodBase>) + sizeof(size_t) - 1) /
            sizeof(size_t)
        ];
    } mObjMethodStorage;
    const ObjectMethodBase* mObjMeth;
private:
    KfsCallbackObj(const KfsCallbackObj&);
    KfsCallbackObj& operator=(const KfsCallbackObj&);
};

}

#endif // _LIBIO_KFSCALLBACKOBJ_H
