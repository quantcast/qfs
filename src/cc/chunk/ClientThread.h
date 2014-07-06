//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/04/23
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
//
//----------------------------------------------------------------------------

#ifndef CLIENT_THREAD_H
#define CLIENT_THREAD_H

class QCMutex;

namespace KFS
{

class ClientSM;
class RemoteSyncSM;
class NetManager;
class RemoteSyncSM;
struct KfsOp;

class ClientThreadImpl;
class ClientThread
{
public:
    class StMutexLocker
    {
    public:
        StMutexLocker(
            ClientThread* inThreadPtr)
            : mThreadPtr(inThreadPtr)
        {
            if (mThreadPtr) {
                StMutexLocker::Lock();
            }
        }
        ~StMutexLocker()
            { StMutexLocker::Unlock(); }
        void Unlock()
        {
            if (mThreadPtr) {
                StMutexLocker::UnlockSelf();
            }
        }
    private:
        ClientThread* mThreadPtr;

        void Lock();
        void UnlockSelf();
    private:
        StMutexLocker(
            const StMutexLocker& inLocker);
        StMutexLocker& operator=(
            const StMutexLocker& inLocker);
    };

    ClientThread();
    ~ClientThread();
    void Start();
    void Stop();
    void Add(
        ClientSM& inClient);
    void Enqueue(
        RemoteSyncSM& inSyncSM,
        KfsOp&        inOp);
    void Finish(
        RemoteSyncSM& inSyncSM);
    NetManager& GetNetManager();
    static ClientThread* GetCurrentClientThreadPtr();
    static QCMutex& GetMutex();
private:
    ClientThreadImpl& mImpl;

    friend class ClientThreadImpl;
private:
    ClientThread(
        const ClientThread& inThread);
    ClientThread& operator=(
        const ClientThread& inThread);
};

}

#endif /* CLIENT_THREAD_H */

