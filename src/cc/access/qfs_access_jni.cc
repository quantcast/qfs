//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2007/08/24
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
// Copyright 2007-2008 Kosmix Corp.
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
// \brief JNI code in C++ world for accesing KFS Client.
//
//----------------------------------------------------------------------------

#include <jni.h>
#include <string>
#include <cstddef>
#include <iostream>
#include <vector>
#include <netinet/in.h>
#include <sstream>
#include <errno.h>

using std::vector;
using std::string;
using std::cout;
using std::endl;
using std::ostringstream;

#include <fcntl.h>
#include "libclient/KfsClient.h"
using namespace KFS;

extern "C" {
    jlong Java_com_quantcast_qfs_access_KfsAccess_initF(
        JNIEnv *jenv, jclass jcls, jstring jpath);

    jlong Java_com_quantcast_qfs_access_KfsAccess_initS(
        JNIEnv *jenv, jclass jcls, jstring jmetaServerHost, jint metaServerPort);

    void Java_com_quantcast_qfs_access_KfsAccess_destroy(
        JNIEnv *jenv, jclass jcls, jlong jptr);

    jint Java_com_quantcast_qfs_access_KfsAccess_cd(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jint Java_com_quantcast_qfs_access_KfsAccess_mkdirs(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jint mode);

    jint Java_com_quantcast_qfs_access_KfsAccess_rmdir(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jint Java_com_quantcast_qfs_access_KfsAccess_compareChunkReplicas(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jobject stringbuffermd5);

    jint Java_com_quantcast_qfs_access_KfsAccess_rmdirs(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jobjectArray Java_com_quantcast_qfs_access_KfsAccess_readdirplus(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jobjectArray Java_com_quantcast_qfs_access_KfsAccess_readdir(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jboolean jpreloadattr);

    jint Java_com_quantcast_qfs_access_KfsAccess_remove(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jint Java_com_quantcast_qfs_access_KfsAccess_rename(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring joldpath, jstring jnewpath,
        jboolean joverwrite);

    jint Java_com_quantcast_qfs_access_KfsAccess_exists(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jint Java_com_quantcast_qfs_access_KfsAccess_isFile(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jint Java_com_quantcast_qfs_access_KfsAccess_isDirectory(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jlong Java_com_quantcast_qfs_access_KfsAccess_filesize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jobjectArray Java_com_quantcast_qfs_access_KfsAccess_getDataLocation(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jlong jstart, jlong jlen);

    jshort Java_com_quantcast_qfs_access_KfsAccess_getReplication(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jshort Java_com_quantcast_qfs_access_KfsAccess_setReplication(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jint jnumReplicas);

    jlong Java_com_quantcast_qfs_access_KfsAccess_getModificationTime(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    jint Java_com_quantcast_qfs_access_KfsAccess_setModificationTime(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jlong jmsec);

    jint Java_com_quantcast_qfs_access_KfsAccess_open(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jstring jmode, jint jnumReplicas,
        jint jnumStripes, jint jnumRecoveryStripes, jint jstripeSize, jint jstripedType, jint jcreateMode);

    jint Java_com_quantcast_qfs_access_KfsAccess_create(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jint jnumReplicas, jboolean jexclusive,
        jint jnumStripes, jint jnumRecoveryStripes, jint jstripeSize, jint jstripedType,
        jboolean foreceType, jint mode);

    jlong Java_com_quantcast_qfs_access_KfsAccess_setDefaultIoBufferSize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jlong jsize);

    jlong Java_com_quantcast_qfs_access_KfsAccess_getDefaultIoBufferSize(
        JNIEnv *jenv, jclass jcls, jlong jptr);

    jlong Java_com_quantcast_qfs_access_KfsAccess_setDefaultReadAheadSize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jlong jsize);

    jlong Java_com_quantcast_qfs_access_KfsAccess_getDefaultReadAheadSize(
        JNIEnv *jenv, jclass jcls, jlong jptr);

    jlong Java_com_quantcast_qfs_access_KfsAccess_setIoBufferSize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong jsize);

    jlong Java_com_quantcast_qfs_access_KfsAccess_getIoBufferSize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);

    jlong Java_com_quantcast_qfs_access_KfsAccess_setReadAheadSize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong jsize);

    jlong Java_com_quantcast_qfs_access_KfsAccess_getReadAheadSize(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);

    jint Java_com_quantcast_qfs_access_KfsAccess_getStripedType(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath);

    void Java_com_quantcast_qfs_access_KfsAccess_setFileAttributeRevalidateTime(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint secs);

    jint Java_com_quantcast_qfs_access_KfsAccess_chmod(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring path, jint mode);

    jint Java_com_quantcast_qfs_access_KfsAccess_chmodr(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring path, jint mode);

    jint Java_com_quantcast_qfs_access_KfsAccess_fchmod(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jint mode);

    jint Java_com_quantcast_qfs_access_KfsAccess_chowns(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring path, jstring user, jstring group);

    jint Java_com_quantcast_qfs_access_KfsAccess_chownsr(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring path, jstring user, jstring group);

    jint Java_com_quantcast_qfs_access_KfsAccess_chown(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring path, jlong user, jlong group);

    jint Java_com_quantcast_qfs_access_KfsAccess_chownr(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring path, jlong user, jlong group);

    jint Java_com_quantcast_qfs_access_KfsAccess_fchowns(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jstring user, jstring group);

    jint Java_com_quantcast_qfs_access_KfsAccess_fchown(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong user, jlong group);

    jint Java_com_quantcast_qfs_access_KfsAccess_setEUserAndEGroup(
        JNIEnv *jenv, jclass jcls, jlong jptr, jlong user, jlong group, jlongArray);

    jint Java_com_quantcast_qfs_access_KfsAccess_stat(
        JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jobject attr);

    jstring Java_com_quantcast_qfs_access_KfsAccess_strerror(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jerr);

    jboolean Java_com_quantcast_qfs_access_KfsAccess_isnotfound(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jerr);

    jint Java_com_quantcast_qfs_access_KfsAccess_close(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);

    jlong Java_com_quantcast_qfs_access_KfsAccess_seek(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong joffset);

    jlong Java_com_quantcast_qfs_access_KfsAccess_tell(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);

    jint Java_com_quantcast_qfs_access_KfsAccess_getUMask(
        JNIEnv *jenv, jclass jcls, jlong jptr);

    jint Java_com_quantcast_qfs_access_KfsAccess_setUMask(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint umask);

   /* Input channel methods */
    jint Java_com_quantcast_qfs_access_KfsInputChannel_read(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jobject buf, jint begin, jint end);

    jint Java_com_quantcast_qfs_access_KfsInputChannel_close(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);

    /* Output channel methods */
    jint Java_com_quantcast_qfs_access_KfsOutputChannel_write(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jobject buf, jint begin, jint end);

    jint Java_com_quantcast_qfs_access_KfsOutputChannel_atomicRecordAppend(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jobject buf, jint begin, jint end);

    jint Java_com_quantcast_qfs_access_KfsOutputChannel_sync(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);

    jint Java_com_quantcast_qfs_access_KfsOutputChannel_close(
        JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd);
}

namespace
{
    inline void setStr(string & dst, JNIEnv * jenv, jstring src)
    {
        char const * s = jenv->GetStringUTFChars(src, 0);
        dst.assign(s);
        jenv->ReleaseStringUTFChars(src, s);
    }
}

jlong Java_com_quantcast_qfs_access_KfsAccess_initF(
    JNIEnv *jenv, jclass jcls, jstring jpath)
{
    string path;
    setStr(path, jenv, jpath);
    KfsClient* const clnt = Connect(path.c_str());
    return (jlong) clnt;
}

jint Java_com_quantcast_qfs_access_KfsAccess_compareChunkReplicas(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jobject stringbuffermd5)
{
    if (! jptr) {
        return -EFAULT;
    }
    string path , md5Sum;
    setStr(path, jenv, jpath);

    KfsClient* const clnt = (KfsClient *) jptr;
    const int res = clnt->CompareChunkReplicas(path.c_str(), md5Sum);
    if (res != 0) {
        return res;
    }
    jcls = jenv->GetObjectClass(stringbuffermd5);
    jmethodID mid = jenv->GetMethodID(jcls, "append",
        "(Ljava/lang/String;)Ljava/lang/StringBuffer;");
    if(mid == 0) {
        return -EFAULT;
    }
    jstring jstr = jenv->NewStringUTF(md5Sum.c_str());
    if (! jstr) {
        return -EFAULT;
    }
    jenv->CallObjectMethod(stringbuffermd5, mid, jstr);
    return res;
}

jlong Java_com_quantcast_qfs_access_KfsAccess_initS(
    JNIEnv *jenv, jclass jcls, jstring jmetaServerHost, jint metaServerPort)
{
    string path;
    setStr(path, jenv, jmetaServerHost);
    KfsClient* const clnt = Connect(path, metaServerPort);
    return (jlong) clnt;
}

void Java_com_quantcast_qfs_access_KfsAccess_destroy(
    JNIEnv *jenv, jclass jcls, jlong jptr)
{
    KfsClient* const clnt = (KfsClient*)jptr;
    delete clnt;
}

jint Java_com_quantcast_qfs_access_KfsAccess_cd(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->Cd(path.c_str());
}

jint Java_com_quantcast_qfs_access_KfsAccess_mkdirs(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jint mode)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->Mkdirs(path.c_str(), (kfsMode_t)mode);
}

jint Java_com_quantcast_qfs_access_KfsAccess_rmdir(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->Rmdir(path.c_str());
}

jint Java_com_quantcast_qfs_access_KfsAccess_rmdirs(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->Rmdirs(path.c_str());
}

jobjectArray Java_com_quantcast_qfs_access_KfsAccess_readdir(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jboolean jpreloadattr)
{
    if (! jptr) {
        return 0;
    }
    string path;
    setStr(path, jenv, jpath);

    KfsClient* const    clnt = (KfsClient*)jptr;
    vector<KfsFileAttr> fattr;
    vector<string>      entries;
    if ((jpreloadattr ?
            clnt->ReaddirPlus(path.c_str(), fattr) :
            clnt->Readdir(path.c_str(), entries)) != 0) {
        return 0;
    }
    jclass jstrClass = jenv->FindClass("java/lang/String");
    if (! jstrClass) {
        jclass excl = jenv->FindClass("java/lang/ClassNotFoundException");
        if (excl) {
            jenv->ThrowNew(excl, 0);
        }
        return 0;
    }
    const jsize  cnt      = jpreloadattr ? fattr.size() : entries.size();
    jobjectArray jentries = jenv->NewObjectArray(cnt, jstrClass, 0);
    if (! jentries) {
        return 0;
    }
    for (jsize i = 0; i < cnt; i++) {
        jstring s = jenv->NewStringUTF(
            jpreloadattr ? fattr[i].filename.c_str() : entries[i].c_str());
        if (! s) {
            return 0;
        }
        jenv->SetObjectArrayElement(jentries, i, s);
        jenv->DeleteLocalRef(s);
    }
    return jentries;
}

jint Java_com_quantcast_qfs_access_KfsAccess_open(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jstring jmode,
    jint jnumReplicas, jint jnumStripes, jint jnumRecoveryStripes,
    jint jstripeSize, jint jstripedType, jint jcreateMode)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path, mode;
    int openMode = 0;

    setStr(path, jenv, jpath);
    setStr(mode, jenv, jmode);

    if (mode == "opendir")
        return clnt->OpenDirectory(path.c_str());

    if (mode == "r")
        openMode = O_RDONLY;
    else if (mode == "rw")
        openMode = O_RDWR | O_CREAT;
    else if (mode == "w")
        openMode = O_WRONLY | O_CREAT;
    else if (mode == "a")
        openMode = O_WRONLY | O_APPEND;

    return clnt->Open(path.c_str(), openMode, jnumReplicas,
        jnumStripes, jnumRecoveryStripes, jstripeSize, jstripedType, jcreateMode);
}

jint Java_com_quantcast_qfs_access_KfsInputChannel_close(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    return clnt->Close(jfd);
}

jint Java_com_quantcast_qfs_access_KfsOutputChannel_close(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    return clnt->Close(jfd);
}

jint Java_com_quantcast_qfs_access_KfsAccess_create(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jint jnumReplicas, jboolean jexclusive,
    jint jnumStripes, jint jnumRecoveryStripes, jint jstripeSize, jint jstripedType,
    jboolean foreceType, jint mode)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->Create(path.c_str(), jnumReplicas, jexclusive,
        jnumStripes, jnumRecoveryStripes, jstripeSize, jstripedType, foreceType, (kfsMode_t)mode);
}

jint Java_com_quantcast_qfs_access_KfsAccess_remove(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->Remove(path.c_str());
}

jint Java_com_quantcast_qfs_access_KfsAccess_rename(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring joldpath,
    jstring jnewpath, jboolean joverwrite)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string opath, npath;
    setStr(opath, jenv, joldpath);
    setStr(npath, jenv, jnewpath);

    return clnt->Rename(opath.c_str(), npath.c_str(), joverwrite);
}

jlong Java_com_quantcast_qfs_access_KfsAccess_setDefaultIoBufferSize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jlong jsize)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    return (jlong)clnt->SetDefaultIoBufferSize(jsize);
}

jlong Java_com_quantcast_qfs_access_KfsAccess_getDefaultIoBufferSize(
    JNIEnv *jenv, jclass jcls, jlong jptr)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    return (jlong)clnt->GetDefaultIoBufferSize();
}

jlong Java_com_quantcast_qfs_access_KfsAccess_setDefaultReadAheadSize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jlong jsize)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    return (jlong)clnt->SetDefaultReadAheadSize(jsize);
}

jlong Java_com_quantcast_qfs_access_KfsAccess_getDefaultReadAheadSize(
    JNIEnv *jenv, jclass jcls, jlong jptr)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    return (jlong)clnt->GetDefaultReadAheadSize();
}

jlong Java_com_quantcast_qfs_access_KfsAccess_setIoBufferSize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong jsize)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    return (jlong)clnt->SetIoBufferSize(jfd, jsize);
}

jlong Java_com_quantcast_qfs_access_KfsAccess_getIoBufferSize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    return (jlong)clnt->GetIoBufferSize(jfd);
}

jlong Java_com_quantcast_qfs_access_KfsAccess_setReadAheadSize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong jsize)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    return (jlong)clnt->SetReadAheadSize(jfd, jsize);
}

jlong Java_com_quantcast_qfs_access_KfsAccess_getReadAheadSize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    return (jlong)clnt->GetReadAheadSize(jfd);
}

jint Java_com_quantcast_qfs_access_KfsAccess_getStripedType(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path;
    setStr(path, jenv, jpath);
    const bool computeFilesize = false;
    KfsFileAttr attr;
    return (jint)(clnt->Stat(path.c_str(), attr, computeFilesize) != 0 ?
        KFS_STRIPED_FILE_TYPE_UNKNOWN : attr.striperType);
}

void Java_com_quantcast_qfs_access_KfsAccess_setFileAttributeRevalidateTime(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint secs)
{
    if (! jptr) {
        return;
    }
    KfsClient* const clnt = (KfsClient*)jptr;
    clnt->SetFileAttributeRevalidateTime(secs);
}

jint Java_com_quantcast_qfs_access_KfsAccess_chmod(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jint mode)
{
    if (! jptr) {
        return -EFAULT;
    }
    string path;
    setStr(path, jenv, jpath);
    KfsClient* const clnt = (KfsClient *) jptr;
    return clnt->Chmod(path.c_str(), (kfsMode_t)mode);
}

jint Java_com_quantcast_qfs_access_KfsAccess_chmodr(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jint mode)
{
    if (! jptr) {
        return -EFAULT;
    }
    string path;
    setStr(path, jenv, jpath);
    KfsClient* const clnt = (KfsClient *) jptr;
    return clnt->ChmodR(path.c_str(), (kfsMode_t)mode);
}

jint Java_com_quantcast_qfs_access_KfsAccess_fchmod(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jint mode)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient *) jptr;
    return clnt->Chmod(jfd, (kfsMode_t)mode);
}

jint Java_com_quantcast_qfs_access_KfsAccess_chowns(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jstring juser, jstring jgroup)
{
    if (! jptr) {
        return -EFAULT;
    }
    string path;
    setStr(path, jenv, jpath);
    string user;
    string group;
    if (juser) {
        setStr(user, jenv, juser);
    }
    if (jgroup) {
        setStr(group, jenv, jgroup);
    }
    KfsClient* const clnt = (KfsClient *) jptr;
    return clnt->Chown(path.c_str(), user.c_str(), group.c_str());
}

jint Java_com_quantcast_qfs_access_KfsAccess_chownsr(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jstring juser, jstring jgroup)
{
    if (! jptr) {
        return -EFAULT;
    }
    string path;
    setStr(path, jenv, jpath);
    string user;
    string group;
    if (juser) {
        setStr(user, jenv, juser);
    }
    if (jgroup) {
        setStr(group, jenv, jgroup);
    }
    KfsClient* const clnt = (KfsClient *) jptr;
    return clnt->ChownR(path.c_str(), user.c_str(), group.c_str());
}

jint Java_com_quantcast_qfs_access_KfsAccess_chown(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jlong user, jlong group)
{
    if (! jptr) {
        return -EFAULT;
    }
    string path;
    setStr(path, jenv, jpath);
    KfsClient* const clnt = (KfsClient *) jptr;
    return clnt->Chown(path.c_str(), (kfsUid_t)user, (kfsGid_t)group);
}

jint Java_com_quantcast_qfs_access_KfsAccess_chownr(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jlong user, jlong group)
{
    if (! jptr) {
        return -EFAULT;
    }
    string path;
    setStr(path, jenv, jpath);
    KfsClient* const clnt = (KfsClient *) jptr;
    return clnt->ChownR(path.c_str(), (kfsUid_t)user, (kfsGid_t)group);
}

jint Java_com_quantcast_qfs_access_KfsAccess_chownR(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jlong user, jlong group)
{
    if (! jptr) {
        return -EFAULT;
    }
    string path;
    setStr(path, jenv, jpath);
    KfsClient* const clnt = (KfsClient *) jptr;
    return clnt->ChownR(path.c_str(), (kfsUid_t)user, (kfsGid_t)group);
}

jint Java_com_quantcast_qfs_access_KfsAccess_fchowns(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jstring juser, jstring jgroup)
{
    if (! jptr) {
        return -EFAULT;
    }
    string user;
    string group;
    if (juser) {
        setStr(user, jenv, juser);
    }
    if (jgroup) {
        setStr(group, jenv, jgroup);
    }
    KfsClient* const clnt = (KfsClient *) jptr;
    return clnt->Chown(jfd, user.c_str(), group.c_str());
}

jint Java_com_quantcast_qfs_access_KfsAccess_fchown(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong user, jlong group)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient *) jptr;
    return clnt->Chown(jfd, (kfsUid_t)user, (kfsGid_t)group);
}

jint Java_com_quantcast_qfs_access_KfsOutputChannel_sync(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;
    return clnt->Sync(jfd);
}

jint Java_com_quantcast_qfs_access_KfsAccess_setEUserAndEGroup(
    JNIEnv *jenv, jclass jcls, jlong jptr, jlong user, jlong group, jlongArray jgroups)
{
    if (! jptr) {
        return -EFAULT;
    }
    kfsGid_t* groups = 0;
    jsize     cnt    = 0;
    if (jgroups) {
        cnt = jenv->GetArrayLength(jgroups);
        jlong* const  jg = jenv->GetLongArrayElements(jgroups, 0);
        groups = new kfsGid_t[cnt];
        for (jsize i = 0; i < cnt; i++) {
            groups[i] = (kfsGid_t)jg[i];
        }
        jenv->ReleaseLongArrayElements(jgroups, jg, 0);
    }
    KfsClient* const clnt = (KfsClient*)jptr;
    const int ret = clnt->SetEUserAndEGroup(
        (kfsUid_t)user, (kfsGid_t)group, groups, (int)cnt);
    delete [] groups;
    return ret;
}

jint Java_com_quantcast_qfs_access_KfsAccess_exists(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path;
    setStr(path, jenv, jpath);
    return (clnt->Exists(path.c_str()) ? 1 : 0);
}

jint Java_com_quantcast_qfs_access_KfsAccess_isFile(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path;
    setStr(path, jenv, jpath);

    return (clnt->IsFile(path.c_str()) ? 1 : 0);
}

jint Java_com_quantcast_qfs_access_KfsAccess_isDirectory(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path;
    setStr(path, jenv, jpath);

    return (clnt->IsDirectory(path.c_str()) ? 1 : 0);
}

jlong Java_com_quantcast_qfs_access_KfsAccess_filesize(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    KfsFileAttr attr;
    string path;
    setStr(path, jenv, jpath);

    if (clnt->Stat(path.c_str(), attr) != 0) {
        return -1;
    }
    return attr.fileSize;
}

jlong Java_com_quantcast_qfs_access_KfsAccess_getModificationTime(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    KfsFileAttr attr;
    string path;
    setStr(path, jenv, jpath);

    if (clnt->Stat(path.c_str(), attr) != 0)
        return -1;

    // The expected return value is in ms
    return ((jlong) attr.mtime.tv_sec) * 1000 + (jlong) (attr.mtime.tv_usec / 1000);
}

jint Java_com_quantcast_qfs_access_KfsAccess_setModificationTime(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jlong jmsec)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path;
    setStr(path, jenv, jpath);

    struct timeval mtime;

    // the input is in ms
    mtime.tv_sec = jmsec / 1000;
    mtime.tv_usec = jmsec % 1000;
    if (clnt->SetMtime(path.c_str(), mtime) != 0)
        return -1;

    return 0;
}

jobjectArray Java_com_quantcast_qfs_access_KfsAccess_getDataLocation(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jlong jstart, jlong jlen)
{
    if (! jptr) {
        return NULL;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    // for each block, there could be multiple locations due to replication; return them all here

    string path;
    setStr(path, jenv, jpath);

    vector< vector<string> > entries;
    const int res = clnt->GetDataLocation(path.c_str(), jstart, jlen, entries);
    if (res < 0) {
        return 0;
    }
    jclass jstrArrClass = jenv->FindClass("[Ljava/lang/String;");
    if (! jstrArrClass) {
        jclass excl = jenv->FindClass("java/lang/ClassNotFoundException");
        if (excl) {
            jenv->ThrowNew(excl, 0);
        }
        return 0;
    }
    jclass jstrClass = jenv->FindClass("java/lang/String");
    if (! jstrArrClass) {
        jclass excl = jenv->FindClass("java/lang/ClassNotFoundException");
        if (excl) {
            jenv->ThrowNew(excl, 0);
        }
        return 0;
    }
    // For each block, return its location(s)
    const jsize sz = (jsize)entries.size();
    jobjectArray jentries = jenv->NewObjectArray(sz, jstrArrClass, 0);
    if (! jentries) {
        return 0;
    }
    for (jsize i = 0; i < sz; i++) {
        const jsize lsz = (jsize)entries[i].size();
        jobjectArray jlocs = jenv->NewObjectArray(lsz, jstrClass, 0);
        if (! jlocs) {
            return 0;
        }
        for (jsize j = 0; j < lsz; j++) {
            jstring s = jenv->NewStringUTF(entries[i][j].c_str());
            if (! s) {
                return 0;
            }
            jenv->SetObjectArrayElement(jlocs, j, s);
            jenv->DeleteLocalRef(s);
        }
        jenv->SetObjectArrayElement(jentries, i, jlocs);
        jenv->DeleteLocalRef(jlocs);
    }

    return jentries;
}

jshort Java_com_quantcast_qfs_access_KfsAccess_getReplication(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->GetReplicationFactor(path.c_str());
}

jshort Java_com_quantcast_qfs_access_KfsAccess_setReplication(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jint jnumReplicas)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    string path;
    setStr(path, jenv, jpath);
    return clnt->SetReplicationFactor(path.c_str(), jnumReplicas);
}

jint Java_com_quantcast_qfs_access_KfsAccess_stat(
    JNIEnv *jenv, jclass jcls, jlong jptr, jstring jpath, jobject attr)
{
    if (! jptr) {
        return -EFAULT;
    }
    if (! jpath || ! attr) {
        return -EINVAL;
    }

    jclass const acls = jenv->GetObjectClass(attr);
    if (! acls) {
        return -EINVAL;
    }

    string path;
    setStr(path, jenv, jpath);
    KfsFileAttr kfsAttr;
    KfsClient* const clnt = (KfsClient*)jptr;
    int ret = clnt->Stat(path.c_str(), kfsAttr);
    if (ret != 0) {
        return (jint)ret;
    }
    string names[3];
    names[0] = kfsAttr.filename;
    ret = clnt->GetUserAndGroupNames(
        kfsAttr.user, kfsAttr.group, names[1], names[2]);
    if (ret != 0) {
        return (jint)ret;
    }

    jfieldID fid = jenv->GetFieldID(acls, "isDirectory", "Z");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetBooleanField(attr, fid, (jboolean)kfsAttr.isDirectory);

    fid = jenv->GetFieldID(acls, "filesize", "J");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetLongField(attr, fid, (jlong)kfsAttr.fileSize);

    fid = jenv->GetFieldID(acls, "modificationTime", "J");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetLongField(attr, fid,
        (jlong)kfsAttr.mtime.tv_sec * 1000 +
        (jlong)kfsAttr.mtime.tv_usec / 1000
    );

    fid = jenv->GetFieldID(acls, "replication", "I");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetIntField(attr, fid, kfsAttr.numReplicas);

    fid = jenv->GetFieldID(acls, "striperType", "I");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetIntField(attr, fid, (jint)kfsAttr.striperType);

    fid = jenv->GetFieldID(acls, "numStripes", "I");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetIntField(attr, fid, (jint)kfsAttr.numStripes);

    fid = jenv->GetFieldID(acls, "numRecoveryStripes", "I");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetIntField(attr, fid, (jint)kfsAttr.numRecoveryStripes);

    fid = jenv->GetFieldID(acls, "stripeSize", "I");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetIntField(attr, fid, (jint)kfsAttr.stripeSize);

    fid = jenv->GetFieldID(acls, "owner", "J");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetLongField(attr, fid, (jlong)kfsAttr.user);

    fid = jenv->GetFieldID(acls, "group", "J");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetLongField(attr, fid, (jlong)kfsAttr.group);

    fid = jenv->GetFieldID(acls, "mode", "I");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetIntField(attr, fid, (jint)kfsAttr.mode);

    fid = jenv->GetFieldID(acls, "fileId", "J");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetLongField(attr, fid, (jlong)kfsAttr.fileId);

    fid = jenv->GetFieldID(acls, "dirCount", "J");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetLongField(attr, fid, (jlong)kfsAttr.dirCount());

    fid = jenv->GetFieldID(acls, "fileCount", "J");
    if (! fid) {
        return -EFAULT;
    }
    jenv->SetLongField(attr, fid, (jlong)kfsAttr.fileCount());

    const char* const fieldNames[] = {"filename", "ownerName", "groupName"};
    for (int i = 0; i < 3; i++) {
        jstring const nm = jenv->NewStringUTF(names[i].c_str());
        if (! nm) {
            return -EFAULT;
        }
        fid = jenv->GetFieldID(acls, fieldNames[i], "Ljava/lang/String;");
        if (! fid) {
            return -EFAULT;
        }
        jenv->SetObjectField(attr, fid, nm);
    }

    return 0;
}

jstring Java_com_quantcast_qfs_access_KfsAccess_strerror(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jerr)
{
    const string str = KFS::ErrorCodeToStr((int)jerr);
    return jenv->NewStringUTF(str.c_str());
}

jboolean Java_com_quantcast_qfs_access_KfsAccess_isnotfound(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jerr)
{
    return (jboolean)(jerr == -ENOENT);
}

jint Java_com_quantcast_qfs_access_KfsAccess_close(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;
    return clnt->Close(jfd);
}

jlong Java_com_quantcast_qfs_access_KfsAccess_seek(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jlong joffset)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;
    return (jlong)clnt->Seek(jfd, joffset);
}

jlong Java_com_quantcast_qfs_access_KfsAccess_tell(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;
    return (jlong)clnt->Tell(jfd);
}

jint Java_com_quantcast_qfs_access_KfsAccess_getUMask(
    JNIEnv *jenv, jclass jcls, jlong jptr)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;
    return (jint)(clnt->GetUMask() & 0777);
}

jint Java_com_quantcast_qfs_access_KfsAccess_setUMask(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint umask)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;
    clnt->SetUMask((kfsMode_t)umask);
    return 0;
}

jint Java_com_quantcast_qfs_access_KfsInputChannel_read(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jobject buf, jint begin, jint end)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    if (! buf) {
        return 0;
    }
    void * addr = jenv->GetDirectBufferAddress(buf);
    jlong cap = jenv->GetDirectBufferCapacity(buf);

    if (! addr || cap < 0) {
        return 0;
    }
    if(begin < 0 || end > cap || begin > end) {
        return 0;
    }
    addr = (void *)(uintptr_t(addr) + begin);

    ssize_t sz = clnt->Read((int) jfd, (char *) addr, (size_t) (end - begin));
    return (jint)sz;
}

jint Java_com_quantcast_qfs_access_KfsOutputChannel_write(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jobject buf, jint begin, jint end)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    if(! buf) {
        return 0;
    }
    void* addr = jenv->GetDirectBufferAddress(buf);
    jlong cap = jenv->GetDirectBufferCapacity(buf);

    if (! addr || cap < 0) {
        return 0;
    }
    if (begin < 0 || end > cap || begin > end) {
        return 0;
    }
    addr = (void *)(uintptr_t(addr) + begin);

    ssize_t sz = clnt->Write((int) jfd, (const char *) addr, (size_t) (end - begin));
    return (jint)sz;
}

jint Java_com_quantcast_qfs_access_KfsOutputChannel_atomicRecordAppend(
    JNIEnv *jenv, jclass jcls, jlong jptr, jint jfd, jobject buf, jint begin, jint end)
{
    if (! jptr) {
        return -EFAULT;
    }
    KfsClient* const clnt = (KfsClient*)jptr;

    if (! buf) {
        return 0;
    }
    void * addr = jenv->GetDirectBufferAddress(buf);
    jlong cap = jenv->GetDirectBufferCapacity(buf);

    if (! addr || cap < 0) {
        return 0;
    }
    if (begin < 0 || end > cap || begin > end) {
        return 0;
    }
    addr = (void *)(uintptr_t(addr) + begin);

    ssize_t sz = clnt->AtomicRecordAppend((int) jfd, (const char *) addr, (int) (end - begin));
    return (jint)sz;
}
