//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/03/01
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
// Kerberos 5 platform specific definitions.
//
//----------------------------------------------------------------------------

#ifndef KFSKRB5_H
#define KFSKRB5_H

#if defined(KFS_OS_NAME_CYGWIN)
#   include <krb5.h>
#   if ! defined(KFS_KRB_USE_HEIMDAL) && ! defined(KFS_KRB_USE_MIT)
#       define KFS_KRB_USE_HEIMDAL
#   endif
#else
#   include <krb5/krb5.h>
#endif

#ifdef KFS_KRB_USE_HEIMDAL
#   include <inttypes.h>
#   include <errno.h>
#   include <string.h>

namespace KFS
{

class KfsKrb5
{
public:
    typedef int32_t int32;

    template<typename CT, typename DT>
    inline static void free_data_contents(
        CT /* inCtx */,
        DT inData)
        { krb5_data_free(inData); }
    template<typename CT, typename DT>
    inline static void free_unparsed_name(
        CT /* inCtx */,
        DT inData)
        { krb5_xfree(inData); }
    template<typename CT, typename ACT, typename AT>
    inline static krb5_error_code auth_con_getauthenticator(
        CT  inCtx,
        ACT inACtx,
        AT  inAT)
        { return krb5_auth_con_getauthenticator(inCtx, inACtx, inAT); }
    template<typename CT, typename PT, typename FT, typename ST, typename LT>
    inline static krb5_error_code unparse_name(
        CT inCtx,
        PT inPrin,
        FT inFlags,
        ST inStrPtr,
        LT inAllocLen)
    {
        if (*inStrPtr) {
            free_unparsed_name(inCtx, *inStrPtr);
            *inStrPtr = 0;
        }
        *inAllocLen = 0;
        return krb5_unparse_name_flags(
            inCtx,
            inPrin,
            inFlags,
            inStrPtr
        );
    }
    // Get and use ticket to obtain client principal, as authentication context
    // has no client principal.
    template<typename AT, typename TT>
    inline static krb5_principal get_client_principal(
        AT /* inAuth */,
        TT  inTicket)
        { return (inTicket ? inTicket->client : 0); }
    inline static krb5_ticket** req_get_ticket_ptr(
        krb5_ticket** inTicketPtr)
        { return inTicketPtr; }
    
    template<typename KT>
    inline static const char* get_key_block_contents(
        KT inKey)
        { return reinterpret_cast<const char*>(inKey->keyvalue.data); }
    template<typename KT>
    inline static int get_key_block_length(
        KT inKey)
        { return inKey->keyvalue.length; }
    template<typename CT, typename PT, typename RT>
    inline static krb5_error_code get_server_rcache(
        CT inCtx,
        PT inPrincipal,
        RT inRCache)
    {
        const char* const theStr =
            krb5_principal_get_comp_string(inCtx, inPrincipal, 0);
        if (! theStr) {
            return EINVAL;
        }
        krb5_data theData = {0};
        krb5_error_code theRet = krb5_data_alloc(&theData, strlen(theStr));
        if (theRet) {
            return theRet;
        }
        memcpy(theData.data, theStr, theData.length);
        theRet = krb5_get_server_rcache(inCtx, &theData, inRCache);
        krb5_data_free(&theData);
        return theRet;
    }
    template<typename CT, typename OT, typename HT>
    inline static krb5_error_code get_init_creds_opt_set_out_ccache(
        CT /* inCtx */ ,
        OT /* inOpts */,
        HT /* inCache */)
        { return 0; }
    template<typename CT, typename ET>
    inline static krb5_error_code free_keytab_entry_contents(
        CT inCtx,
        ET inEntry)
        { return krb5_kt_free_entry(inCtx, inEntry); }
};

} // namespace KFS

#else /* KFS_KRB_USE_HEIMDAL */

namespace KFS
{
class KfsKrb5
{
public:
    typedef krb5_int32 int32;

    template<typename CT, typename DT>
    inline static void free_data_contents(
        CT inCtx,
        DT inData)
        { krb5_free_data_contents(inCtx, inData); }
    template<typename CT, typename DT>
    inline static void free_unparsed_name(
        CT inCtx,
        DT inData)
        { krb5_free_unparsed_name(inCtx, inData); }
    template<typename CT, typename ACT, typename AT>
    inline static krb5_error_code auth_con_getauthenticator(
        CT  inCtx,
        ACT inACtx,
        AT  inAT)
        { return krb5_auth_con_getauthenticator(inCtx, inACtx, &inAT); }
    template<typename CT, typename PT, typename FT, typename ST, typename LT>
    inline static krb5_error_code unparse_name(
        CT inCtx,
        PT inPrin,
        FT inFlags,
        ST inStrPtr,
        LT inAllocLen)
    {
        if (*inStrPtr) {
            // Work around mit krb5 lib. The lib always does malloc() when the
            // size matches, instead of doing nothing, resulting in memory leak.
            // Another way to work around this would be to lie about the
            // size/length, by setting the size/length to 0 or subtracting one
            // from it. Of course, the danger is that doing so might break in
            // non obvious ways with other kerberos implementations or releases.
            // For now just always free, then allocate the block.
            free_unparsed_name(inCtx, *inStrPtr);
            *inStrPtr   = 0;
            *inAllocLen = 0;
        }
#if ! defined(KRB5_PRINCIPAL_UNPARSE_SHORT) && \
        ! defined(KRB5_PRINCIPAL_UNPARSE_NO_REALM) && \
        ! defined(KRB5_PRINCIPAL_UNPARSE_DISPLAY)
        // FIXME: make flags work with older versions.
        (void)inFlags;
        return krb5_unparse_name_ext(
            inCtx,
            inPrin,
            inStrPtr,
            inAllocLen
        );
#else
        return krb5_unparse_name_flags_ext(
            inCtx,
            inPrin,
            inFlags,
            inStrPtr,
            inAllocLen
        );
#endif
    }
    template<typename KT>
    inline static const char* get_key_block_contents(
        KT inKey)
        { return reinterpret_cast<const char*>(inKey->contents); }
    template<typename KT>
    inline static int get_key_block_length(
        KT inKey)
        { return inKey->length; }
    // Do not use ticket, use authentication context instead
    template<typename AT, typename TT>
    inline static krb5_principal get_client_principal(
        AT inAuth,
        TT /* inTicket */)
        { return (inAuth ? inAuth->client : 0); }
    inline static krb5_ticket** req_get_ticket_ptr(
        krb5_ticket** /* inTicketPtr */)
        { return 0; }
    template<typename CT, typename PT, typename RT>
    inline static krb5_error_code get_server_rcache(
        CT inCtx,
        PT inPrincipal,
        RT inRCache)
    {
        return krb5_get_server_rcache(
            inCtx,
            krb5_princ_component(inCtx, inPrincipal, 0),
            inRCache
        );
    }
    template<typename CT, typename OT, typename HT>
    inline static krb5_error_code get_init_creds_opt_set_out_ccache(
        CT inCtx,
        OT inOpts,
        HT inCache)
    {
        return krb5_get_init_creds_opt_set_out_ccache(inCtx, inOpts, inCache);
    }
    template<typename CT, typename ET>
    inline static krb5_error_code free_keytab_entry_contents(
        CT inCtx,
        ET inEntry)
        { return krb5_free_keytab_entry_contents(inCtx, inEntry); }
};

#if ! defined(KRB5_PRINCIPAL_UNPARSE_SHORT) && \
        ! defined(KRB5_PRINCIPAL_UNPARSE_NO_REALM) && \
        ! defined(KRB5_PRINCIPAL_UNPARSE_DISPLAY)
// For now add stubs to compile.
enum {
    KRB5_PRINCIPAL_UNPARSE_SHORT,
    KRB5_PRINCIPAL_UNPARSE_NO_REALM,
    KRB5_PRINCIPAL_UNPARSE_DISPLAY
};
#endif

} // namespace KFS

#endif /* KFS_KRB_USE_HEIMDAL */

#endif /* KFSKRB5_H */

