#include "KrbService.h"

#include <krb5/krb5.h>

#include <string>

namespace KFS
{

using std::string;

class KrbService::Impl
{
public:
    Impl()
        : mCtx(),
          mAuthCtx(),
          mServer(),
          mKeyTab(),
          mErrCode(0),
          mInitedFlag(false),
          mServiceName(),
          mErrorMsg()
        {}
    ~Impl()
    {
        Impl::CleanupSelf();
    }
    const char* Init(
        const char* inServeiceNamePtr)
    {
        CleanupSelf();
        mErrorMsg.clear();
        mServiceName.clear();
        if (inServeiceNamePtr) {
            mServiceName = inServeiceNamePtr;
        }
        InitSelf();
        if (mErrCode) {
            mErrorMsg = ErrToStr(mErrCode);
            CleanupSelf();
        }
        return mErrorMsg.c_str();
    }
private:
    string            mKeyTabFileName;
    krb5_context      mCtx;
    krb5_auth_context mAuthCtx;
    krb5_principal    mServer;
    krb5_keytab       mKeyTab;
    krb5_error_code   mErrCode;
    bool              mInitedFlag;
    string            mServiceName;
    string            mErrorMsg;

    void InitSelf()
    {
        mErrCode = krb5_init_context(&mCtx);
        if (mErrCode) {
            return;
        }
        mErrCode = krb5_auth_con_init(mCtx, &mAuthCtx);
        if (mErrCode) {
            krb5_free_context(mCtx);
            return;
        }
        mInitedFlag = true;
        krb5_rcache theRCachePtr = 0;
        mErrCode = krb5_auth_con_getrcache(mCtx, mAuthCtx, &theRCachePtr);
        if (mErrCode) {
            return;
        }
	mErrCode = krb5_sname_to_principal(
            mCtx, 0, mServiceName.c_str(), KRB5_NT_SRV_HST, &mServer);
        if (mErrCode) {
            return;
        }
	if (! theRCachePtr)  {
            mErrCode = krb5_get_server_rcache(
                mCtx, krb5_princ_component(mCtx, mServer, 0), &theRCachePtr);
            if (mErrCode) {
                return;
            }
        }
        mErrCode = krb5_auth_con_setrcache(mCtx, mAuthCtx, theRCachePtr);
        if (mErrCode) {
            return;
        }
        mKeyTab = 0;
        mErrCode = mKeyTabFileName.empty() ?
            krb5_kt_default(mCtx, &mKeyTab) :
            krb5_kt_resolve(mCtx, mKeyTabFileName.c_str(), &mKeyTab);
    }
    krb5_error_code CleanupSelf()
    {
        if (! mInitedFlag) {
            return 0;
        }
        mInitedFlag = false;
        krb5_error_code theErr = 0;
        if (mKeyTab) {
            theErr = krb5_kt_close(mCtx, mKeyTab);
            mKeyTab = 0;
        }
        const krb5_error_code theCtxErr = krb5_auth_con_free(mCtx, mAuthCtx);
        if (! theErr && theCtxErr) {
            theErr = theCtxErr;
        }
        krb5_free_context(mCtx);
        return theErr;
    }
    string ErrToStr(
        krb5_error_code inErrCode) const
    {
        if (! inErrCode) {
            return string();
        }
        if ( ! mCtx) {
            return string("no kerberos context");
        }
        const char* const theMsgPtr = krb5_get_error_message(mCtx, inErrCode);
        return string(theMsgPtr);
    }
};

KrbService::KrbService()
    : mImpl(*(new Impl()))
{
}

KrbService::~KrbService()
{
    delete &mImpl;
}

    const char*
KrbService::Init(
        const char* inServeiceNamePtr)
{
    return mImpl.Init(inServeiceNamePtr);
} 

}
