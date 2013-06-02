#ifndef KFS_KRB_SERVICE_H
#define KFS_KRB_SERVICE_H

namespace KFS
{
class KrbService
{
public:
    KrbService();
    ~KrbService();
    const char* Init(
        const char* inServeiceNamePtr);
private:
    class Impl;
    Impl& mImpl;
    
};

}

#endif /* KFS_KRB_SERVICE_H */
