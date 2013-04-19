#include "KfsCallbackObj.h"

#include <stdlib.h>

namespace KFS
{

inline static const ObjectMethodBase*
MakeNullObjMethod()
{
    static const ObjectMethod<KfsCallbackObj> sObjMethod(0, 0);
    return &sObjMethod;
}

const ObjectMethodBase* const kNullObjMethod = MakeNullObjMethod();

/* virtual */
KfsCallbackObj::~KfsCallbackObj()
{
    if (mObjMeth == kNullObjMethod) {
        abort(); // Catch double delete.
        return;
    }
    if (mObjMeth) {
        mObjMeth->~ObjectMethodBase();
    }
    mObjMeth = kNullObjMethod;
}

}
