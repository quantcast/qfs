/*!
 * $Id$
 *
 * \file kfstypes.h
 * \brief simple typedefs and enums for the KFS metadata server
 * \author Blake Lewis (Kosmix Corp.)
 *
 * Copyright 2008-2012 Quantcast Corp.
 * Copyright 2006-2008 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */
#if !defined(KFS_TYPES_H)
#define KFS_TYPES_H

#include "common/kfstypes.h"

namespace KFS {

/*!
 * \brief KFS metadata types.
 */
enum MetaType {
    //!< Key class and meta tree ordering / logic depends on these values.
    KFS_UNINIT    = 0x00,   //!< uninitialized
    KFS_INTERNAL  = 0x10,   //!< internal node
    KFS_FATTR     = 0x20,   //!< file attributes
    KFS_CHUNKINFO = 0x21,   //!< chunk information
    KFS_DENTRY    = 0x22,   //!< directory entry
    KFS_SENTINEL  = 0x30    //!< internal use, must be largest
};

/*!
 * \brief KFS file types
 */
enum FileType {
    KFS_NONE,       //!< uninitialized
    KFS_FILE,       //!< plain file
    KFS_DIR         //!< directory
};

/*!
 * \brief KFS lease types
 */
enum LeaseType {
    READ_LEASE,
    WRITE_LEASE
};

}
#endif // !defined(KFS_TYPES_H)
