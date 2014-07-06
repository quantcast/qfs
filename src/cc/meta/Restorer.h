/*!
 * $Id$
 *
 * \file Restorer.h
 * \brief rebuild metatree from saved checkpoint
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
 */
#if !defined(KFS_RESTORE_H)
#define KFS_RESTORE_H

#include <fstream>
#include <string>
#include "util.h"

namespace KFS
{
using std::ifstream;
using std::string;

/*!
 * \brief state for restoring from a checkpoint file
 */
class Restorer
{
public:
    Restorer()
        : file()
        {}
    ~Restorer()
        {}
    /*
     * process the CP file.  also, if the # of replicas of a file is below
     * the specified value, bump up replication.  this allows us to change
     * the filesystem wide degree of replication in a simple manner.
     */
    bool rebuild(string cpname, int16_t minNumReplicasPerFile = 1);
private:
    ifstream file;          //!< the CP file
private:
    // No copy.
    Restorer(const Restorer&);
    Restorer& operator=(Restorer&);
};

class DETokenizer;
extern bool restore_chunkVersionInc(DETokenizer& c);
extern bool restore_setintbase(DETokenizer& c);

/*
 * Whenever a checkpoint file is loaded in, it takes up a ton of memory.
 * To prevent logcompactor from stomping over a filechecker or filelister or
 * vice-versa, all of these tools acquire a lock file before loading the
 * checkpoint.
 */
extern int try_to_acquire_lockfile(const string &lockfn);
extern int acquire_lockfile(const string &lockfn, int ntries);

extern string restoreChecksum;
extern bool   lastLineChecksumFlag;
extern bool   restore_checksum(DETokenizer& c);
extern bool   restore_delegate_cancel(DETokenizer& c);
extern bool   restore_filesystem_info(DETokenizer& c);

}
#endif // !defined(KFS_RESTORE_H)
