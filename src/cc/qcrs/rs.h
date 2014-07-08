/*---------------------------------------------------------- -*- Mode: C -*-----
 * $Id$
 *
 * Created 2010/07/24
 * Author: Dan Adkins
 * Edited by: Chao Tian (AT&T) 2014/3/25
 *
 * Copyright 2010-2011 Quantcast Corp.
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
 * \file rs.h
 * \brief Reed Solomon encoder and decoder public interface.
 *
 *------------------------------------------------------------------------------
 */

#ifndef RS_H
#define RS_H

#ifdef __cplusplus
extern "C" {
#endif
// Chao's edit: to allow more coding parameters
//#define RS_LIB_MAX_DATA_BLOCKS 64
//#define RS_LIB_MAX_RECOVERY_BLOCKS 3


#define RS_LIB_MAX_DATA_BLOCKS 255
#define RS_LIB_MAX_RECOVERY_BLOCKS 255
#define RS_LIB_PACKETSIZE 1024



//void rs_encode(int nblocks, int mrecoveryblocks, int blocksize, void **data);
//void rs_decode1(int nblocks, int blocksize, int x, void **data);
//void rs_decode2(int nblocks, int blocksize, int x, int y, void **data);
//void rs_decode3(int nblocks, int blocksize, int x, int y, int z, void **data);
// end of Chao's edit
#ifdef __cplusplus
}
#endif

#endif
