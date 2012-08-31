/*---------------------------------------------------------- -*- Mode: C -*-----
 * $Id$
 *
 * Created 2010/07/24
 * Author: Dan Adkins
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
 * \file rs_table.h
 * \brief Reed Solomon encoder and decoder constant declarations.
 *
 *------------------------------------------------------------------------------
 */

#ifndef RS_TABLE_H
#define RS_TABLE_H

#include "rs.h"
#include "prim.h"

struct rs_nibtab
{
    v16 lo;     /* mul by lo nibble: 0x00, 0x01, 0x02, ..., 0x0F */
    v16 hi;     /* mul by hi nibble: 0x00, 0x10, 0x20, ..., 0xF0 */
};
typedef struct rs_nibtab rs_nibtab;

/* Nibble multiplication table */
extern const rs_nibtab rs_nibmul[];

/* Recovery coefficients for 1 missing data block
 * e.g., use rs_r1P[x] if x is missing and P is available.
 */
extern const uint8_t rs_r1P[];
extern const uint8_t rs_r1Q[];
extern const uint8_t rs_r1R[];

/* Recovery coefficients for 2 missing data blocks.
 * r2map[x][y] is an index into r2_r...
 * e.g. use rs_r2PQ[r2map[x][y]] if x and y are missing,
 * but P and Q are available.
 */
extern const uint8_t rs_r2PQ[][4];
extern const uint8_t rs_r2PR[][4];
extern const uint8_t rs_r2QR[][4];
extern const uint16_t rs_r2map[RS_LIB_MAX_DATA_BLOCKS][RS_LIB_MAX_DATA_BLOCKS];

/* Recovery coefficients for 3 missing data blocks.
 * r3map[x][y][z] is an index into r3_r...
 * e.g. use rs_r3[r3map[x][y][z]] if x, y, and x are
 * missing.  All of P, Q, and R must be available.
 */
extern const uint8_t rs_r3[][9];
extern const uint16_t rs_r3map[RS_LIB_MAX_DATA_BLOCKS][RS_LIB_MAX_DATA_BLOCKS][
    RS_LIB_MAX_DATA_BLOCKS];

#endif /* RS_TABLE_H */
