//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/05/25
//
// Copyright 2010-2012 Quantcast Corp.
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
// \brief Kfs checksum (adler32) unit test.
//
//----------------------------------------------------------------------------

#include "kfsio/checksum.cc"

#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <stdlib.h>

int main(int argc, char** argv)
{
    if (argc > 1 && (!strcmp(argv[1], "-h") || !strcmp(argv[1], "--help"))) {
        printf("Usage: %s [flags]\n"
               "       flags can be any combination of 'c', 'n', 'd'.\n"
               "       c: test adler32 combine.\n"
               "       n: don't pad with 0.\n"
               "       d: debug.\n"
               "       The test reads input from STDIN ended by Ctrl+D.\n",
               argv[0]);
        return 0;
    }

    static char   buf[KFS::CHECKSUM_BLOCKSIZE * 4];
    char*         p = buf;
    ssize_t       n = 0;
    unsigned long o = 0;
    const bool    padd  = argc <= 1 || strchr(argv[1], 'n') == 0;
    const bool    tcomb = argc > 1 && strchr(argv[1], 'c');
    const bool    debug = argc > 1 && strchr(argv[1], 'd');
    char* const   e = p + (tcomb ? sizeof(buf) : KFS::CHECKSUM_BLOCKSIZE);

    do {
        while (p < e && (n = read(0, p, e - p)) > 0) {
            p += n;
        }
        if (p <= buf) {
            break;
        }
        const size_t len = (padd ? e : p) - buf;
        if (padd && p < e) {
            memset(p, 0, e - p);
        }
        const uint32_t cksum = KFS::ComputeBlockChecksum(buf, len);
        if (tcomb) {
            uint32_t cck = 0;
            KFS::ComputeChecksums(buf, len, &cck);
            if (cck != cksum) {
                printf("mismatch %lu %lu %u %u\n", o, (unsigned long)len,
                    (unsigned int)cksum, (unsigned int)cck);
                abort();
            }
        }
        if (! tcomb || debug) {
            printf("%lu %lu %u\n", o, (unsigned long)len, (unsigned int)cksum);
        }
        o += len;
        p = buf;
    } while (n > 0);
    return 0;
}
