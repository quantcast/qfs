//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/05/20
// Author: Mike Ovsainnikov
//
// Copyright 2011-2012 Quantcast Corp.
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
// \brief Pseudo random number generator for various test purposes.
//
//----------------------------------------------------------------------------

#include <boost/random/mersenne_twister.hpp>

#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <unistd.h>
#include <cstdio>
#include <iostream>

using std::cerr;
typedef boost::mt19937               RandomGenerator;
typedef RandomGenerator::result_type ResultType;

int
main(int argc, char **argv)
{
    if (argc <= 1 || strncmp(argv[1], "-g", 2) != 0) {
        cerr << argv[0] << " -g [size] [seed]\n";
        return 1;
    }
    int64_t len =
        argc > 2 ? (int64_t)atof(argv[2]) : ((int64_t)1 << 63);
    const ResultType seed =
        argc > 3 ? (ResultType)atol(argv[3]) : (ResultType)time(0);
    static RandomGenerator::result_type buf[100000];
    RandomGenerator gen(seed);
    const int64_t   bs = (int64_t)sizeof(buf);
    for (; len > 0;  len -= bs) {
        size_t n = (size_t)(len > bs ? bs : len);
        size_t r = (n + sizeof(buf[0]) - 1) / sizeof(buf[0]);
        while (r > 0) {
            buf[--r] = gen();
        }
        ssize_t     w = 0;
        const char* p = (const char*)buf;
        while ((w = write(1, p, n)) < (ssize_t)n) {
            if (w < 0) {
                perror("write");
                return 1;
            }
            n -= w;
            p += w;
        }
    }
    return 0;
}
