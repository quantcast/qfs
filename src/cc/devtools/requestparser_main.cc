//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/05/14
// Author: Mike Ovsiannikov
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
//
//----------------------------------------------------------------------------

#include "common/RequestParser.h"
#include "common/Properties.h"

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>

using namespace KFS;

class AbstractTest
{
public:
    int64_t seq;

    AbstractTest()
        : seq(-1)
        {}
    virtual ~AbstractTest()
        {}
    bool Validate() const { return true; }
    virtual std::ostream& Show(
        std::ostream& inStream)
        { return inStream; }
    bool ValidateRequestHeader(
        const char* /* name */,
        size_t      /* nameLen */,
        const char* /* header */,
        size_t      /* headerLen */,
        bool        /* hasChecksum */,
        uint32_t    /* checksum */)
        { return true; }
    bool HandleUnknownField(
        const char* /* key */, size_t /* keyLen */,
        const char* /* val */, size_t /* valLen */)
        { return true; }
    template<typename T> static T& ParserDef(
        T& inParser)
    {
        return inParser
            .Def("Cseq", &AbstractTest::seq, int64_t(-1))
        ;
    }
};

class AbstractTest1
{
public:
    int vers;

    AbstractTest1()
        : vers(-1)
        {}
    virtual ~AbstractTest1()
        {}
    template<typename T> static T& ParserDef(
        T& inParser)
    {
        return inParser
            .Def("Client-Protocol-Version", &AbstractTest1::vers, -1)
        ;
    }
};

class Test : public AbstractTest, public AbstractTest1
{
public:
    StringBufT<64> host;
    StringBufT<256> path;
    std::string hostStr;
    std::string pathStr;
    int64_t   fid;
    int64_t   offset;
    int64_t   reserve;
    bool      append;
    int       maxAppenders;
    double    doubleTest;

    Test()
        : AbstractTest(),
          host(),
          path(),
          hostStr(),
          pathStr(),
          fid(-1),
          offset(-1),
          reserve(-1),
          append(false),
          maxAppenders(64),
          doubleTest(-1)
        {}

    // bool Validate() const { return false; }
    virtual ~Test()
        {}
    template<typename T> static T& ParserDef(
        T& inParser)
    {
       return
            AbstractTest1::ParserDef(
            AbstractTest::ParserDef(
                inParser
            ))
            // .Def("Cseq",                    &Test::seq,          int64_t(-1))
            // .Def("Client-Protocol-Version", &Test::vers,         -1         )
            .Def("Client-host",             &Test::host                     )
            .Def("Pathname",                &Test::path                     )
            .Def("File-handle",             &Test::fid,          int64_t(-1))
            .Def("Chunk-offset",            &Test::offset,       int64_t(-1))
            .Def("Chunk-append",            &Test::append,       false      )
            .Def("Space-reserve",           &Test::reserve,      int64_t(-1))
            .Def("Max-appenders",           &Test::maxAppenders, 64         )
            .Def("Double test",             &Test::doubleTest,   -1.        )
        ;
    }
    template<typename T> void Load(
        const Properties& props,
        const char*       name,
        T&                val,
        T                 def=T())
        { val = props.getValue(name, def); }
    void Load(
        const Properties& props)
    {
            Load(props, "Cseq",                    seq,          int64_t(-1));
            Load(props, "Client-Protocol-Version", vers,         -1         );
            Load(props, "Client-host",             hostStr                  );
            Load(props, "Pathname",                pathStr                  );
            Load(props, "File-handle",             fid,          int64_t(-1));
            Load(props, "Chunk-offset",            offset,       int64_t(-1));
            Load(props, "Chunk-append",            append,       false      );
            Load(props, "Space-reserve",           reserve,      int64_t(-1));
            Load(props, "Max-appenders",           maxAppenders, 64         );
            Load(props, "Double test",             doubleTest,   -1.        );
    }
    static AbstractTest* Load(
        std::istream& is)
    {
        Test* const res = new Test;
        const char separator = ':';
        Properties props;
        props.loadProperties(is, separator);
        res->Load(props);
        return res;
    }
    virtual std::ostream& Show(
        std::ostream& inStream)
    {
        return inStream <<
            "Cseq:                    " << seq          << "\r\n"
            "Version:                 " "KFS/1.0"          "\r\n"
            "Client-Protocol-Version: " << vers         << "\r\n"
            "Client-host:             " << host << hostStr << "\r\n"
            "Pathname:                " << path << pathStr << "\r\n"
            "File-handle:             " << fid          << "\r\n"
            "Chunk-offset:            " << offset       << "\r\n"
            "Chunk-append:            " << append       << "\r\n"
            "Space-reserve:           " << reserve      << "\r\n"
            "Max-appenders:           " << maxAppenders << "\r\n"
            "Double test:             " << doubleTest   << "\r\n"
        ;
    }
};

/*
ALLOCATE\r
Cseq: $seq\r
Version: KFS/1.0\r
Client-Protocol-Version: 100\r
Client-host: somehostname\r
Pathname: /sort/job/1/fanout/27/file.27\r
File-handle: $fid\r
Chunk-offset: 0\r
Chunk-append: 1\r
Space-reserve: 0\r
Max-appenders: 640000000\r
*/
/*
    To benchmark:
    ../src/test-scripts/allocatesend.pl 1e6 | ( time src/cc/devtools/requestparser_test q )
*/

typedef RequestHandler<AbstractTest> ReqHandler;
static const ReqHandler& MakeRequestHandler()
{
    static ReqHandler sHandler;
    return sHandler
        .MakeParser<Test>("ALLOCATE")
        .MakeParser<Test>("xALLOCATE")
    ;
}
static const ReqHandler& sReqHandler = MakeRequestHandler();

int
main(int argc, char** argv)
{
    if (argc <= 1 || (!strcmp(argv[1], "-h") || !strcmp(argv[1], "--help"))) {
        return 0;
    }

    static char buf[1 << 20];
    char* ptr = buf;
    char* end = buf + sizeof(buf);
    BufferInputStream myis;
    ssize_t     nrd;
    const bool  quiet   = argc > 1 && strchr(argv[1], 'q');
    const bool  noparse = argc > 1 && strchr(argv[1], 'n');
    const bool  alloc   = argc > 1 && strchr(argv[1], 'a');
    const bool  useprop = argc > 1 && strchr(argv[1], 'p');

    while ((nrd = read(0, ptr, end - ptr)) > 0) {
        end = ptr + nrd;
        ptr = buf;
        if (*ptr == '\n') {
            ptr++;
        }
        char* re = ptr;
        while ((re = (char*)memchr(re, '\n', end - re))) {
            if (re + 2 >= end) {
                break;
            }
            if (re[-1] != '\r' || re[1] != '\r' || re[2] != '\n') {
                re++;
                continue;
            }
            re += 3;
            if (! quiet) {
                std::cout << "Request:\n";
                std::cout.write(ptr, re - ptr);
            }
            if (! noparse || alloc) {
                AbstractTest* const tst = useprop ?
                    Test::Load(myis.Set(ptr, noparse ? 0 : re - ptr)) :
                    sReqHandler.Handle(ptr, noparse ? 0 : re - ptr);
                if (tst) {
                    if (! quiet) {
                        std::cout << "Parsed request:\n";
                        tst->Show(std::cout);
                    }
                    delete tst;
                } else {
                    std::cout << "parse failure\nRequest:\n";
                    std::cout.write(ptr, re - ptr);
                }
            }
            ptr = re;
        }
        memmove(buf, ptr, end - ptr);
        ptr = buf + (end - ptr);
        end = buf + sizeof(buf);
    }
    return 0;
}
