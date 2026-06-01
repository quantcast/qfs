//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Micro benchmark for RFC-0001 NamespaceV2 scaffolding.
//
// Copyright 2026 Quantcast Corporation. All rights reserved.
//
// This file is part of Kosmos File System (KFS).
//
// Licensed under the Apache License, Version 2.0.
//
//----------------------------------------------------------------------------

#include "NamespaceV2.h"

#include <stdint.h>
#include <sys/time.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

using namespace KFS;
using namespace KFS::NamespaceV2;
using std::cout;
using std::string;
using std::vector;

namespace
{

struct Options
{
    uint64_t entries;
    uint64_t dirs;
    uint64_t lookupSamples;
    size_t   readdirPageSize;
    int      largeThreshold;

    Options()
        : entries(1000000),
          dirs(1),
          lookupSamples(100000),
          readdirPageSize(1000),
          largeThreshold(Config().dirLargeThreshold)
        {}
};

    uint64_t
NowUsec()
{
    timeval tv;
    gettimeofday(&tv, 0);
    return uint64_t(tv.tv_sec) * 1000000 + tv.tv_usec;
}

    double
OpsPerSec(
    uint64_t count,
    uint64_t usec)
{
    return usec == 0 ? 0 : (double(count) * 1000000.0) / double(usec);
}

    bool
ParseUInt64(
    const char* namePtr,
    const char* valuePtr,
    uint64_t&   value)
{
    char* endPtr = 0;
    errno = 0;
    const unsigned long long parsed = strtoull(valuePtr, &endPtr, 10);
    if (errno != 0 || ! endPtr || *endPtr != 0) {
        cout << "invalid " << namePtr << ": " << valuePtr << "\n";
        return false;
    }
    value = uint64_t(parsed);
    return true;
}

    bool
ParseSize(
    const char* namePtr,
    const char* valuePtr,
    size_t&     value)
{
    uint64_t parsed = 0;
    if (! ParseUInt64(namePtr, valuePtr, parsed)) {
        return false;
    }
    value = size_t(parsed);
    return true;
}

    bool
ParseInt(
    const char* namePtr,
    const char* valuePtr,
    int&        value)
{
    char* endPtr = 0;
    errno = 0;
    const long parsed = strtol(valuePtr, &endPtr, 10);
    if (errno != 0 || ! endPtr || *endPtr != 0 || parsed <= 0) {
        cout << "invalid " << namePtr << ": " << valuePtr << "\n";
        return false;
    }
    value = int(parsed);
    return true;
}

    void
Usage(
    const char* progPtr)
{
    cout <<
        "Usage: " << progPtr << " [options]\n"
        "  --entries N          number of file creates, default 1000000\n"
        "  --dirs N             parent directories to spread creates, default 1\n"
        "  --lookup-samples N   committed lookups to sample, default 100000\n"
        "  --readdir-page N     committed readdir page size, default 1000\n"
        "  --threshold N        Small to Large promotion threshold, default 4096\n"
    ;
}

    bool
ParseOptions(
    int      argc,
    char**   argv,
    Options& options)
{
    for (int i = 1; i < argc; i++) {
        const char* const argPtr = argv[i];
        if (strcmp(argPtr, "--help") == 0 || strcmp(argPtr, "-h") == 0) {
            Usage(argv[0]);
            return false;
        }
        if (i + 1 >= argc) {
            cout << "missing value for " << argPtr << "\n";
            Usage(argv[0]);
            return false;
        }
        const char* const valuePtr = argv[++i];
        if (strcmp(argPtr, "--entries") == 0) {
            if (! ParseUInt64(argPtr, valuePtr, options.entries)) {
                return false;
            }
        } else if (strcmp(argPtr, "--dirs") == 0) {
            if (! ParseUInt64(argPtr, valuePtr, options.dirs)) {
                return false;
            }
        } else if (strcmp(argPtr, "--lookup-samples") == 0) {
            if (! ParseUInt64(argPtr, valuePtr, options.lookupSamples)) {
                return false;
            }
        } else if (strcmp(argPtr, "--readdir-page") == 0) {
            if (! ParseSize(argPtr, valuePtr, options.readdirPageSize)) {
                return false;
            }
        } else if (strcmp(argPtr, "--threshold") == 0) {
            if (! ParseInt(argPtr, valuePtr, options.largeThreshold)) {
                return false;
            }
        } else {
            cout << "unknown option: " << argPtr << "\n";
            Usage(argv[0]);
            return false;
        }
    }
    options.entries       = std::max<uint64_t>(1, options.entries);
    options.dirs          = std::max<uint64_t>(1, options.dirs);
    options.lookupSamples = std::min(options.entries,
        std::max<uint64_t>(1, options.lookupSamples));
    options.readdirPageSize = std::max<size_t>(1, options.readdirPageSize);
    return true;
}

    string
MakeName(
    const char* prefixPtr,
    uint64_t    index)
{
    char buf[64];
    snprintf(buf, sizeof(buf), "%s%llu", prefixPtr,
        (unsigned long long)index);
    return string(buf);
}

    int
CreateParentDirs(
    NamespaceStore& store,
    const Options&  options,
    vector<fid_t>&  parentFids)
{
    parentFids.clear();
    if (options.dirs == 1) {
        parentFids.push_back(store.GetRootFid());
        return 0;
    }
    parentFids.reserve(size_t(options.dirs));
    for (uint64_t i = 0; i < options.dirs; i++) {
        CreateResult result;
        const int status = store.Create(
            store.GetRootFid(), MakeName("d_", i), kInodeTypeDir, &result);
        if (status != 0) {
            cout << "mkdir failed index=" << i << " status=" << status << "\n";
            return status;
        }
        parentFids.push_back(result.fid);
    }
    store.CommitThrough(store.GetLastTxn());
    return 0;
}

    int
BenchmarkCreate(
    NamespaceStore&     store,
    const Options&      options,
    const vector<fid_t>& parentFids)
{
    const uint64_t startUsec = NowUsec();
    for (uint64_t i = 0; i < options.entries; i++) {
        const fid_t parentFid = parentFids[size_t(i % parentFids.size())];
        const int status = store.ApplyCreatePending(
            parentFid, MakeName("f_", i), kInodeTypeFile);
        if (status != 0) {
            cout << "create failed index=" << i << " status=" << status <<
                "\n";
            return status;
        }
    }
    const uint64_t createUsec = NowUsec() - startUsec;
    cout << "create count=" << options.entries <<
        " usec=" << createUsec <<
        " ops_per_sec=" << OpsPerSec(options.entries, createUsec) << "\n";

    const uint64_t commitStartUsec = NowUsec();
    store.CommitThrough(store.GetLastTxn());
    const uint64_t commitUsec = NowUsec() - commitStartUsec;
    cout << "commit txn=" << store.GetCommittedTxn() <<
        " usec=" << commitUsec << "\n";
    return 0;
}

    int
BenchmarkLookup(
    const NamespaceStore& store,
    const Options&        options,
    const vector<fid_t>&  parentFids)
{
    uint64_t found = 0;
    const uint64_t stride = std::max<uint64_t>(
        1, options.entries / options.lookupSamples);
    const uint64_t startUsec = NowUsec();
    for (uint64_t n = 0, i = 0; n < options.lookupSamples; n++,
            i = (i + stride) % options.entries) {
        const fid_t parentFid = parentFids[size_t(i % parentFids.size())];
        LookupResult result;
        const int status = store.Lookup(parentFid, MakeName("f_", i), result);
        if (status == 0) {
            found++;
        } else {
            cout << "lookup failed index=" << i << " status=" << status <<
                "\n";
            return status;
        }
    }
    const uint64_t lookupUsec = NowUsec() - startUsec;
    cout << "lookup count=" << options.lookupSamples <<
        " found=" << found <<
        " usec=" << lookupUsec <<
        " ops_per_sec=" << OpsPerSec(options.lookupSamples, lookupUsec) <<
        "\n";
    return 0;
}

    int
BenchmarkReaddir(
    const NamespaceStore& store,
    const Options&        options,
    const vector<fid_t>&  parentFids)
{
    uint64_t entryCount = 0;
    uint64_t pageCount  = 0;
    const uint64_t startUsec = NowUsec();
    for (size_t i = 0; i < parentFids.size(); i++) {
        ReaddirCookie cookie;
        const ReaddirCookie* cookiePtr = 0;
        do {
            ReaddirResult result;
            const int status = store.Readdir(parentFids[i], cookiePtr,
                options.readdirPageSize, result);
            if (status != 0) {
                cout << "readdir failed parent_index=" << i <<
                    " status=" << status << "\n";
                return status;
            }
            entryCount += result.entries.size();
            pageCount++;
            cookie = result.nextCookie;
            cookiePtr = result.moreEntriesFlag ? &cookie : 0;
            if (! result.moreEntriesFlag) {
                break;
            }
        } while (true);
    }
    const uint64_t readdirUsec = NowUsec() - startUsec;
    cout << "readdir entries=" << entryCount <<
        " pages=" << pageCount <<
        " usec=" << readdirUsec <<
        " entries_per_sec=" << OpsPerSec(entryCount, readdirUsec) << "\n";
    return entryCount == options.entries ? 0 : -EIO;
}

} // namespace

    int
main(
    int    argc,
    char** argv)
{
    Options options;
    if (! ParseOptions(argc, argv, options)) {
        return 1;
    }

    Config cfg;
    cfg.enabledFlag       = true;
    cfg.dirLargeThreshold = options.largeThreshold;
    NamespaceStore store(cfg);
    vector<fid_t> parentFids;

    cout << "namespacev2bench entries=" << options.entries <<
        " dirs=" << options.dirs <<
        " threshold=" << options.largeThreshold <<
        " lookup_samples=" << options.lookupSamples <<
        " readdir_page=" << options.readdirPageSize << "\n";

    uint64_t startUsec = NowUsec();
    int status = CreateParentDirs(store, options, parentFids);
    uint64_t setupUsec = NowUsec() - startUsec;
    if (status != 0) {
        return 1;
    }
    cout << "setup dirs=" << parentFids.size() <<
        " usec=" << setupUsec << "\n";

    status = BenchmarkCreate(store, options, parentFids);
    if (status != 0) {
        return 1;
    }
    status = BenchmarkLookup(store, options, parentFids);
    if (status != 0) {
        return 1;
    }
    status = BenchmarkReaddir(store, options, parentFids);
    if (status != 0) {
        return 1;
    }
    return 0;
}
