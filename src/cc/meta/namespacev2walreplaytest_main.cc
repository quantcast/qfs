//---------------------------------------------------------- -*- Mode: C++ -*-
// Minimal integration test:
//   write nv2batch WAL line -> Replay::playLine() -> validate NamespaceV2 state.
//
// This intentionally bypasses LogWriter / disk IO, and tests the log record
// format + replay parser + NamespaceV2 apply/commit chain.

#include "Replay.h"
#include "NamespaceV2.h"

#include "kfsio/Base64.h"

#include <stdint.h>

#include <string>
#include <vector>
#include <iostream>

using std::string;
using std::vector;
using std::cout;

namespace
{

static void
AppendLe(
    string&   out,
    uint64_t  v,
    size_t    bytes)
{
    for (size_t i = 0; i < bytes; i++) {
        out.push_back((char)(v >> (i * 8)));
    }
}

static void
AppendU8(string& out, uint8_t v)  { AppendLe(out, v, 1); }
static void
AppendU16(string& out, uint16_t v) { AppendLe(out, v, 2); }
static void
AppendU32(string& out, uint32_t v) { AppendLe(out, v, 4); }
static void
AppendU64(string& out, uint64_t v) { AppendLe(out, v, 8); }
static void
AppendI16(string& out, int16_t v)  { AppendLe(out, (uint16_t)v, 2); }
static void
AppendI64(string& out, int64_t v)  { AppendLe(out, (uint64_t)v, 8); }

static string
EncodeNv2BatchPayload()
{
    // Two ops:
    //  1) mkdir  /d
    //  2) create /d/f
    const KFS::fid_t   root = KFS::ROOTFID;
    const KFS::fid_t   dFid = 1001;
    const KFS::fid_t   fFid = 1002;
    // Use contiguous txn ids, and let main() seed committed txn to 0.
    const uint64_t dTxn = 1;
    const uint64_t fTxn = 2;
    const uint32_t user = 1;
    const uint32_t group = 1;
    const uint16_t modeDir = 0755;
    const uint16_t modeFile = 0644;
    const int16_t  repl = 1;
    const int64_t  mtime = 123456789;

    string payload;
    payload.reserve(256);

    // opType=2 mkdir
    AppendU8(payload, 2);
    AppendI64(payload, (int64_t)root);
    AppendI64(payload, (int64_t)dFid);
    AppendU64(payload, dTxn);
    AppendU32(payload, user);
    AppendU32(payload, group);
    AppendU16(payload, modeDir);
    AppendI16(payload, 0);
    AppendI64(payload, mtime);
    const string dname("d");
    AppendU16(payload, (uint16_t)dname.size());
    payload.append(dname);

    // opType=1 create file
    AppendU8(payload, 1);
    AppendI64(payload, (int64_t)dFid);
    AppendI64(payload, (int64_t)fFid);
    AppendU64(payload, fTxn);
    AppendU32(payload, user);
    AppendU32(payload, group);
    AppendU16(payload, modeFile);
    AppendI16(payload, repl);
    AppendI64(payload, mtime);
    const string fname("f");
    AppendU16(payload, (uint16_t)fname.size());
    payload.append(fname);

    return payload;
}

static string
Base64Encode(
    const string& bytes)
{
    vector<char> buf((size_t)KFS::Base64::GetEncodedMaxBufSize((int)bytes.size()));
    const int len = KFS::Base64::Encode(bytes.data(), (int)bytes.size(), &buf[0], true);
    if (len <= 0) {
        return string();
    }
    return string(&buf[0], len);
}

static int
Fail(
    const char* msg)
{
    cout << "FAILED: " << msg << "\n";
    return 1;
}

} // anonymous

int
main()
{
    // Ensure namespace v2 store exists in this process.
    KFS::NamespaceV2::NamespaceStore& store = KFS::NamespaceV2::GetStore();
    // Simulate "checkpoint committed txn == 0" so that replay can commit a
    // contiguous txn range starting at 1.
    store.CommitThroughRange(0, 0);
    (void)store;

    const string payload = EncodeNv2BatchPayload();
    const string b64 = Base64Encode(payload);
    if (b64.empty()) {
        return Fail("base64 encode");
    }

    // One nv2batch line + one placeholder, to mimic WAL sequence count.
    const string line1 = "nv2batch/c/2/b/" + b64 + "\n";
    const string line2 = "nv2batchc\n";

    // Use a fresh block seq for each line.
    int status = KFS::replayer.playLine(line1.data(), (int)line1.size(), 1);
    if (status != 0) {
        cout << "nv2batch line: " << line1;
        return Fail("replay nv2batch");
    }
    status = KFS::replayer.playLine(line2.data(), (int)line2.size(), 2);
    if (status != 0) {
        return Fail("replay nv2batchc");
    }

    // Verify namespace state after replay commit.
    KFS::NamespaceV2::LookupResult d;
    if (KFS::NamespaceV2::GetStore().Lookup(KFS::ROOTFID, "d", d) != 0 ||
            d.type != KFS::NamespaceV2::kInodeTypeDir) {
        return Fail("lookup dir d");
    }
    KFS::NamespaceV2::LookupResult f;
    if (KFS::NamespaceV2::GetStore().Lookup(d.fid, "f", f) != 0 ||
            f.type != KFS::NamespaceV2::kInodeTypeFile) {
        return Fail("lookup file f");
    }
    cout << "NamespaceV2 WAL replay integration test passed\n";
    return 0;
}
