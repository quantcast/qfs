#include "tests/environments/ChunkserverEnvironment.h"

#include <errno.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <string>

#include "common/StrUtils.h"
#include "tests/environments/MetaserverEnvironment.h"

namespace KFS {
namespace Test {

using namespace std;

ChunkserverEnvironment::ChunkserverEnvironment(const MetaserverEnvironment* m)
    : mMetaserverPort(m->GetChunkserverPort())
    , mClientPort(-1)
{ }

string
ChunkserverEnvironment::GenerateConfig()
{
    mClientPort = QFSTestUtils::GetRandomPort(SOCK_STREAM, "127.0.0.1");
    mChunkDir = QFSTestUtils::CreateTempDirectory();

    string config;
    config += "chunkServer.metaServer.hostname = localhost\n";
    config += StrUtils::str_printf("chunkServer.metaServer.port = %d\n",
            mMetaserverPort);
    config += StrUtils::str_printf("chunkServer.clientPort = %d\n",
            mClientPort);
    config += StrUtils::str_printf("chunkServer.chunkDir = %s\n",
            mChunkDir.c_str());
    config += "chunkServer.clusterKey = some-random-unique-identifier\n";
    config += "chunkServer.stdout = /dev/null\n";
    config += "chunkServer.stderr = /dev/null\n";
    config += "chunkServer.msgLogWriter.logLevel = INFO\n";

    return QFSTestUtils::WriteTempFile(config);
}

bool
ChunkserverEnvironment::Start()
{
    if (QFSTestUtils::IsProcessAlive(mPid)) {
        return true;
    }

    string configPath = GenerateConfig();
    string binaryPath = StrUtils::str_printf("build/%s/bin/chunkserver",
            mBuildType.c_str());

    int fd = QFSTestUtils::CreateTempFile(&mLogFile);

    pid_t pid = fork();
    if (pid < 0) {
        ADD_FAILURE() << "chunkserver: fork failed: " << strerror(errno);
        return false;
    }
    else if (pid == 0) {
        setsid();
        EXPECT_NE(-1, dup2(fd, fileno(stderr)));
        close(fd);

        execl(binaryPath.c_str(), binaryPath.c_str(), configPath.c_str(), NULL);
        ADD_FAILURE() << binaryPath << ": execl failed: " << strerror(errno);
        abort();
    }

    cout << "starting chunkserver (pid: " << pid
        << ", logfile: " << mLogFile
        << ")" << endl;

    // TODO(fsareshwala): replace this sleep with a check by the KFS client to
    // the chunkserver that we can connect.
    sleep(1);

    mPid = pid;
    return true;
}

} // namespace Test
} // namespace KFS
