#include "tests/environments/MetaserverEnvironment.h"

#include <errno.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <string>

#include "common/StrUtils.h"

namespace KFS {
namespace Test {

using namespace std;

MetaserverEnvironment::MetaserverEnvironment()
    : mClientPort(-1)
    , mChunkserverPort(-1)
{ }

string
MetaserverEnvironment::GenerateConfig()
{
    mClientPort = QFSTestUtils::GetRandomPort(SOCK_STREAM, "127.0.0.1");
    mChunkserverPort = QFSTestUtils::GetRandomPort(SOCK_STREAM, "127.0.0.1");

    mTransactionLogDir = QFSTestUtils::CreateTempDirectory();
    mCheckpointDir = QFSTestUtils::CreateTempDirectory();

    string config;
    config += StrUtils::str_printf("metaServer.clientPort = %d\n", mClientPort);
    config += StrUtils::str_printf("metaServer.chunkServerPort = %d\n",
            mChunkserverPort);
    config += StrUtils::str_printf("metaServer.logDir = %s\n",
            mTransactionLogDir.c_str());
    config += StrUtils::str_printf("metaServer.cpDir = %s\n",
            mCheckpointDir.c_str());
    config += "metaServer.recoveryInterval = 30\n";
    config += "metaServer.createEmptyFs = 1\n";
    config += "metaServer.clusterKey = some-random-unique-identifier\n";
    config += "metaServer.msgLogWriter.logLevel = INFO\n";
    config += "chunkServer.msgLogWriter.logLevel = NOTICE\n";

    return QFSTestUtils::WriteTempFile(config);
}

bool
MetaserverEnvironment::Start()
{
    if (QFSTestUtils::IsProcessAlive(mPid)) {
        return true;
    }

    string configPath = GenerateConfig();
    string binaryPath = StrUtils::str_printf("build/%s/bin/metaserver",
            mBuildType.c_str());

    int fd = QFSTestUtils::CreateTempFile(&mLogFile);

    pid_t pid = fork();
    if (pid < 0) {
        ADD_FAILURE() << "metaserver: fork failed: " << strerror(errno);
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

    cout << "starting metaserver (pid: " << pid
        << ", logfile: " << mLogFile
        << ")" << endl;

    // TODO(fsareshwala): replace this sleep with a check by the KFS client to
    // the metaserver that we can connect.
    sleep(1);

    mPid = pid;
    return true;
}

} // namespace Test
} // namespace KFS
