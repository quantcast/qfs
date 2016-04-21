#include "tests/environments/MetaserverEnvironment.h"

#include <errno.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/lexical_cast.hpp>
#include <iostream>
#include <map>
#include <string>

namespace KFS {
namespace Test {

using namespace std;
using boost::lexical_cast;

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

    map<string, string> c;
    c["metaServer.clientPort"] = lexical_cast<string>(mClientPort);
    c["metaServer.chunkServerPort"] = lexical_cast<string>(mChunkserverPort);
    c["metaServer.logDir"] = mTransactionLogDir;
    c["metaServer.cpDir"] = mCheckpointDir;
    c["metaServer.recoveryInterval"] = "30";
    c["metaServer.createEmptyFs"] = "1";
    c["metaServer.clusterKey"] = "some-random-unique-identifier";
    c["metaServer.msgLogWriter.logLevel"] = "INFO";
    c["chunkServer.msgLogWriter.logLevel"] = "NOTICE";

    string config;
    for (map<string, string>::const_iterator itr = c.begin(); itr != c.end();
            ++itr) {
        config += itr->first + " = " + itr->second + "\n";
    }

    return QFSTestUtils::WriteTempFile(config);
}

bool
MetaserverEnvironment::Start()
{
    if (QFSTestUtils::IsProcessAlive(mPid)) {
        return true;
    }

    string configPath = GenerateConfig();
    string binaryPath = "build/" + mBuildType + "/bin/metaserver";

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
