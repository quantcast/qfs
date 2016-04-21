#include "tests/environments/ChunkserverEnvironment.h"

#include <errno.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/lexical_cast.hpp>
#include <iostream>
#include <map>
#include <string>

#include "tests/environments/MetaserverEnvironment.h"

namespace KFS {
namespace Test {

using namespace std;
using boost::lexical_cast;

ChunkserverEnvironment::ChunkserverEnvironment(const MetaserverEnvironment* m)
    : mMetaserverPort(m->GetChunkserverPort())
    , mClientPort(-1)
{ }

string
ChunkserverEnvironment::GenerateConfig()
{
    mClientPort = QFSTestUtils::GetRandomPort(SOCK_STREAM, "127.0.0.1");
    mChunkDir = QFSTestUtils::CreateTempDirectory();

    map<string, string> c;
    c["chunkServer.metaServer.hostname"] = "localhost";
    c["chunkServer.metaServer.port"] = lexical_cast<string>(mMetaserverPort);
    c["chunkServer.clientPort"] = lexical_cast<string>(mClientPort);
    c["chunkServer.chunkDir"] = mChunkDir;
    c["chunkServer.clusterKey"] = "some-random-unique-identifier";
    c["chunkServer.stdout"] = "/dev/null";
    c["chunkServer.stderr"] = "/dev/null";
    c["chunkServer.msgLogWriter.logLevel"] = "INFO";

    string config;
    for (map<string, string>::const_iterator itr = c.begin(); itr != c.end();
            ++itr) {
        config += itr->first + " = " + itr->second + "\n";
    }

    return QFSTestUtils::WriteTempFile(config);
}

bool
ChunkserverEnvironment::Start()
{
    if (QFSTestUtils::IsProcessAlive(mPid)) {
        return true;
    }

    string configPath = GenerateConfig();
    string binaryPath = "build/" + mBuildType + "/bin/chunkserver";

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
