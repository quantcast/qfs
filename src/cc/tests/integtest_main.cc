#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>

#include <limits.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "tests/environments/ChunkserverEnvironment.h"
#include "tests/environments/MetaserverEnvironment.h"
#include "tests/integtest.h"

// TODO(fsareshwala): catch SIGINT and remove kill all environments and remove
// all files

using namespace KFS::Test;
using namespace std;

const string QFSTestUtils::kTestHome = "/tmp/qfs";

static void
RandInit()
{
    struct timeval now;
    gettimeofday(&now, NULL);

    int pid = getpid();
    int seed = (now.tv_sec * 1000 + now.tv_usec / 1000) ^ (pid * 0x5bd1e995);

    srand(seed);
    srandom(seed);
}

GTEST_API_ int
main(int argc, char **argv)
{
    RandInit();

    if (QFSTestUtils::FileExists(QFSTestUtils::kTestHome)) {
        QFSTestUtils::RemoveForcefully(QFSTestUtils::kTestHome);
    }

    mkdir(QFSTestUtils::kTestHome.c_str(), S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH);

    cout << "Running custom main() from tests/integtest_main" << endl;
    testing::InitGoogleTest(&argc, argv);

    static const int kNumChunkServers = 3;
    QFSTest::Init(kNumChunkServers);

    testing::AddGlobalTestEnvironment(QFSTest::sMetaserver);
    for (unsigned int i = 0; i < kNumChunkServers; i++) {
        testing::AddGlobalTestEnvironment(QFSTest::sChunkservers[i]);
    }

    int ret = RUN_ALL_TESTS();
    QFSTestUtils::RemoveForcefully(QFSTestUtils::kTestHome);
    return ret;
}
