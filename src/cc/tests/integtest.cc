#include "tests/integtest.h"

#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <gtest/gtest.h>
#include <libgen.h>
#include <limits.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "tests/environments/MetaserverEnvironment.h"
#include "tests/environments/ChunkserverEnvironment.h"

namespace KFS {
namespace Test {

using namespace std;

static const string kBuildTypeDebug = "debug";
static const string kBuildTypeRelease = "release";
static const string kContinuousIntegrationEnvironmentVariable = "CI";

MetaserverEnvironment* QFSTest::sMetaserver;
vector<ChunkserverEnvironment*> QFSTest::sChunkservers;

QFSTestEnvironment::QFSTestEnvironment()
    : mPid(-1)
{ }

void
QFSTestEnvironment::SetUp()
{
    char* buildType = getenv("BUILD_TYPE");
    if (buildType == NULL) {
        mBuildType = kBuildTypeDebug;
    }
    else {
        mBuildType = buildType;
    }

    ASSERT_TRUE(Start());
}

void
QFSTestEnvironment::TearDown()
{
    ASSERT_TRUE(Stop());

    if (getenv(kContinuousIntegrationEnvironmentVariable.c_str()) != NULL) {
        ifstream in(mLogFile.c_str());
        string line;
        while (getline(in, line)) {
            cout << line << endl;
        }
    }

    if (!QFSTestUtils::IsProcessAlive(mPid)) {
        return;
    }

    QFSTestUtils::TermProcess(mPid, 0);
    mPid = -1;
}

void
QFSTest::Init(int numChunkservers)
{
    QFSTest::sMetaserver = new MetaserverEnvironment;;

    for (int i = 0; i < numChunkservers; i++) {
        ChunkserverEnvironment* env =
            new ChunkserverEnvironment(QFSTest::sMetaserver);
        sChunkservers.push_back(env);
    }
}

void
QFSTest::Destroy()
{
    QFSTest::sMetaserver->TearDown();
    for (unsigned int i = 0; i < QFSTest::sChunkservers.size(); i++) {
        QFSTest::sChunkservers[i]->TearDown();
    }
}

void
QFSTest::SetUp()
{

}

void
QFSTest::TearDown()
{

}

int
QFSTestUtils::CreateTempFile(string* path)
{
     string filename = QFSTestUtils::kTestHome + "/tmp_file.XXXXXX";
     int fd = mkstemp(const_cast<char*>(filename.c_str()));
     EXPECT_NE(-1, fd) << "Could not mkstep: " << strerror(errno);

     path->assign(filename);
     return fd;
}

string
QFSTestUtils::WriteTempFile(const string& data)
{
    string filename;
    int fd = QFSTestUtils::CreateTempFile(&filename);

    size_t bytes = 0;
    do {
        ssize_t ret = write(fd, data.data() + bytes, data.size() - bytes);
        EXPECT_NE(-1, ret);

        bytes += ret;
    } while (bytes < data.size());

    close(fd);
    return filename;
}

bool
QFSTestUtils::FileExists(const string& path, struct stat* s) {
    bool ownMemory = false;
    if (s == NULL) {
        s = new struct stat;
        ownMemory = true;
    }

    bool returnValue = false;
    if (stat(path.c_str(), s) == -1) {
        returnValue = false;
        goto finish;
    }

    returnValue = true;
    goto finish;

finish:
    if (ownMemory) {
        delete s;
    }

    return returnValue;
}

bool
QFSTestUtils::IsFile(const string& path) {
    struct stat s;
    return FileExists(path, &s) && S_ISREG(s.st_mode);
}

bool
QFSTestUtils::IsDirectory(const string& path) {
    struct stat s;
    return FileExists(path, &s) && S_ISDIR(s.st_mode);
}

uint16_t
QFSTestUtils::GetRandomPort(int type, const string& ip)
{
    for (unsigned int i = 0 ; i < 20 ; i++) {
        int fd = socket(AF_INET, type, 0);
        if (fd == -1) {
            continue;
        }

        int port =  rand() % (25*1024) + 1024;
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

        if (bind(fd, (struct sockaddr *)&(addr), sizeof(addr)) != 0) {
            close(fd);
            continue;
        }

        close(fd);
        return ntohs(addr.sin_port);
    }

    ADD_FAILURE() << "Couldn't find an open port to bind to";
    return -1;
}

string
QFSTestUtils::CreateTempDirectory() {
    string fullpath = QFSTestUtils::kTestHome + "/temp_dir.XXXXXX";
    EXPECT_STRNE(NULL, mkdtemp(const_cast<char*>(fullpath.c_str())));
    return fullpath;
}

void
QFSTestUtils::RemoveForcefully(const string& path)
{
    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        return;
    }

    if (S_ISDIR(st.st_mode)) {
        DIR* dir = opendir(path.c_str());
        struct dirent *de;

        if (dir == NULL) {
            perror(path.c_str());
            return;
        }

        while ((de = readdir(dir))) {
            if (de->d_name[0] == '.') {
                continue;
            }

            string newpath = path + de->d_name;
            RemoveForcefully(newpath);
        }

        closedir(dir);
    }

    if (remove(path.c_str()) == -1) {
        perror(path.c_str());
    }
}

string QFSTestUtils::GetProcessName(pid_t pid) {
    ostringstream oss;
    oss << "/proc/" << pid << "/exe";
    string path = oss.str();

    if (!QFSTestUtils::FileExists(path)) {
        return "unknown";
    }

    char name[PATH_MAX];
    memset(name, 0, sizeof(name));

    ssize_t size = readlink(path.c_str(), name, sizeof(name) - 1);
    EXPECT_NE(-1, size);

    name[size] = '\0';
    return basename(name);
}

void
QFSTestUtils::TermProcess(pid_t pid, int exitCode)
{
    std::cout << "Killing " << QFSTestUtils::GetProcessName(pid) << " (pid "
        << pid << ")" << std::endl;

    QFSTestUtils::SendSignal(pid, SIGTERM);
    QFSTestUtils::WaitForProcess(pid, exitCode);
}

void
QFSTestUtils::SendSignal(pid_t pid, int signum)
{
    if (pid < 2) {
        cout << "WARNING: pid " << pid << " is less than 2. Not sending signal "
            << strsignal(signum) << "." << endl;
        return;
    }

    kill(pid, signum);
}

void
QFSTestUtils::WaitForProcess(pid_t pid, int exitCode)
{
    int status = 0;
    waitpid(pid, &status, 0);

    if (WIFEXITED(status)
            || (WIFSIGNALED(status) && WTERMSIG(status) == SIGTERM)) {
        EXPECT_EQ(exitCode, WEXITSTATUS(status))
            << QFSTestUtils::GetProcessName(pid) << " exited unexpectedly.";
    }
    else {
        std::ostringstream oss;
        oss << QFSTestUtils::GetProcessName(pid) << " did not exit cleanly.";

        if (WIFSIGNALED(status)) {
            oss << " Killed by signal " << WTERMSIG(status);

            if (WCOREDUMP(status)) {
                oss << " (core dumped)";
            }
        }

        FAIL() << oss.str();
    }
}

bool
QFSTestUtils::IsProcessAlive(pid_t pid)
{
    return pid != -1 && !waitpid(pid, NULL, WNOHANG);
}

} // namespace Test
} // namespace KFS
