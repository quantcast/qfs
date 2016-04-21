#ifndef TESTS_INTEGTEST
#define TESTS_INTEGTEST

#include <gtest/gtest.h>
#include <stdint.h>

int main(int, char**);

namespace KFS {
namespace Test {

using namespace std;

class MetaserverEnvironment;
class ChunkserverEnvironment;

/**
 * The QFSTestEnvironment is a base environment that all test environments
 * should inherit from. It takes care of basic setup and teardown for your
 * environments such that you only need to implement a Start() and Stop()
 * function.
 *
 * QFS mostly consists of a metaserver and many chunkservers. For the most part,
 * developers won't have to deal with this environment class since the
 * environments for the metaserver and chunkserver have already been written.
 * See {@link MetaserverEnvironment} and {@link ChunkserverEnvironment}.
 */
class QFSTestEnvironment : public ::testing::Environment
{
public:
    /**
     * The default constructor for QFSTestEnvironment does no work other than
     * instantiating variables.
     */
    QFSTestEnvironment();

    /**
     * SetUp handles setting up the environment at test start. It is called
     * directly by the Google Test framework and the casing (SetUp) is very
     * important.
     */
    virtual void SetUp();

    /**
     * TearDown handles tearing down the environment at test completion. It is
     * called directly by the Google Test framework and the casing (TearDown) is
     * very important.
     */
    virtual void TearDown();

protected:
    /**
     * Start is a pure virtual function that all descendants of the
     * QFSTestEnvironment will have to implement. The functionality placed in
     * Start should do whatever necessary in order to simulate the start of the
     * service your environment provides, e.g. generating a configuration file
     * and forking a metaserver binary.
     */
    virtual bool Start() = 0;

    /**
     * Stop should perform any tasks necessary before the unit test framework
     * kills the currently running service.
     *
     * @return True if the cleanup processes completed successfully
     */
    virtual bool Stop() { return true; }

    /**
     * mLogFile is the path to the logfile which contains the output of the
     * service simulated by this instance of QFSTestEnvironment. These logfiles
     * are available for review by developers when running tests. On continuous
     * integration platforms, where inspection is harder, these logfiles are
     * emitted onto stdout.
     */
    string mLogFile;

    /**
     * QFS builds support two types of builds: debug and release. Each build
     * type enables different compiler flags, e.g. for optimization. The debug
     * and release binaries are also stored in different locations. The
     * mBuildType variable holds the build type to help us reconstruct the path
     * to various different binaries depending on the build type we are using.
     */
    string mBuildType;

    /**
     * mPid tracks the pid of the service simulated by this instance of
     * QFSTestEnvironment. This pid is used to control the external service,
     * e.g. sending a SIGTERM to it when it's time to end the service.
     */
    pid_t mPid;
};

/**
 * The QFSTest class is the base class for all tests in the QFS test suite which
 * wish to access the metaserver or chunkservers that the test suite starts up.
 * It also provides basic setup and teardown for each test so that they run in
 * isolation.
 */
class QFSTest : public ::testing::Test
{
public:
    /**
     * Init initializes the qfs test class. This function needs to only be
     * called once during a test run.
     *
     * @param numChunkservers The number of chunkservers to simulate
     */
    static void Init(int numChunkservers);

    /**
     * Destroy is responsible for bringing down all currently running
     * environments. This function usually doesn't need to be called. The Google
     * Test framework should bring down all environments cleanly at test end.
     * However, should the test program not exit cleanly, this function can be
     * used to make sure that all spawned processes from the test program are
     * properly killed.
     */
    static void Destroy();

    /**
     * SetUp handles setting up the individual test before the actual code for a
     * given test is run. It is called directly by the Google Test framework and
     * casing (SetUp) is very important.
     */
    virtual void SetUp();

    /**
     * TearDown handles tearing down the individual test after the actual code
     * for a given test is run. It is called directly by the Google Test
     * framework and casing (SetUp) is very important.
     */
    virtual void TearDown();

protected:
    /**
     * The MetaserverEnvironment and ChunkserverEnvironment pointers here are
     * given to Google Test as global environments. They are simply stored here
     * so that tests can access them easily.
     */
    static MetaserverEnvironment* sMetaserver;
    static vector<ChunkserverEnvironment*> sChunkservers;

    friend int ::main(int, char**);
};

class QFSTestUtils
{
public:
    static const string kTestHome;

    /**
     * Creates a temporary file in the filesystem to be used somehow by the
     * caller.
     *
     * @param path Output parameter set to the path of the new temporary file.
     * @return The file descriptor number that can be used to write to the
     * temporary file. The caller must close the file descriptor once they are
     * finished writing.
     */
    static int CreateTempFile(string* path);

    /**
     * Writes a string to a new temporary file.
     *
     * @param data The string to write to a new file.
     * @return The path of the temporary file.
     */
    static string WriteTempFile(const string& data);

    /**
     * Check if a file exists at the given path. This function does not check if
     * the file is a regular file. See {@link IsFile}.
     *
     * @param path The path to check if a file exists
     * @param s Optional output parameter to get the stat value of the file
     * @return True if a file exists at the given path
     */
    static bool FileExists(const string& path, struct stat* s = NULL);

    /**
     * Check if a file exists at the given path and that it is a regular file.
     *
     * @param path The path to check if a file exists
     * @return True if a file exists at the given path and it is a regular file
     */
    static bool IsFile(const string& path);

    /**
     * Check if a directory exists at the given path.
     *
     * @param path The path to check if a directory exists
     * @return True if a directory exists at the given path
     */
    static inline bool IsDirectory(const string& path);

    /**
     * Generates a random port, checking it can be bound to
     */
    static uint16_t GetRandomPort(int type, const string& ip);

    /**
     * Create a new temporary directory.
     *
     * @return The full path of the directory
     */
    static string CreateTempDirectory();

    /**
     * Remove a file or directory forecefully, e.g. calling rm -rf on it.
     *
     * @param path The path to remove
     */
    static void RemoveForcefully(const string& path);

    /**
     * Given a pid, the process name of that pid. This function looks at the
     * /proc filesystem to determine the process name of a given pid.
     *
     * @param pid The process id
     * @return The name of the process. Returns string "unknown" if no such
     * process exists.
     */
    static string GetProcessName(pid_t pid);

    /*
     * Send a SIGTERM to a process and wait for it to exit. Check that it exits
     * cleanly and with the expected exit code.
     *
     * @param pid The process id
     * @param exitCode The expected exit code that the process should end with
     */
    static void TermProcess(pid_t pid, int exitCode);

    /*
     * Sends a signal to a pid.
     *
     * @param pid The process id
     * @param signum The signal to send to the process
     */
    static void SendSignal(pid_t pid, int signum);

    /*
     * Wait for process to end on its own (possibly after something else has
     * sent it a signal to finish or a SIGTERM). Check that it exits cleanly and
     * with the expected exit code.
     *
     * @param pid The process id
     * @param exitCode The expected exit code that the process should end with
     */
    static void WaitForProcess(pid_t pid, int exitCode);

    /**
     * Check whether a particular process is alive or not.
     *
     * @return True if the process is alive
     */
    static bool IsProcessAlive(pid_t pid);
};

} // namespace Test
} // namespace KFS

#endif
