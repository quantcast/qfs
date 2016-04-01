#ifndef TESTS_ENVIRONMENTS_METASERVER_ENVIRONMENT
#define TESTS_ENVIRONMENTS_METASERVER_ENVIRONMENT

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "tests/integtest.h"

namespace KFS {
namespace Test {

using namespace std;

/**
 * MetaserverEnvironment is responsible for managing a single metaserver's
 * lifecycle during unit tests.
 */
class MetaserverEnvironment : public QFSTestEnvironment
{
public:
    /**
     * Instantiate a new metaserver environment.
     */
    MetaserverEnvironment();

    /**
     * @return the hostname that this metaserver is listening on
     */
    string GetHostname() const { return "127.0.0.1"; }

    /**
     * @return the port that the metaserver is listening on for client
     * connections
     */
    uint16_t GetClientPort() const { return mClientPort; }

    /**
     * @return the port that the metaserver is listening on for chunkserver
     * connections
     */
    uint16_t GetChunkserverPort() const { return mChunkserverPort; }

protected:
    /**
     * GenerateConfig creates a configuration suitable for the metaserver to
     * start up with. It selects random ports for the metaserver to bind on and
     * creates the filesystem environment necessary for the metaserver to
     * operate in, e.g. creates a directory for the metaserver to store its
     * checkpoints and transaction logs in.
     *
     * @return The path to the configuration file
     */
    string GenerateConfig();

    /**
     * Start a new metaserver. If the metaserver is already running, this
     * function is a no-op.
     *
     * @return True if the metaserver was successfully started
     */
    virtual bool Start();

private:
    uint16_t mClientPort;
    uint16_t mChunkserverPort;
    string mTransactionLogDir;
    string mCheckpointDir;
};

} // namespace Test
} // namespace KFS

#endif
