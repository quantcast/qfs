#ifndef TESTS_ENVIRONMENTS_CHUNKSERVER_ENVIRONMENT
#define TESTS_ENVIRONMENTS_CHUNKSERVER_ENVIRONMENT

#include <gtest/gtest.h>

#include "tests/integtest.h"

namespace KFS {
namespace Test {

class MetaserverEnvironment;

/**
 * ChunkserverEnvironment is responsible for managing a single chunkserver's
 * lifecycle during unit tests.
 */
class ChunkserverEnvironment : public QFSTestEnvironment
{
public:
    /**
     * Instantiate a new chunkserver environment.
     *
     * @param metaserver The metaserver this chunkserver should connect to
     */
    ChunkserverEnvironment(const MetaserverEnvironment* metaserver);

    /**
     * @return the port that the chunkserver is listening on for client
     * connections
     */
    uint16_t GetClientPort() const { return mClientPort; }

protected:
    /**
     * GenerateConfig creates a configuration suitable for the chunkserver to
     * start up with. It selects random ports for the chunkserver to bind on and
     * creates the filesystem environment necessary for the chunkserver to
     * operate in, e.g. creates a directory for the chunkserver to store chunks.
     *
     * @return The path to the configuration file
     */
    string GenerateConfig();

    /**
     * Start a new chunkserver. If the chunkserver is already running, this
     * function is a no-op.
     *
     * @return True if the chunkserver was successfully started
     */
    virtual bool Start();

private:
    uint16_t mMetaserverPort;
    uint16_t mClientPort;
    string mChunkDir;
};

} // namespace Test
} // namespace KFS

#endif
