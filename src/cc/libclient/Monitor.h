#ifndef SRC_CC_LIBCLIENT_MONITOR_H_
#define SRC_CC_LIBCLIENT_MONITOR_H_

namespace KFS
{
namespace client
{
class KfsClientImpl;
}
using client::KfsClientImpl;
struct ServerLocation;

/// Monitor class is responsible for collecting metrics from client
/// instances created in a process and propagating them to a monitor plugin
/// library.
/// There are two sets of metrics collected from each client instance.
///
///    * Set 1: Global counters maintained by a client instance for user
///    ops such as append, read and write along with meta ops.
///    Example: "Read.ChunkServer.BytesReceived=4195848" shows the total number
///    of bytes received from all chunk-servers for read ops.
///    These metrics are collected by invoking GetStats function on a client
///    instance and present a detailed summary on the interaction between the
///    client instance and the corresponding filesystem since the creation
///    of client instance until the time of the metric collection.
///
///    * Set 2: Chunk-server level counters for errors occurred during
///    individual read and write ops. Each counter shows the total number
///    of occurrence of a specific type of error when reading/writing from a
///    particular chunk-server.
///    example: "10.6.62.7:12337_error_total_count=5" shows the total number of
///    errors occurred during read ops on chunk-server 10.6.62.7:12337.
///    Upon its observation, a client instance reports an error incident
///    to Monitor class by invoking static function Monitor::ReportError.
///    Monitor class is responsible for recording the total number of
///    occurrences of each error type for each chunk-server.
///
///    Note that neither metrics in set 1 nor in set 2 are reset between
///    subsequent reports to monitor plugin library, i.e. all metrics are
///    non-decreasing.
///
/// Collection and report of metrics from all client instances in a process
/// is orchestrated by a static Monitor class instance. Static Monitor instance
/// manages a dedicated thread to perform these tasks periodically. Dedicated
/// monitor thread is started when the first client instance is created and
/// it is stopped when all client instances are destroyed. The monitor thread
/// can be restarted if another client instance is created later on by the same
/// process.
///
/// Monitor thread reports metrics for a filesystem to plugin library
/// either if there are still monitored client instances for that filesystem.
/// Otherwise, the most recent snapshot of metrics for that filesystem is kept
/// around, but is never sent to plugin library.
class Monitor {

private:
    class Impl;

public:
    enum
    {
        kReadOpError,
        kWriteOpError
    };
    // tells Monitor to start monitoring a client instance.
    // returns 0 on success, -1 otherwise.
    static bool AddClient(
            KfsClientImpl* client,
            char* pluginPath,
            int reportInterval,
            int maxErrorsToRecord)
    {
        return Instance().AddClientSelf(client, pluginPath,
                reportInterval, maxErrorsToRecord);
    }
    // tells Monitor to stop monitoring a client instance
    static void RemoveClient(
            KfsClientImpl* client)
    {
        Instance().RemoveClientSelf(client);
    }
    // reports a read/write incident (chunk-server and error code) to Monitor
    static void ReportError(
            int errSource,
            const ServerLocation& metaserverLocation,
            const ServerLocation& chunkserverLocation,
            int errCode)
    {
        Instance().ReportErrorSelf(errSource, metaserverLocation,
                chunkserverLocation, errCode);
    }
    static Monitor& Instance()
    {
        static Monitor sInstance;
        return sInstance;
    }
private:
    Monitor();
    ~Monitor();
    bool AddClientSelf(
            KfsClientImpl* client,
            char* pluginPath,
            int reportInterval,
            int maxErrorToRecord);
    void ReportErrorSelf(
            int errSource,
            const ServerLocation&
            metaserverLocation,
            const ServerLocation& chunkserverLocation,
            int errCode);
    void RemoveClientSelf(
            KfsClientImpl* client);
    Impl* mImpl;
};

}
#endif /* SRC_CC_LIBCLIENT_MONITOR_H_ */
