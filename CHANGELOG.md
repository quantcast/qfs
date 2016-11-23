# Changes

* [QFS-332](https://quantcast.atlassian.net/browse/QFS-332) make QuantcastFileSystem honor
umask setting from jobconf for better Hadoop/Hive compatibility. See [#196](https://github.com/quantcast/qfs/pull/196)

* [QFS-325](https://quantcast.atlassian.net/browse/QFS-325) throw exception if native code
load fails instead of calling exit. See [#189](https://github.com/quantcast/qfs/pull/189)

* [QFS-321](https://quantcast.atlassian.net/browse/QFS-321) add s3 option to
sample setup script. See [#177](https://github.com/quantcast/qfs/pull/177)

* [QFS-320](https://quantcast.atlassian.net/browse/QFS-320) pass an argument
to qfstest.sh within the Makefile. See [#175](https://github.com/quantcast/qfs/pull/175)

* [QFS-187](https://quantcast.atlassian.net/browse/QFS-187) make chunkserver error
message display all possibilities upon receiving EBADCLUSTERKEY. See [#169](https://github.com/quantcast/qfs/pull/169)

* [QFS-171](https://quantcast.atlassian.net/browse/QFS-171) display better error
messages in cptoqfs when file configuration parameters are invalid. See [#170](https://github.com/quantcast/qfs/pull/170)

* [QFS-289](https://quantcast.atlassian.net/browse/QFS-289) display more detail
in qfsfileenum help menu. See [#167](https://github.com/quantcast/qfs/pull/167)

* [QFS-53](https://quantcast.atlassian.net/browse/QFS-53) display the default
file configuration that qfs tool uses file copying. See [#164](https://github.com/quantcast/qfs/pull/164)

* [QFS-263](https://quantcast.atlassian.net/browse/QFS-263) binary distributions
for mac os. See [#165](https://github.com/quantcast/qfs/pull/165)

* [QFS-78](https://quantcast.atlassian.net/browse/QFS-78) delete obsolete test
files. See [#166](https://github.com/quantcast/qfs/pull/166)

* make the read call control the actual read size in KfsInputChannel,
if read ahead logic is turned off. See [#156](https://github.com/quantcast/qfs/pull/156)

* add API support to pass create parameters for new files.
See [#154](https://github.com/quantcast/qfs/pull/154)

* [QFS-293](https://quantcast.atlassian.net/browse/QFS-293) wiki/Binary-Distributions.md:
update links to different versions of the binary distributions. See [#148](https://github.com/quantcast/qfs/pull/148)

* buildsystem: the build system now correctly assigns the hash, tag, or version
when gathering build information. See [#129](https://github.com/quantcast/qfs/pull/129)

* hadoop shim: if a QFS instance fails initialization, give more info and throw an IOException rather
than doing an unfriendly exit. See [here](https://github.com/quantcast/qfs/commit/e5453202ec0f663c974b70a1b3f8515e1cd0b320)

* support to change io buffer size and read ahead size of a file through hadoop-qfs.
See [here](https://github.com/quantcast/qfs/commit/27b8a384f011040a796edc05c7646865bb744b88)

* configurable disk IO size for writes. See [here](https://github.com/quantcast/qfs/commit/7d411180b733de2cb36bda4d837b5bf37e020314)
