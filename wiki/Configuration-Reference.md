# Configuration Reference

## Client Tool

The following parameters may be included in the configuration file passed to the
QFS client tool (e.g. `bin/tools/qfs`).

| parameter                | default                    | description                                                                                                        |
| ------------------------ | -------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| fs.msgLogWriter.logLevel | INFO                       | trace log level, one of the following: DEBUG, INFO, NOTICE, ERROR, CRIT, ALERT, FATAL                              |
| fs.trash.minPathDepth    | 5                          | file or directories that path depth less than this value will not be moved into trash unless dfs.force.remove=true |
| fs.trash.current         | Current                    | the name of the current trash checkpoint directory                                                                 |
| fs.trash.homesPrefix     | /user                      | home directories prefix                                                                                            |
| fs.trash.interval        | 60                         | interval in seconds between emptier runs                                                                           |
| fs.trash.trash           | .Trash                     | the name of the trash directory                                                                                    |
| dfs.force.remove         | false                      | see fs.trash.minPathDepth                                                                                          |
| fs.euser                 | process effective id       | Numeric effective user id                                                                                          |
| fs.egroup                | process effective group id | Numeric effective group id                                                                                         |

## Metaserver

[Metaserver annotated configuration file](https://github.com/quantcast/qfs/blob/master/conf/MetaServer.prp).
The configuration file includes a description and a default value for each
metaserver configuration parameter.

Metaserver configuration file is organized into two sections of parameters. The
parameters in the first section are static in the sense that they can not be
modified at run time but require a metaserver restart for the changes to take
effect. On the other hand, the parameters in the second section can be changed
and start to take effect at run time by editing the configuration file and
sending the metaserver process a SIGHUP signal. Please look for the line in the
metaserver configuration file that starts with "The parameters below this line"
to be able to tell the different sections.

**NOTE:** In order to restore a parameter to its default at run time, the
default value must be explicitly set in the configuration file.  In other
words, commenting out the parameter will not have any effect until a
restart.

## Chunk Server

[Chunk server annotated configuration](https://github.com/quantcast/qfs/blob/master/conf/ChunkServer.prp).
The configuration file includes a description and a default value for each
chunk server configuration parameter.

Chunk server parameters are categorized into two. The first set of parameters
are static in the sense that a chunk server restart is required for any changes
on them to take effect. These parameters are defined in chunk server
configuration file only. The second set of parameters are the ones which can be
changed dynamically at run time. These parameters can be defined both in chunk
server and metaserver configuration file. Configuration parameters in the
metaserver configuration file take precedence over the parameters defined in
chunk server configuration files, i.e. the values defined in chunk server
configuration file are overridden dynamically by the metaserver during the
initial handshake. Please look for the line in metaserver configuration file
that reads "Chunk servers configuration parameters." to see the list of these
parameters.

## Web Monitor

[Web reporting interface annotated configuration file](https://github.com/quantcast/qfs/blob/master/webui/server.conf).

**NOTE:** One should not have to modify the settings in the `[chunk]` section of the web
server configuration file.

![Quantcast](//pixel.quantserve.com/pixel/p-9fYuixa7g_Hm2.gif?labels=opensource.qfs.wiki)
