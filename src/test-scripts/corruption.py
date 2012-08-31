#!/usr/bin/env python
#
# $Id$
#
# Created 2006
# Author: Sriram Rao (Kosmix Corp)
#
# Copyright 2006 Kosmix Corp.
#
# This file is part of Kosmos File System (KFS).
#
# Licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
#
# script that tests data corruption: we write to a file; corrupt it;
# try to read the data from the corrupted node and it should fail
# 

import os,os.path,sys,getopt,time
from KfsTestLib import *

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage: %s <test binaries dir> <config fn> <kfs clnt prp fn>" % sys.argv[0]
        sys.exit(-1)

    testBinPath = sys.argv[1]    
    configFn = sys.argv[2]
    kfsClntPrpFn = sys.argv[3]
    if not os.path.exists(configFn):
        print "%s : directory doesn't exist\n" % configFn
        sys.exit(-1)

    loadSetup(configFn)
    setTestBinDir(testBinPath)
    
    print "Formatting servers..."
    formatAllServers()

    print "Starting servers.."
    startMetaServer()
    # start 2 of them
    startServers(2)
    # wait for things to get going
    time.sleep(30)
    doWrite(kfsClntPrpFn, "/foo.1", 128)
    # we are checking re-replication; so start the third server and
    # give it at most 10 mins; by then re-replication should've pushed
    # the data blocks to the third node
    # startAllServers()
    stopAllChunkServers()
    chunks = chunkServers[0].ListChunks()
    print "Corrupting chunk %s on chunkserver %s" % (chunks[0], chunkServers[0])
    chunkServers[0].CorruptChunk(int(chunks[0]))
    # will fail
    restartChunkServer(0)
    # wait for it startup and get ready..
    time.sleep(30)
    doRead(kfsClntPrpFn, "/foo.1", 128)
    # verify that the corrupted chunk is in the lost+found directory
    chunkServers[0].CheckLostFound(chunks[0])
    # should succeed
    restartChunkServer(1)
    time.sleep(30)
    doRead(kfsClntPrpFn, "/foo.1", 128)
    stopAllServers()
