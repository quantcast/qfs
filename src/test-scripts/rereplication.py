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
# script that tests re-replication: we create a file with two servers;
#startup a third one; we wait a bit and check that the data makes to
#the third one in reasonable time.
# 


import os,os.path,sys,getopt,time
from KfsTestLib import *

def checkReReplication(serverToCheck, expectEntries):
    """Check a server to see if has  a set of chunks"""
    print "Checking re-replication..."
    for round in range(5):
        print "Round: %d" % round
        dirListNew = serverToCheck.ListChunks()
        if (len(expectEntries) == len(dirListNew)):
            print "dirlists are same...re-replication worked"
            return True
        time.sleep(60)

    print "dirlists are different...re-replication didn't work"
    return False
        
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
    startMetaServer()
    startServers(2)
    time.sleep(10)
    doWrite(kfsClntPrpFn, "/foo.1", 128)
    dirList = chunkServers[0].ListChunks()
    startServers(3)
    if (checkReReplication(chunkServers[2], dirList) == False):
        print "Part I of Test failed!"
        stopAllServers()        
        sys.exit(-1)
    print "Part 1 of re-replication passed..."
    doWrite(kfsClntPrpFn, "/foo.2", 128)
    chunkServers[2].Stop()            
    chunkServers[3].Start()
    # whatever is on 2 should show up on 3
    dirList = chunkServers[2].ListChunks()    
    if (checkReReplication(chunkServers[3], dirList) == False):
        print "Part II of Test failed!"
        stopAllServers()        
        sys.exit(-1)
    print "Part 2 of re-replication passed..."
    chunkServers[2].Start()
    # Verify that the # of copies goes down by 1
    sys.exit(0)

    


