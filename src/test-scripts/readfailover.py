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
# script that tests read failover.  we start reading data from one
#server; that is killed while the read is going on; the reader should
#failover to another server
# 

import os,os.path,sys,getopt,time
import random
from KfsTestLib import *

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage: %s <test binaries dir> <config fn> <kfs clnt prp fn>" % sys.argv[0]
        sys.exit(-1)

    testBinPath = sys.argv[1]
    configFn = sys.argv[2]
    kfsClntPrpFn = sys.argv[3]
    numMB = 512
    if not os.path.exists(configFn):
        print "%s : directory doesn't exist\n" % configFn
        sys.exit(-1)

    loadSetup(configFn)
    setTestBinDir(testBinPath)

    print "Formatting servers..."
    formatAllServers()
    startMetaServer()
    startServers(3)
    time.sleep(10)
    # Write out a decent amount of data
    print "Doing some writes..."
    workers = []
    for i in xrange(5):
        fn = "/foo.%d" % (i)
        w = IOWorker(kfsClntPrpFn, fn, numMB, doWrite)
        workers.append(w)

    for i in xrange(len(workers)):
        workers[i].start()

    for i in xrange(len(workers)):
        workers[i].join()

    print "Writes are done...doing reads"

    workers = []
    for i in xrange(5):
        fn = "/foo.%d" % (i)
        w = IOWorker(kfsClntPrpFn, fn, numMB, doRead)
        workers.append(w)
    
    for i in xrange(len(workers)):
        workers[i].start()

    print "Restarting servers..."
    # in a loop start/stop servers at will
    for round in xrange(10):
        i = random.randint(0, 3)
        restartChunkServer(i)
        time.sleep(20)

    print "Waiting for readers to join..."
    for i in xrange(len(workers)):
        # wait for 2 mins to join; otherwise, kill it
        workers[i].join(120.0)
        if workers[i].isAlive():
            print "Worker %d is still alive...will kill it" % i

    print "All done..."
    stopAllServers()
    sys.exit(0)
    

    
