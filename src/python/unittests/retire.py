#!/usr/bin/env python
#
# $Id: failover.py $
#
# Created 2008/10/06
#
# Copyright 2008 Quantcast Corporation.
#
# Author: Sriram Rao (Quantcast Corp.)
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
# This is a unittest for node retirement:
# 1. A node is taken down with a promise it will be back in N minutes;
#the node is brought back within the promised time.  This should not
#incur any re-replications.
# 2. A node is taken down with a promise it will be back in N minutes;
#the node is not brought back within the promised time.
#Re-replications should occur
# 3. A node is retired.  This should cause re-replications

import kfs
import sys
import os, os.path, getopt, time
import ConfigParser
import unittest
import time
from stat import *

from KfsTestLib import *
from constants import *

TESTNAME = "retire"
kfsClient = None

def constructBlockLocationSet(blockLocations):
    """Given a list of block locations, construct the set of servers on which the blocks are hosted"""
    beforeSet = set()
    for i in xrange(len(blockLocations)):
        block = blockLocations[i]
        for j in xrange(len(block)):
            beforeSet.add(block[j])
    return beforeSet

class RetireTestCase(unittest.TestCase):
    def setUp(self):
        global kfsClient
        if kfsClient is None:
            kfsClient = kfs.client(KFS_CLNT_PRP_FILE)
        self.kfsClient = kfsClient
        if not self.kfsClient.isdir(TESTNAME):
            self.kfsClient.mkdir(TESTNAME)
        self.testfile = os.path.join(TESTNAME, TEST_FILE_NAME)
        self.srcfile = TEST_DATA_DIR + TEST_FILE_NAME
        if not self.kfsClient.isfile(self.testfile):
            # Copy the file
            constrainNumChunkServers(3)
            print "Copying the file for testing"
            writer = IOWorker(self.kfsClient, self.testfile, srcfile, -1, Write)
            writer.start()
            writer.join()

    def tearDown(self):
        pass


    def testRetireRestart(self):
        '''Retire a chunkserver with promise it will be back'''
        statInfo = self.kfsClient.stat(self.testfile)
        kfsFp = self.kfsClient.open(self.testfile, "r+")
        # startup a server for kicks
        restartChunkServer(4)
        blockLocationsBefore = constructBlockLocationSet(kfsFp.chunk_locations(0, statInfo[ST_SIZE]))
        retireChunkServer(2, 180, True)
        blockLocationsAfter = constructBlockLocationSet(kfsFp.chunk_locations(0, statInfo[ST_SIZE]))
        assert blockLocationsBefore == blockLocationsAfter

    def testRetireNoRestart(self):
        '''Retire a chunkserver with promise it will be back but it fails to come back'''
        statInfo = self.kfsClient.stat(self.testfile)
        kfsFp = self.kfsClient.open(self.testfile, "r+")
        # startup a server so that we can re-replicate there
        restartChunkServer(4)
        blockLocationsBefore = constructBlockLocationSet(kfsFp.chunk_locations(0, statInfo[ST_SIZE]))
        retireChunkServer(2, 60, False)
        time.sleep(15)
        blockLocationsAfter = constructBlockLocationSet(kfsFp.chunk_locations(0, statInfo[ST_SIZE]))
        assert blockLocationsAfter.issubset(blockLocationsBefore)
        time.sleep(180)
        blockLocationsAfter = constructBlockLocationSet(kfsFp.chunk_locations(0, statInfo[ST_SIZE]))
        # should have a copy in a new place
        if CLUSTER_MODE != "local":
            newserverSet = set()
            newserverSet.add(getChunkServerIp(4))
            assert blockLocationsAfter.difference(blockLocations.before) == newserver

    def testRetire(self):
        """Retire a chunkserver; it'll go down after it helps in replicating blocks"""
        statInfo = self.kfsClient.stat(self.testfile)
        kfsFp = self.kfsClient.open(self.testfile, "r+")
        # startup a server so that we can re-replicate there
        restartChunkServer(4)
        blockLocationsBefore = kfsFp.chunk_locations(0, statInfo[ST_SIZE])
        print "before: ", blockLocationsBefore
        retireChunkServer(2, -1, False)
        time.sleep(180)
        blockLocationsAfter = constructBlockLocationSet(kfsFp.chunk_locations(0, statInfo[ST_SIZE]))
        # should have a copy in a new place
        if CLUSTER_MODE != "local":
            newserverSet = set()
            newserverSet.add(getChunkServerIp(4))
            assert blockLocationsAfter.difference(blockLocations.before) == newserver
        # verify that the server did indeed retire
        dataVerify(self.kfsClient, self.testfile, self.srcfile)

if __name__ == '__main__':
    loadSetup()
    startAllServers()
    suite = unittest.TestSuite()
    suite.addTest(RetireTestCase("testRetireRestart"))
    # suite = unittest.TestLoader().loadTestsFromTestCase(RetireTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
