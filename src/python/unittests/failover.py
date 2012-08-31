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
# This is a unittest for failover:
#  -- during writes, when one of the nodes in the chain fails, the
# write should still succeed at the other nodes (with a version #
# change).
#  -- during reads, when the node the client is reading from fail, the
# read should failover to another node
#

import kfs
import sys
import os, os.path, getopt, time
import ConfigParser
import unittest
import time
from stat import *

from KfsTestLib import *
from constants import *

TESTNAME = "failover"
kfsClient = None

class FailoverTestCase(unittest.TestCase):
    def setUp(self):
        global kfsClient
        if kfsClient is None:
            kfsClient = kfs.client(KFS_CLNT_PRP_FILE)
        self.kfsClient = kfsClient
        if not self.kfsClient.isdir(TESTNAME):
            self.kfsClient.mkdir(TESTNAME)

    def tearDown(self):
        pass

    def testWrite(self):
        """Test handling of failures during write: one or more of the
        chunkservers are killed when a write of a file is on.  The
        client will detect the failure and re-do the writes as
        appropriate."""
        now = time.asctime()
        testfile = TESTNAME + "/file.1." + "_".join(now.split())
        srcfile = TEST_DATA_DIR + TEST_FILE_NAME
        writer = IOWorker(self.kfsClient, testfile, srcfile, -1, Write)
        writer.start()
        sleepTime = 3
        print "Writer is started...waiting for %d seconds" % sleepTime
        time.sleep(sleepTime)
        print "Stopping a chunkserver"
        stopChunkServer(1)
        stopChunkServer(2)
        print "Waiting for writer to finish"
        writer.join()
        print "# of bytes: ", writer.getNumDone()
        # do a data verify

    def testRead(self):
        """Test read failover"""
	if 1:
		return
        testfile = TESTNAME + TEST_FILE_NAME
        srcfile = TEST_DATA_DIR + TEST_FILE_NAME
        if not self.kfsClient.isfile(testfile):
            # Copy the file
            print "Copying the file for reading"
            writer = IOWorker(self.kfsClient, testfile, srcfile, -1, Write)
            writer.start()
            writer.join()
        filestat = kfsClient.stat(testfile)
        reader = IOWorker(self.kfsClient, testfile, srcfile, -1, Read)
        reader.start()
        sleepTime = 0.1
        print "Reader is started...waiting for %f seconds" % sleepTime
        time.sleep(sleepTime)
        print "Restarting all chunkservers"
        restartChunkServer(0)
        restartChunkServer(1)
        restartChunkServer(2)
        print "Waiting for reader to finish"
        reader.join()
        assert filestat[ST_SIZE] == reader.getNumDone()

if __name__ == '__main__':
    loadSetup()
    startAllServers()
    unittest.main()
