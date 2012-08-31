#!/usr/bin/env python
#
# $Id: write.py 182 2008-10-04 00:11:28Z sriramsrao $
#
# Created 2008/07/28
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
# This is a unittest for write block placement: when picking 3
#replicas for a chunk, the write block allocator should put one of the
#copies on the same node as the client doing the write (assuming a
#chunkserver is running on that node).
#

import kfs
import sys
import os, os.path, socket
import mmap
from stat import *
import ConfigParser
import unittest
import time
from stat import *

from KfsTestLib import *
from constants import *

TESTNAME = "write-placement"

default_test_params = {
	"kfs_properties": "conf/KfsClient.prp", # KFS property file
	"log_file": TESTNAME + ".log",	# log of operations
	"test_files": "20",		# no. of files to use
	"max_file_size": "100000",	# maximum file size
	"max_rw_size": "4096",		# max. read or write length
	"source_file": "/usr/share/dict/words"} # source for writes

default_config_file = TESTNAME + ".cfg"	# test configuration file
param_section = "Test parameters"	# section heading in file
config = ConfigParser.ConfigParser(default_test_params)
#config = ConfigParser.ConfigParser()
kfsClient = None

def setup_params(config_file):
    """Read in the configuration file"""
    if config_file:
        config.read(config_file)
    else:
        config.add_section(param_section)

def get_param_string(name):
    """Look up a string parameter"""
    return config.get(param_section, name)

def specialDirEntry(entry):
    return entry == '.' or entry == '..'

def start_client(props):
    """ Create an instance of the KFS client.
    The KFS meta/chunkservers must already be running."""
    try:
        global kfsClient
        kfsClient = kfs.client(props)
    except:
        print "Unable to start the KFS client."
        sys.exit(1)

def isLocalHost(hostname):
    s = socket.gethostbyname(socket.gethostname())
    return socket.gethostname() == hostname or s == hostname

    
class WritePlacementTestCase(unittest.TestCase):
    def setUp(self):
        global kfsClient
        self.kfsClient = kfsClient
        # make the directory hierarchy
        if not self.kfsClient.isdir(TESTNAME):
            res = self.kfsClient.mkdir(TESTNAME)

    def tearDown(self):
        if not CLEANUP_IN_TESTCASE:
            return
        if self.kfsClient.isdir(TESTNAME):
            self.kfsClient.rmdirs(TESTNAME)

    def testBlockPlacement(self):
        """Testing block placement"""
        now = time.asctime()
        filename = TESTNAME + "/file.1." + "_".join(now.split()) + ".tmp"
        self.file = self.kfsClient.create(filename)
        assert self.file is not None
        msg = "hello world"
        res = self.file.write(msg)
        assert res is None
        self.file.sync()
        numwrote = 0
        for l in open("/usr/share/dict/words", "r").xreadlines():
            res = self.file.write(l)
            numwrote = numwrote + len(l)
            if numwrote >= 23000:
                break
            assert res is None
        self.file.sync()
        chunklocs = self.file.chunk_locations(0, len(msg))
        for chunkloc in chunklocs:
            numlocal = 0
            for l in chunkloc:
                if isLocalHost(l):
                    numlocal = numlocal + 1
            assert numlocal >= 1

        self.file.close()

    def testAppend(self):
        """Testing append"""
        now = time.asctime()
        filename = TESTNAME + "/file.append.1." + "_".join(now.split()) + ".tmp"
        part1Data = ""
        part2Data = ""
        numWrote = 0
        self.file = self.kfsClient.create(filename)
        for l in open("/usr/share/dict/words", "r").xreadlines():
            res = self.file.write(l)
            numWrote = numWrote + len(l)
            part1Data = part1Data + l
            if numWrote >= 10000:
                part1 = numWrote
                break
        assert self.file.tell() == part1
        self.file.close()
        self.file = self.kfsClient.open(filename, "a")
        assert self.file.tell() == part1
        numWrote = 0
        for l in open("/usr/share/dict/words", "r").xreadlines():
            res = self.file.write(l)
            numWrote = numWrote + len(l)
            part2Data = part2Data + l            
            if numWrote >= 20000:
                part2 = numWrote
                break
        assert self.file.tell() == part1 + part2
        self.file.close()
        statInfo = self.kfsClient.stat(filename)
        assert statInfo[ST_SIZE] == part1 + part2
        # read the thing back
        self.file = self.kfsClient.open(filename, "r+")
        assert self.file.read(part1) == part1Data
        assert self.file.read(part2) == part2Data
        self.file.close()

    def testLargeFile(self):
        """Testing read/write of a file that spans multiple chunks"""
        now = time.asctime()
        filename = TESTNAME + "/file.large." + "_".join(now.split())
        srcfile = TEST_DATA_DIR + TEST_FILE_NAME
        self.kfsClient.log_level("INFO")
        """Writing data to """, filename
        writer = IOWorker(self.kfsClient, filename, srcfile, -1, Write)
        writer.start()
        writer.join()
        srcStat = os.stat(srcfile)
        assert srcStat[ST_SIZE] == writer.getNumDone()
        """Reading data back to verify"""
        reader = IOWorker(self.kfsClient, filename, srcfile, -1, Read)
        reader.start()
        reader.join()
        assert srcStat[ST_SIZE] == reader.getNumDone()
        assert reader.getMd5() == writer.getMd5()

if __name__ == '__main__':
    setup_params(None)
    client = start_client(get_param_string('kfs_properties'))
    suite = unittest.TestLoader().loadTestsFromTestCase(WritePlacementTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
