#!/usr/bin/env python
#
# $Id: write-flush.py 182 2008-10-04 00:11:28Z sriramsrao $
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
# This is a unittest for writes that involve a flush: for instance, a
#sequence of writes of the form---write, flush, write, flush; after
#the 2nd flush, data on the server should match what was written.
#

import kfs
import sys
import os, os.path
import mmap, stat
from stat import *
import ConfigParser
import unittest
import time

TESTNAME = "write-flush"

default_test_params = {
	"kfs_properties": "conf/KfsClient.prp", # KFS property file
	"log_file": TESTNAME + ".log",	# log of operations
	"max_file_size": "100000",	# maximum file size
	"source_file": "/usr/share/dict/words"} # source for writes

default_config_file = TESTNAME + ".cfg"	# test configuration file
param_section = "Test parameters"	# section heading in file
config = ConfigParser.ConfigParser(default_test_params)
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

def start_client(props):
    """ Create an instance of the KFS client.
    The KFS meta/chunkservers must already be running."""
    try:
        global kfsClient
        kfsClient = kfs.client(props)
    except:
        print "Unable to start the KFS client."
        sys.exit(1)

class WriteFlushTestCase(unittest.TestCase):
    def setUp(self):
        global kfsClient
        self.kfsClient = kfsClient
        # make the directory hierarchy
        if not self.kfsClient.isdir(TESTNAME):
            res = self.kfsClient.mkdir(TESTNAME)
        # setup the mmap'ed file
        fd = os.open(get_param_string('source_file'), os.O_RDONLY)
        size = os.fstat(fd)[stat.ST_SIZE]
        self.datamap = mmap.mmap(fd, size, mmap.MAP_SHARED, mmap.PROT_READ)

    def tearDown(self):
        # want a scrub done
        #if self.kfsClient.isdir(TESTNAME):
        # self.kfsClient.rmdirs(TESTNAME)
        pass

    def testSmallWrite(self):
        """Testing write, flush, write, close, read sequence for a small file"""
        now = time.asctime()
        filename = TESTNAME + "/file.2." + "_".join(now.split()) + ".tmp"
        self.file = self.kfsClient.create(filename)
        assert self.file is not None
        part1 = self.datamap.read(139)
        res = self.file.write(part1)
        assert res is None
        self.file.sync()

        filename2 = TESTNAME + "/file.3." + "_".join(now.split()) + ".tmp"
        self.file2 = self.kfsClient.create(filename2)
        assert self.file2 is not None
        part3 = self.datamap.read(128)
        res = self.file2.write(part3)
        assert res is None
        self.file2.sync()

	self.kfsClient.log_level("DEBUG")
        
        part2 = self.datamap.read(22)
        res = self.file.write(part2)        
        self.file.close()
        self.file2.close()
        msg = part1 + part2
        self.file = self.kfsClient.open(filename, 'r')
        res = self.file.data_verify(msg)
        assert res == 1
        data1 = self.file.read(139)
        assert len(data1) == 139
        data2 = self.file.read(22)
        assert len(data2) == 22
        data = data1 + data2
        assert msg == data
	self.kfsClient.log_level("INFO")
        
    def testFlush(self):
        """Testing write, flush, write, flush, close sequence"""
        now = time.asctime()
        filename = TESTNAME + "/file.1." + "_".join(now.split()) + ".tmp"
        self.file = self.kfsClient.create(filename)
        assert self.file is not None
        part1 = self.datamap.read(30)
        res = self.file.write(part1)
        assert res is None
        self.file.sync()
        # verify checksums
        res = self.file.data_verify(part1)
        assert res == 1
        # write about 25k:
        part2 = self.datamap.read(23000)
        res = self.file.write(part2)
        assert res is None
        self.file.sync()
        # verify checksums
        msg = part1 + part2
        res = self.file.data_verify(msg)
        assert res == 1        
        self.file.close()
        # sleep for 5 m
        statInfo = self.kfsClient.stat(filename)
        assert statInfo[ST_SIZE] == len(msg)
        self.file = self.kfsClient.open(filename, 'r')
        res = self.file.data_verify(msg)
        assert res == 1
        # read the data and verify it back
        data = self.file.read(statInfo[ST_SIZE])
        assert msg == data
        self.file.close()

if __name__ == '__main__':
    setup_params(None)
    client = start_client(get_param_string('kfs_properties'))
    unittest.main()
