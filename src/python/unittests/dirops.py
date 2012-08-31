#!/usr/bin/env python
#
# $Id: dirops.py 182 2008-10-04 00:11:28Z sriramsrao $
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
# This is a unittest for directory operations:
#  -- create a directory hierarchy; when we read the contents of a
# directory, all the entries in the tree that we created should show
# up.
#

import kfs
import sys
import os, os.path
import mmap
import ConfigParser
import unittest
import time

TESTNAME = "dirops"

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

class DirOpsTestCase(unittest.TestCase):
    def setUp(self):
        global kfsClient
        self.kfsClient = kfsClient
        # make the directory hierarchy
        if not self.kfsClient.isdir(TESTNAME):
            res = self.kfsClient.mkdir(TESTNAME)

    def tearDown(self):
        if self.kfsClient.isdir(TESTNAME):
            self.kfsClient.rmdirs(TESTNAME)

    def testReaddir(self):
        """Testing the readdir API"""
        entries = self.kfsClient.readdir(TESTNAME)
        assert len(entries) == 2
        entries = [e for e in entries if not specialDirEntry(e)]
        assert len(entries) == 0

    def testReaddirPlus(self):
        """Testing the readdirplus API"""        
        entries = self.kfsClient.readdirplus(TESTNAME)
        assert len(entries) == 2
        entries = [e[0] for e in entries if not specialDirEntry(e[0])]
        assert len(entries) == 0

if __name__ == '__main__':
    setup_params(None)
    client = start_client(get_param_string('kfs_properties'))
    unittest.main()
