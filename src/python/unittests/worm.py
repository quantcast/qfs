#!/usr/bin/env python
#
# $Id: worm.py 182 2008-10-04 00:11:28Z sriramsrao $
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
# This is a unittest for the WORM support:
#  -- files/dirs with .tmp extension can be removed/renamed
#  -- all other files/dirs cannot be removed/renamed
#

import kfs
import sys
import os, os.path
import mmap
import ConfigParser
import unittest
import time

TESTNAME = "worm"
#
# Test parameter handling
#
default_test_params = {
	"kfs_properties": "KfsClient.prp", # KFS property file
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

def start_client(props):
    """ Create an instance of the KFS client.
    The KFS meta/chunkservers must already be running.  The
    metaserver should be configured in WORM mode."""
    try:
        global kfsClient
        kfsClient = kfs.client(props)
    except:
        print "Unable to start the KFS client."
        sys.exit(1)
    
class WORMTestCase(unittest.TestCase):
    def setUp(self):
        print "In setup"        
        global kfsClient
        self.kfsClient = kfsClient

    def tearDown(self):
        print "In teardown"
        pass
    
    def testTmpDirRm(self):
        """Testing rmdir on .tmp dir"""
        now = time.asctime()
        dirname = TESTNAME + ".1." + "_".join(now.split()) + ".tmp"
        res = self.kfsClient.mkdir(dirname)
        assert res is None
        res = self.kfsClient.rmdir(dirname)
        assert res is None

    def testTmpDirRename(self):
        """Testing rename on .tmp dir"""
        now = time.asctime()
        dirname = TESTNAME + ".2." + "_".join(now.split()) + ".tmp"
        res = self.kfsClient.mkdir(dirname)
        assert res is None
        res = self.kfsClient.rename(dirname, os.path.splitext(dirname)[0])
        assert res is None

    def testTmpFileRm(self):
        """Testing rm on .tmp file"""
        now = time.asctime()
        dirname = TESTNAME + ".3." + "_".join(now.split()) + ".tmp"
        res = self.kfsClient.mkdir(dirname)
        assert res is None
        fname = dirname + "/" + TESTNAME + ".tmp"
        self.file = self.kfsClient.create(fname)
        res = self.kfsClient.remove(fname)
        assert res is None
        res = self.kfsClient.rmdir(dirname)
        assert res is None

    def testTmpFileRename(self):
        """Testing rename on .tmp file"""
        now = time.asctime()
        dirname = TESTNAME + ".4." + "_".join(now.split()) + ".tmp"
        res = self.kfsClient.mkdir(dirname)
        assert res is None
        fname = dirname + "/" + TESTNAME + ".tmp"
        targetname = dirname + "/" + TESTNAME
        self.file = self.kfsClient.create(fname)
        res = self.kfsClient.rename(fname, targetname)
        assert res is None
        assert self.kfsClient.isfile(targetname) == True
        try:
            self.kfsClient.rename(targetname, fname)
        except IOError:
            pass
        else:
            self.fail("Bad: Rename non-temp -> tmp succeeded!")
        # res = self.kfsClient.rmdirs(dirname)
        # assert res is None

    def testFileRm(self):
        """Testing rm on non .tmp files; should fail"""
        now = time.asctime()
        dirname = TESTNAME + ".5." + "_".join(now.split()) + ".tmp"
        res = self.kfsClient.mkdir(dirname)
        assert res is None
        fname = dirname + "/" + TESTNAME
        self.file = self.kfsClient.create(fname + '.tmp')
        res = self.kfsClient.rename(fname + '.tmp', fname)
        assert res is None
        try:
            self.kfsClient.remove(fname)
        except IOError:
            pass
        else:
            self.fail("Remove didn't raise error")
        #self.kfsClient.rmdirs(dirname)

    def testFileRename(self):
        """Testing rename of .tmp file on existing file and vice-versa: should fail"""
        now = time.asctime()
        dirname = TESTNAME + ".6." + "_".join(now.split()) + ".tmp"
        res = self.kfsClient.mkdir(dirname)
        assert res is None
        fname = dirname + "/" + TESTNAME + ".tmp"
        self.file = self.kfsClient.create(fname)
        res = self.kfsClient.rename(fname, os.path.splitext(fname)[0])
        assert res is None
        existingFname = os.path.splitext(fname)[0]
        fname = dirname + "/" + TESTNAME + "2.tmp"
        self.file = self.kfsClient.create(fname)
        try:
            self.kfsClient.rename(fname, existingFname)
        except IOError:
            pass
        else:
            self.fail("Rename tmp->existing didn't raise error")

        try:
            self.kfsClient.rename(existingFname, fname)
        except IOError:
            pass
        else:
            self.fail("Rename existing->tmp didn't raise error")

        #self.kfsClient.rmdirs(dirname)

def suite():
    print "In suite..."
    suite = unittest.makeSuite(WORMTestCase, 'test')

if __name__ == '__main__':
    setup_params(None)
    client = start_client(get_param_string('kfs_properties'))
    unittest.main()


