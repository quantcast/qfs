#!/usr/bin/env python
#
# $Id: constants.py $
#
# Created 2008/10/06
#
# Copyright 2008 Quantcast Corporation.  All rights reserved.
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
# This defines the location of config files and such for the tests
#

""" This file defines the config files and such used for the tests """

#SERVER_CONFIG_FILE = "conf/machines_test.cfg"
SERVER_CONFIG_FILE = "conf/machines-4.cfg"
SERVER_MACHINES_FILE = "conf/machines_test.txt"
TEST_DATA_DIR = "/Users/sriram/kfstestdata/"
KFS_BIN_DIR = "/Users/sriram/code/kosmosfs/build/debug/bin"
KFS_CLNT_PRP_FILE = "conf/KfsClient.prp"
TEST_FILE_NAME = "user-ct-test-collection-01.txt"
# choices are local/distributed/rack
CLUSTER_MODE = "local"
# leave whatever files the testcase produced in the FS; otherwise, we'll nuke
CLEANUP_IN_TESTCASE = True
