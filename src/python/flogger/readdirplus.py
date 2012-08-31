#
# $Id$
#
# Copyright 2006 Kosmix Corp.
#
# Author: Blake Lewis (Kosmix Corp.)
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
# KFS readdirplus definitions
#
"""
A little module that defines offsets and a few simple functions
for accessing KFS readdirplus data, in the spirit of the standard
'stat' module.
"""

RD_NAME = 0
RD_FILEID = 1
RD_MTIME = 2
RD_CTIME = 3
RD_CRTIME = 4
RD_TYPE = 5
RD_SIZE = 6

def rd_isreg(rdtuple):
	return rdtuple[RD_TYPE] == "file"

def rd_isdir(rdtuple):
	return rdtuple[RD_TYPE] == "dir"

def rd_name(rdtuple):
	return rdtuple[RD_NAME]

def rd_size(rdtuple):
	return rdtuple[RD_SIZE]

def rd_id(rdtuple):
	return rdtuple[RD_FILEID]
