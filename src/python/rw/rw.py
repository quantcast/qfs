#!/usr/bin/env python
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
# Random KFS reads and writes, with verification of the reads,
# for test purposes.
#
# The test procedure is as follows:
#
# Create a test directory (rw.<time>) and a specified number of files
# within it.  Choose a moderately large (non-KFS) file to supply text
# for the writes (default: /usr/share/dict/words).  Randomly alternate
# between reads and writes, weighting the choice so that we initially
# do all writes and eventually change to all reads when a sufficient
# amount of writing has been done.  For each write, choose a random
# offset with the source file, a random offset within the target,
# and a random amount.  Keep track of these parameters (and make sure
# to adjust them if a subsequent write overlaps).  For reading,
# choose a previously written interval and compare it with the source
# file.
#
import kfs
import sys
import os
import mmap
import time
import random
import ConfigParser
import re
from stat import *

TESTNAME = "rw"

#
# Test parameter handling
#
default_test_params = {
	"kfs_properties": "KfsClient.prp", # KFS property file
	"log_file": TESTNAME + ".log",	# log of operations
	"read_count": "1000",		# no. or reads to perform
	"write_count": "500",		# no. of writes to perform
	"test_files": "20",		# no. of files to use
	"max_file_size": "100000",	# maximum file size
	"max_rw_size": "4096",		# max. read or write length
	"source_file": "/usr/share/dict/words"} # source for writes

default_config_file = TESTNAME + ".cfg"	# test configuration file
param_section = "Test parameters"	# section heading in file
config = ConfigParser.ConfigParser(default_test_params)

def setup_params(config_file):
	"""Read in the configuration file"""
	if config_file:
		config.read(config_file)
	else:
		config.add_section(param_section)

def get_param_int(name):
	"""Look up an integer parameter"""
	return config.getint(param_section, name)

def get_optional_param_int(name):
	"""Look up a parameter; return -1 if undefined"""
	if config.has_option(param_section, name):
		return get_param_int(name)
	else:
		return -1

def get_param_string(name):
	"""Look up a string parameter"""
	return config.get(param_section, name)

#
# Initialize KFS client
#
def start_client(props):
	"""Create an instance of the KFS client.

	The KFS meta and chunkservers must already be running.
	"""
	try:
		return kfs.client(props)
	except:
		print "Unable to start the KFS client."
		print "Make sure that the meta- and chunkservers are running."
		sys.exit(1)

#
# Mmap source file
#
def map_source(src):
	"""Mmap the source file for reading"""
	try:
		fd = os.open(src, os.O_RDONLY)
	except OSError:
		print "Unable to open source file %s" % src
		sys.exit(1)
	size = os.fstat(fd)[ST_SIZE]
	return mmap.mmap(fd, size, mmap.MAP_SHARED, mmap.PROT_READ)

def make_test_directory(client):
	"""Create a new directory for testing"""
	now = time.asctime()
	dir = TESTNAME + "." + "_".join(now.split())
	client.mkdir(dir)
	return dir

def pick_region(mapping):
	"""Choose random source, dest, and length parameters for write"""
	size = mapping.size()
	src_off = random.randint(0, size - 1)
	dst_off = random.randint(0, get_param_int("max_file_size") - 1)
	maxlen = min(get_param_int("max_rw_size"), size - src_off)
	len = random.randint(1, maxlen)
	return (src_off, dst_off, len)

def dest(region):
	"""Extract the destination part of a region triple"""
	return (region[1], region[2])

def collides(region1, region2):
	"""Check whether two regions overlap"""
	dest1 = dest(region1)
	dest2 = dest(region2)
	first = min(dest1, dest2)
	second = max(dest1, dest2)
	return second[0] < first[0] + first[1]

def split(oldr, newr):
	"""Split (or trim) region oldr to avoid overlapping newr"""
	dold = dest(oldr)
	dnew = dest(newr)
	eold = sum(dold)
	enew = sum(dnew)
	r = []
	if dold < dnew:
		r.append((oldr[0], oldr[1], newr[1] - oldr[1]))
	if enew < eold:
		delta = enew - dold[0]
		r.append((oldr[0] + delta, enew, eold - enew))

	return r

def resolve_collisions(rlist, newr, collision):
	"""Adjust region list rlist to avoid overlaps with newr"""
	updated = []
	for i in range(len(rlist)):
		if collision[i]:
			updated.extend(split(rlist[i], newr))
		else:
			updated.append(rlist[i])

	return updated

def first_diff(s, t):
	"""position of first difference between s and t"""
	shorter = min(len(s), len(t))	# should be equal
	i = 0
	while i != shorter and s[i] == t[i]:
		i += 1
	return i

def sample(s, n):
	"""Show a sample of string s centered at position n"""
	start = max(n - 8, 0)
	finish = min(n + 24, len(s))
	return re.escape(s[start:finish])

class test_file:
	"""Keep track of test file state"""
	def __init__(self, client, filename, logfp):
		self.client = client
		self.name = filename
		self.logfp = logfp
		self.file = client.create(filename)
		self.regions = []

	def do_write(self, srcmap):
		"""Write to a random region in the file"""
		region = pick_region(srcmap)
		collisions = [collides(r, region) for r in self.regions]
		if len(collisions) != 0 and max(collisions):
			self.regions = resolve_collisions( \
				self.regions, region, collisions)
		self.regions.append(region)
		srcmap.seek(region[0])
		self.file.seek(region[1])
		self.file.write(srcmap.read(region[2]))
		print >> self.logfp, "%s: wrote %d @ %d" % \
			(self.name, region[2], region[1])

	def do_read(self, srcmap):
		"""Read and check a previously written region"""
		region = random.choice(self.regions)
		srcmap.seek(region[0])
		srcdata = srcmap.read(region[2])
		self.file.seek(region[1])
		kfsdata = self.file.read(region[2])
		print >> self.logfp, "%s: read %d @ %d" % \
			(self.name, region[2], region[1])
		if srcdata != kfsdata:
			print >> self.logfp, \
			    "!!! Data does not match source @ %d" % region[0]
			d = first_diff(srcdata, kfsdata)
			print >> self.logfp, \
			    "First difference at character %d" % d
			print >> self.logfp, "src: %s ..." % sample(srcdata, d)
			print >> self.logfp, "kfs: %s ..." % sample(kfsdata, d)


def pick_op(r, maxr, w, maxw):
	"""Choose a read or a write operation"""
	if r == maxr or random.random() >= float(w) / maxw:
		return "write"
	else:
		return "read"

#
# Main test routine
#
def run_test(config_file = default_config_file):
	#
	# Read test parameters, start up KFS client, open log, etc.
	#
	setup_params(config_file)
	client = start_client(get_param_string("kfs_properties"))
	logfp = open(get_param_string("log_file"), "w")
	seed = get_optional_param_int("random_seed")
	if seed != -1:
		random.seed(seed)
	mapping = map_source(get_param_string("source_file"))
	testdir = make_test_directory(client)
	nfiles = get_param_int("test_files")
	print >> logfp, "Creating %d files in %s" % (nfiles, testdir)
	client.cd(testdir)
	file_list = [test_file(client, TESTNAME + "." + str(i), logfp) \
				for i in range(nfiles)]

	maxreads = get_param_int("read_count")
	maxwrites = get_param_int("write_count")
	nreads, nwrites = 0, 0

	while nreads != maxreads or nwrites != maxwrites:
		op = pick_op(nreads, maxreads, nwrites, maxwrites)
		if op == "write":
			nwrites += 1
			file = random.choice(file_list)
			file.do_write(mapping)
		else:
			nreads += 1
			nonempty = [f for f in file_list \
					if len(f.regions) != 0]
			file = random.choice(nonempty)
			file.do_read(mapping)

	mapping.close()
	print >> logfp, "%d writes and %d reads done" % (nwrites, nreads)

if __name__ == "__main__":
	if len(sys.argv) == 1:
		config_file = default_config_file
	elif sys.argv[1] == "-":
		config_file = None
	else:
		config_file = sys.argv[1]
	run_test(config_file)
