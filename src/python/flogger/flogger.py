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
# \file flogger.py
# \brief perform random sequence of KFS operations for testing
#
import qfs
import sys
import random
import ConfigParser
from stat import *
from readdirplus import *

TESTNAME = "flogger"

#
# Test parameter handling
#
default_test_params = { 
	"kfs_properties": "KfsClient.prp", # KFS property file
	"log_file": TESTNAME + ".log",	# log of operations
	"op_count": "5000",		# no. of ops to perform
	"max_file_size": "100000",	# maximum file size
	"max_tree_depth": "100",	# max. directory tree depth
	"max_files": "10000",		# max. no. of files
	"max_directories": "1000",	# max. no. of directories
	"max_subdirs": "50",		# max. subdirs in a directory
	"max_dirsize": "500",		# max. entries in a directory
	"max_rw_size": "4096",		# max. read or write length
	"oplist" : "ascend descend mkdir rmdir create remove read write truncate",
	"opweight" : "100 100 60 40 200 180 220 300 150" }
	
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
		return qfs.client(props)
	except:
		print "Unable to start the KFS client."
		print "Make sure that the meta- and chunkservers are running."
		sys.exit(1)

#
# File system state information
# XXX make this a class to allow multiple test instances?
#
fs_state = {
	"depth" : 0,		# depth of current directory in tree
	"files_created" : 0,	# no. of files created
	"dirs_created" : 0 }	# no. of directorires created

def get_state(name):
	return fs_state[name]

def set_state(name, value):
	fs_state[name] = value

def change_state(name, delta):
	fs_state[name] += delta

#
# Utility functions
#
def special(dir):
	"""Is this a special directory name?"""
	return dir in (".", "..", "/")

def isempty(client, path):
	"""Is this an empty directory?"""
	dirlist = client.readdir(path)
	normal = [x for x in dirlist if not special(x)]
	return len(normal) == 0

def files(dirlist):
	"""extract plain files from a readdirplus list"""
	return [f for f in dirlist if rd_isreg(f)]

def nonzero_files(dirlist):
	return [f for f in files(dirlist) if rd_size(f) != 0]

def subdirs(dirlist):
	"""extract subdirectories from a readdir list"""
	return [d for d in dirlist if rd_isdir(d) and not special(rd_name(d))]

def emptydirs(client, dirlist):
	"""extract empty subdirectories from a readdir list"""
	return [d for d in subdirs(dirlist) if isempty(client, rd_name(d))]

def pick_one(namelist):
	"""Pick a random name from the list"""
	return random.choice(namelist)

def weighted_pick(weights):
	"""
	Given a list of N weights, return a random index 0 <= i < N
	with the probability of each choice proportional to its weight.
	"""
	total = sum(weights)
	fraction = random.randint(1, total)
	i = -1
	accum = 0
	while accum < fraction:
		i += 1
		accum += weights[i]

	return i

def invent_name(dirlist):
	"""Choose a random name not already present"""
	done = False
	dirnames = [rd_name(d) for d in dirlist]
	while not done:
		name = pick_one(filenames)
		done = name not in dirnames
	return name
#
# Classes representing each test operation.  Each class provides
# a weight function that determines the probability that the operation
# will get picked next, and a "doit" function that actually carries
# it out.  A log is recorded in the form of a series of Unix commands
# so that the same sequence can be tried on a non-KFS file system
# (however, the read and write operations are not real commands).
#
# The naming scheme is important: for each operation listed in the
# "oplist" entry of the config file, we expect to find a corresponding
# <operation>_op class defined here.
#
class test_op:
	"""Base class for test ops"""
	def __init__(self, client, logfp, wt):
		self.client = client	# kfs client object
		self.logfp = logfp	# file handle for log
		self.maxweight = wt	# maximum weight for op
		self.count = 0		# no. of times done

	def weight(self, dirlist):
		"Default: return max. weight unmodified"
		return self.maxweight

	def doit(self, dirlist):
		self.count += 1

	def log(self, msg):
		"Append messages to the log file"
		print  >> self.logfp, msg

class ascend_op(test_op):
	"""Go up one directory level"""
	def weight(self, dirlist):
		"""Weight to give to ascend op"""
		d = get_state("depth")
		reldepth = float(d) / get_param_int("max_tree_depth")
		return int(self.maxweight * reldepth)

	def doit(self, dirlist):
		"""Move up one level in the directory tree"""
		self.client.cd("..")
		change_state("depth", -1)
		self.log("cd ..")
		test_op.doit(self, dirlist)

class descend_op(test_op):
	"""Descend into a subdirectory"""
	def weight(self, dirlist):
		"""Weight for descend op"""
		nsub = len(subdirs(dirlist))
		if nsub == 0: return 0
		d = get_state("depth")
		reldepth = float(d) / get_param_int("max_tree_depth")
		return int(self.maxweight * (1 - reldepth))

	def doit(self, dirlist):
		"""Move down into a random subdirectory"""
		dirtuple = pick_one(subdirs(dirlist))
		dir = rd_name(dirtuple)
		self.client.cd(dir)
		change_state("depth", 1)
		self.log("cd " + dir)
		test_op.doit(self, dirlist)

class mkdir_op(test_op):
	"""Create a directory"""
	def weight(self, dirlist):
		"""Weight for mkdir"""
		if get_state("dirs_created") == get_param_int("max_directories"):
			return 0
		nsub = len(subdirs(dirlist))
		relpop = float(nsub) / get_param_int("max_subdirs")
		return int(self.maxweight * (1 - relpop))

	def doit(self, dirlist):
		dir = invent_name(dirlist)
		self.client.mkdir(dir)
		change_state("dirs_created", 1)
		self.log("mkdir " + dir)
		test_op.doit(self, dirlist)

class rmdir_op(test_op):
	"""Remove an empty directory"""
	def weight(self, dirlist):
		"""Weight for rmdir"""
		nsub = len(emptydirs(self.client, dirlist))
		return int(self.maxweight * nsub)

	def doit(self, dirlist):
		dirtuple = pick_one(emptydirs(self.client, dirlist))
		dir = rd_name(dirtuple)
		self.client.rmdir(dir)
		change_state("dirs_created", -1)
		self.log("rmdir " + dir)
		test_op.doit(self, dirlist)

class create_op(test_op):
	"""Create a file"""
	def weight(self, dirlist):
		"""Weight for create"""
		if get_state("files_created") == get_param_int("max_files"):
			return 0
		nfile = len(files(dirlist))
		relpop = float(nfile) / get_param_int("max_dirsize")
		return int(self.maxweight * (1 - relpop))

	def doit(self, dirlist):
		name = invent_name(dirlist)
		f = self.client.create(name)
		f.close()
		change_state("files_created", 1)
		self.log("touch " + name)
		test_op.doit(self, dirlist)

class remove_op(test_op):
	"""Remove a file"""
	def weight(self, dirlist):
		"""Weight for remove"""
		nfile = len(files(dirlist))
		relpop = float(nfile) / get_param_int("max_dirsize")
		return int(self.maxweight * relpop)

	def doit(self, dirlist):
		nametuple = pick_one(files(dirlist))
		name = rd_name(nametuple)
		self.client.remove(name)
		change_state("files_created", -1)
		self.log("rm " + name)
		test_op.doit(self, dirlist)

class read_op(test_op):
	"""Read from a file (no verification yet)"""
	def weight(self, dirlist):
		if len(nonzero_files(dirlist)) == 0:
			return 0
		else:
			return self.maxweight

	def doit(self, dirlist):
		nametuple = pick_one(nonzero_files(dirlist))
		name = rd_name(nametuple)
		size = rd_size(nametuple)
		if size == 0:
			return
		offset = random.randint(0, size - 1)
		length = min(random.randint(1, size - offset),
				get_param_int("max_rw_size"))
		f = self.client.open(name, "r")
		f.seek(offset)
		result = f.read(length)
		f.close()
		self.log("read %d from %s @%d" % (length, name, offset))
		test_op.doit(self, dirlist)

class write_op(test_op):
	"""Write to a random file offset"""
	def weight(self, dirlist):
		if len(files(dirlist)) == 0:
			return 0
		else:
			return self.maxweight

	def doit(self, dirlist):
		nametuple = pick_one(files(dirlist))
		name = rd_name(nametuple)
		biggest = get_param_int("max_file_size")
		offset = random.randint(0, biggest - 1)
		length = min(random.randint(1, biggest - offset),
				get_param_int("max_rw_size"))
		f = self.client.open(name, "w+")
		f.seek(offset)
		output = "x" * length
		result = f.write(output)
		f.close()
		self.log("write %d to %s @%d" % (length, name, offset))
		test_op.doit(self, dirlist)

class truncate_op(test_op):
	"""Truncate at a random offset"""
	def weight(self, dirlist):
		if len(nonzero_files(dirlist)) == 0:
			return 0
		else:
			return self.maxweight

	def doit(self, dirlist):
		nametuple = pick_one(nonzero_files(dirlist))
		name = rd_name(nametuple)
		size = rd_size(nametuple)
		offset = random.randint(0, size - 1)
		f = self.client.open(name, "w+")
		f.truncate(offset)
		f.close()
		self.log("truncated %s @%d" % (name, offset))
		test_op.doit(self, dirlist)

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
	global filenames
	filenames = []
	for w in open("/usr/share/dict/words", "r"):
		filenames.append(w.rstrip())

	#
	# Instantiate objects for each test operation
	#
	opname = get_param_string("oplist").split(" ")
	opweight = [int(w) for w in get_param_string("opweight").split(" ")]
	ops = []
	for i in range(len(opname)):
		classname = opname[i] + "_op"
		opclass = globals()[classname]
		opinstance = opclass(client, logfp, opweight[i])
		ops.append(opinstance)

	#
	# Repeatedly perform random operations until done
	#
	opsdone = 0
	while opsdone != get_param_int("op_count"):
		dirlist = client.readdirplus(".")
		weights = [op.weight(dirlist) for op in ops]
		i = weighted_pick(weights)
		ops[i].doit(dirlist)
		opsdone += 1

	print "%d ops completed:" % (opsdone,)
	for i in range(len(ops)):
		print "%s\t%d" % (opname[i], ops[i].count)

if __name__ == "__main__":
	if len(sys.argv) == 1:
		config_file = default_config_file
	elif sys.argv[1] == "-":
		config_file = None
	else:
		config_file = sys.argv[1]
	run_test(config_file)
