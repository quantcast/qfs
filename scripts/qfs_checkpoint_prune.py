#!/usr/bin/env python
#
# $Id$
#
# Copyright 2008-2012 Quantcast Corp.
# Copyright 2006 Kosmix Corp.
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
# \file qfs_checkpoint_prune.py
# \brief QFS checkpoint housekeeping
#
# Housekeeping script to clean up the KFS checkpoint and log files,
# which would otherwise accumulate without limit.  Looks for all
# files of the form <prefix>.<N> in a specified directory and removes
# all but a subset of them according to the rules
#
# - maintain at least the specified minimum number
# - keep more if necessary to cover the specified time span
# - but don't exceed the specified maximum
#
# Two instances of this script should be run periodically from the
# main cleanup script, one for the checkpoints and the other for the
# logs.
#
# Note that the decision to measure the relative age of the files
# by their sequence numbers rather than mtime is deliberate, since
# QFS controls the numbers whereas time is controlled externally
# and can go backwards.
#
# Old files can either be deleted or compressed via gzip.
#
import os
import sys
import glob
import stat
import time
import getopt
import gzip
import re

def age(file):
	"""return age of file (last mtime) in seconds"""

	now = time.time()
	return now - os.stat(file)[stat.ST_MTIME]

def isNonCompressedFile (filename):
	if filename.find(".tmp") >= 0:
		return False
	extn = os.path.splitext(filename)[1][1:]
	if re.match("^\d+$", extn):
		return True
	return False

def oldfiles(prefix, minsave, maxsave, mintime):
	"""return a list of the oldest files of a given type
	
	Look up all files of the form "prefix.*" in the current
	directory and determine which ones are candidates for
	deletion.  The rules for deciding this are as follows:
	
		the part of the file after the dot is assumed to be a
		monotonically increasing integer, which determines the
		relative age of the files;
	
		at least "minsave" files are to be preserved;
	
		the youngest removed file should have an mtime at least
		"mintime" seconds in the past; we will preserve up to
		"maxsave" files to try to satisfy this condition.
	
	"""
	# get all candidates and rearrange by sequence number
	files = filter(isNonCompressedFile, glob.glob(prefix + ".*"))
	tails = [int(os.path.splitext(f)[1][1:]) for f in files]
	tails.sort()
	files = ["%s.%d" % (prefix, t) for t in tails]

	# trim off the minimum number
	files = files[:-minsave]

	# trim extras required by time constraint
	saved = minsave
	while len(files) != 0 and saved < maxsave and age(files[-1]) < mintime:
		del files[-1]
		saved += 1

	return files

def compressFiles(oldones):
	""" Compress a list of files using gzip """
	for fn in oldones:
		f = open(fn, 'rb')
		cf = gzip.open(fn + '.gz', 'wb')
		while 1:
			data = f.read(4096)
			if data == "":
				break
			cf.write(data)
		f.close()
		cf.close()
	
def prunefiles(dir, prefix, minsave, maxsave, mintime, compress):
	"""remove/compress all sufficiently old files from directory "dir"

	Change directory to "dir", find old files, and delete/compress them;
	see oldfiles above for an explanation of the parameters

	"""
	os.chdir(dir);
	oldones = oldfiles(prefix, minsave, maxsave, mintime)
	if compress > 0:
		compressFiles(oldones)
	for f in oldones:
		os.remove(f)

if (__name__ == "__main__"):
	minsave, maxsave, mintime = 10, 100, 3600

	(opts, args) = getopt.getopt(sys.argv[1:], "m:M:t:z",
					["min=", "max=", "time=", "compress"])

	compress = 0
	for (o, a) in opts:
		if o in ("-m", "--min"):
			minsave = int(a)
		elif o in ("-M", "--max"):
			maxsave = int(a)
		elif o in ("-t", "--time"):
			mintime = int(a)
		elif o in ("-z", "--compress"):
			compress = 1

	if maxsave < 0 or minsave < 0 or mintime < 0 or maxsave < minsave:
		raise getopt.GetoptError, "invalid options"

	if len(args) != 2:
		raise getopt.GetoptError, "missing arguments"

	prunefiles(args[0], args[1], minsave, maxsave, mintime, compress)
