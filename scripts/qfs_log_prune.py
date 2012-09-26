#!/usr/bin/env python
#
# Copyright 2008 Quantcast Corp.
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
# \file qfs_log_prune.py
# \brief KFS transaction log housekeeping
#
# We gzip and keep all the old log files around.  This is a bit of an
# overkill---we can prune away files that are no longer referenced by
# any checkpoint.  We find the oldest checkpoint file and find that
# the log it references; files older than that log file are deleted.
#
import os
import sys
import glob
import stat
import time
import getopt
import gzip

def age(file):
	"""return age of file (last mtime) in seconds"""

	now = time.time()
	return now - os.stat(file)[stat.ST_MTIME]

def orderByAge(this, that):
	if age(this) > age(that):
		return this
	return that

def olderThanLog(logfile, lognum):
	"""Return True if logfile which is of the form log.# has a
	sequence number less than lognum"""
	(base, extn) = os.path.splitext(logfile)
	extn = extn[1:]
	if extn == 'gz':
		val = int(os.path.splitext(base)[1][1:])
	else:
		val = int(extn)
	return val < lognum
		
def prunefiles(cpdir, logdir):
	"""Find the log file that is referenced by the oldest CP file.
	Log files that are older than that one can be deleted."""
	oldest = reduce(orderByAge, glob.glob(cpdir + '/chkpt.*'))
	if oldest is None:
		return
	print "Oldest cp: %s" % oldest
	# get the log file
	for l in open(oldest).xreadlines():
		if l.startswith('log/'):
			lognum = int(os.path.splitext(l[4:])[1][1:])
			print lognum
			alllogfiles = glob.glob(logdir + '/log.*')
			oldones = [f for f in alllogfiles if olderThanLog(f, lognum)]
			for f in oldones:
				os.remove(f)
			break

if (__name__ == "__main__"):
	if len(sys.argv) != 3:
		raise getopt.GetoptError, "missing arguments"

	# kfscpdir, kfslogdir
	prunefiles(sys.argv[1], sys.argv[2])
