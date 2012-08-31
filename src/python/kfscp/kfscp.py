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
#
# Python module that parses a KFS checkpoint file into a set of
# dictionaries to allow easy examination and consistency checking.
#
import re

def strip_dots(path):
	"""Strip '.' and '..' components from path"""
	if (path == "/"):
		return path

	out = []
	for p in path[1:].split('/'):
		if p == "..":
			if len(out) != 0:
				del out[-1]
			else:
				out.append(p)
		elif p != ".":
			out.append(p)

	return "/" + "/".join(out)

def add_to_path(current, new):
	"""Combine current and new into one path"""
	if len(new) == 0:
		path = current
	elif new[0] == "/":
		path = new
	elif current == "/":
		path = "/" + new
	else:
		path = current + "/" + new
	return strip_dots(path)

class cp:
	"""Read a KFS checkpoint file and convert it into Python structures

	The cp file is converted into a set of dictionaries that can be
	inspected.  The main one, 'byfid', is indexed by file ID and
	collects all the information for each file; 'byname' maps the
	tuple (parent diretory file id, name) to a file id, and 'bychunk'
	maps chunk ids to the file id of their owners.

	As an aid to navigate, 'cd' and 'ls' commands are provided.
	Also, the 'check' command runs consistency checks.
	"""
	
	def __init__(self, cpfile):
		self.read(cpfile)

	def reset(self):
		self.byfid = { }
		self.byname = { }
		self.bychunk = { }
		self.cpdescr = { }
		self.cwd = "/"

	def read(self, cpfile):
		"""Read cp from file and generate Python representation"""
		self.reset()
		try:
			cp = open(cpfile, "r")
		except IOError:
			print "can't open", cpfile
			return

		line = 0
		for entry in cp:
			line += 1
			if len(entry) == 1:	# just the newline
				continue
			parts = entry.rstrip().split('/')
			parser_name = parts[0] + "_parse"
			valid = hasattr(self, parser_name)
			if valid:
				parser = getattr(self, parser_name)
				valid = callable(parser) and parser(parts)
			if not valid:
				print "invalid entry, line %d: %s" % \
						(line, entry)

	def validate(self, parts, re_list):
		m = [re.match(re_list[i] + "$", parts[i + 1]) \
				for i in range(len(re_list))]
		return bool(min(m))

	def checkpoint_parse(self, parts):
		ok = self.validate(parts, ["\d+"])
		if ok:
			self.cpdescr["number"] = int(parts[1])
		return ok

	def version_parse(self, parts):
		ok = self.validate(parts, ["\d+"])
		if ok:
			self.cpdescr["version"] = int(parts[1])
		return ok

	def fid_parse(self, parts):
		ok = self.validate(parts, ["\d+"])
		if ok:
			self.cpdescr["next_fid"] = int(parts[1])
		return ok

	def chunkId_parse(self, parts):
		ok = self.validate(parts, ["\d+"])
		if ok:
			self.cpdescr["next_chunkId"] = int(parts[1])
		return ok

	def time_parse(self, parts):
		tpat = "[A-Z][a-z]{2} [A-Z][a-z]{2}\s+\d+ \d+:\d+:\d+ \d+"
		ok = self.validate(parts, [tpat])
		if ok:
			self.cpdescr["saved_at"] = parts[1]

		return ok

	def log_parse(self, parts):
		self.cpdescr["log_file"] = "/".join(parts[1:])
		return True

	def makefattr(self, parts):
		d = dict()
		d["type"] = parts[1]	# dir | file
		d["chunkcount"] = int(parts[5])
		d["mtime"] = (int(parts[7]), int(parts[8]))
		d["ctime"] = (int(parts[10]), int(parts[11]))
		d["crtime"] = (int(parts[13]), int(parts[14]))
		return d

	def fattr_parse(self, parts):
		pat = ["dir|file", "id", "\d+", "chunkcount", "\d+",
			"mtime", "\d+", "\d+", "ctime", "\d+", "\d+",
			"crtime", "\d+", "\d+"]
		ok = self.validate(parts, pat)
		if ok:
			fdict = self.makefattr(parts)
			my_id = int(parts[3])
			if my_id not in self.byfid:
				self.byfid[my_id] = dict()
			self.byfid[my_id]["fattr"] = fdict

		return ok

	def dentry_parse(self, parts):
		pat = ["name", ".+", "id", "\d+", "parent", "\d+"]
		ok = self.validate(parts, pat)
		if ok:
			my_name = parts[2]
			my_id = int(parts[4])
			parent = int(parts[6])
		else:
			pat2 = ["name", "", "", "id", "\d+", "parent", "\d+"]
			ok = self.validate(parts, pat2)
			if ok:
				my_name = "/"
				my_id = int(parts[5])
				parent = int(parts[7])

		if ok:
			if my_id not in self.byfid:
				self.byfid[my_id] = dict()
			d = self.byfid[my_id]
			if parent not in self.byfid:
				self.byfid[parent] = dict()
			p = self.byfid[parent]
			if my_name == ".":
				if "dot" in d:
					print "Duplicate '.' entries"
					return False
				if my_id != parent:
					print "'.' not linked to self"
					return False
				d["dot"] = my_id
			elif my_name == "..":
				if "dotdot" in p:
					print "Duplicate '..' entries"
					return False
				p["dotdot"] = my_id
			else:
				if "name" in d:
					print "Duplicate directory entries"
					return False
				d["name"] = my_name
				d["parent"] = parent
				if "children" not in p:
					p["children"] = []
				p["children"].append(my_id)
				self.byname[(parent, my_name)] = my_id

		return ok

	def chunkinfo_parse(self, parts):
		pat = ["fid", "\d+", "chunkid", "\d+", "offset", "\d+"]
		ok = self.validate(parts, pat)
		if ok:
			my_id = int(parts[2])
			chunk_id = int(parts[4])
			offset = int(parts[6])
			if my_id not in self.byfid:
				self.byfid[my_id] = dict()
			d = self.byfid[my_id]
			if "chunklist" not in d:
				d["chunklist"] = dict()
			chlist = d["chunklist"]
			if offset in chlist:
				print "Duplicate chunkinfo for offset"
				return False
			chlist[offset] = chunk_id
			if chunk_id in self.bychunk:
				print "Duplicate chunkinfo for chunk_id"
				return False
			self.bychunk[chunk_id] = my_id

		return ok

	def check_missing_dentries(self):
		for id in self.byfid:
			if "name" not in self.byfid[id]:
				print "File %d has no directory entry" % id

	def check_missing_fattr(self):
		for id in self.byfid:
			if "fattr" not in self.byfid[id]:
				print "File %d has no attributes" % id

	def check_dot(self):
		for id in self.byfid:
			e = self.byfid[id]
			if "fattr" not in e:
				continue	# already reported
			type = e["fattr"]["type"]
			if type == "dir" and "dot" not in e:
				print "Directory %d has no '.' link" % id
			if type != "dir" and "dot" in e:
				print "Plain file %d has a '.' link" % id

	def check_dotdot(self):
		for id in self.byfid:
			e = self.byfid[id]
			if "fattr" not in e:
				continue	# already reported
			type = e["fattr"]["type"]
			if type == "dir" and "dotdot" not in e:
				print "Directory %d has no '..' link" % id
			if type != "dir" and "dotdot" in e:
				print "Plain file %d has a '..' link" % id
			if "dotdot" not in e:
				continue
			if "parent" not in e:
				continue	# already reported
			link = e["dotdot"]
			parent = e["parent"]
			if link != parent:
				print "Directory %d has invalid '..' link" % id

	def check(self):
		"""Run consistency checks"""
		self.check_missing_dentries()
		self.check_missing_fattr()
		self.check_dot()
		self.check_dotdot()

	def lookup(self, path):
		"""Convert path into file id"""
		target = add_to_path(self.cwd, path)
		id = 2				# start at root
		if target == "/":
			return id

		for p in target[1:].split('/'):
			if id not in self.byfid:
				return -1
			if (id, p) not in self.byname:
				return -1
			id = self.byname[(id, p)]

		return id

	def cd(self, path):
		"""Change current directory"""
		id = self.lookup(path)
		if id < 0:
			print "%s: No such file or directory" % path
		elif self.byfid[id]["fattr"]["type"] != "dir":
			print "%s: Not a directory" % path
		else:
			self.cwd = add_to_path(self.cwd, path)

	def name(self, id):
		return self.byfid[id]["name"]

	def name_and_type(self, id):
		e = self.byfid[id]
		return (e["name"], e["fattr"]["type"])

	def ls(self, path = "", flags = ""):
		"""List file or directory"""
		id = self.lookup(path)
		if id < 0:
			print "%s: No such file or directory" % path
			return None
		e = self.byfid[id]
		if e["fattr"]["type"] == "file":
			return [ path ]

		if "children" in e:
			kids = e["children"]
		else:
			kids = []

		if flags == "F":
			formatter = self.name_and_type
		else:
			formatter = self.name
			
		return [formatter(k) for k in kids]
