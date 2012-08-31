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
# do a recursive remove on a KFS directory
#
import kfs
from readdirplus import *
from stat import *

def rmr(client, path):
	if S_ISREG(client.stat(path)[ST_MODE]):
		client.remove(path)
	else:
		do_rmr(client, path)

def do_rmr(client, path):
	rdp = client.readdirplus(path)
	dot = [r for r in rdp if rd_name(r) == '.']
	my_id = rd_id(dot[0])
	subs = [r for r in rdp if rd_isdir(r) and rd_id(r) != my_id and \
			rd_name(r) != '..']
	plain = [r for r in rdp if rd_isreg(r)]
	for r in plain:
		client.remove(path + "/" + rd_name(r))
	for r in subs:
		do_rmr(client, path + "/" + rd_name(r))
	client.rmdir(path)
