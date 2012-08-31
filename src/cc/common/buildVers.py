#!/usr/bin/env python
#
# $Id: buildVers.py 192 2008-10-22 05:33:26Z sriramsrao $
#
# Copyright 2008-2010 Quantcast Corp.
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
# Script to generate Version.cc.  What we are interested in is the
# revision # from  the repository for the entire tree as we built the
# source.  This script is invoked by the makefile.
#

import os,sys

# get the version info from p4
buildstr = "perforce1:1666/trunk@1000"
srcRevision = "100"

fh = open(sys.argv[2], "w")
print >> fh, "//"
print >> fh, "// This file is generated during compilation.  DO NOT EDIT!"
print >> fh, "//"
print >> fh, "#include \"Version.h\" "
print >> fh, "const std::string KFS::KFS_BUILD_VERSION_STRING=\"%s\";" % buildstr
print >> fh, "const std::string KFS::KFS_SOURCE_REVISION_STRING=\"%s\";" % srcRevision

