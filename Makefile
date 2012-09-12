# $Id$
#
# Created 2012/07/27
# Author: Mike Ovsiannikov
#
# Copyright 2012 Quantcast Corp.
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
# Do not assume gnumake -- keep it as simple as possible

UNAME := $(shell uname -s)
ARCH := $(shell uname -m)
FLAVOR := $(shell uname -s)
VERSION := $(shell uname -r)

QFSVERSION := 1.0
tarname :=

ifeq ($(UNAME), Linux)
 FLAVOR := $(shell head -n 1 /etc/issue | cut -d" " -f1)
 ifeq ($(FLAVOR), Ubuntu)
   VERSION := $(shell head -n 1 /etc/issue | cut -d" " -f2)
 else
   VERSION := $(shell head -n 1 /etc/issue | cut -d" " -f3)
 endif
endif

ifneq (,$(findstring CYGWIN,$(UNAME)))
 FLAVOR := 'Cygwin'
endif

tarname := qfs-$(FLAVOR)-$(VERSION)-$(QFSVERSION)-$(ARCH)
tarname := $(shell echo $(tarname) | tr A-Z a-z)

all: release

prep:
	test -d build || mkdir build

release: prep
	cd build && \
	{ test -d release || mkdir release; } && \
	cd release && \
	cmake -D CMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	make install
	if test -x "`which ant 2>/dev/null`"; then ant jar; fi
	if test -x "`which python 2>/dev/null`"; then \
            cd build/release && python ../../src/cc/access/kfs_setup.py build; fi

debug: prep
	cd build && \
	{ test -d debug || mkdir debug; } && \
	cd debug && \
	cmake ../.. && \
	make install
	if test -x "`which ant 2>/dev/null`"; then ant jar; fi
	if test -x "`which python 2>/dev/null`"; then \
            cd build/debug && python ../../src/cc/access/kfs_setup.py build; fi

tarball: release
	cd build && \
	{ test -d tmpreldir/$(tarname) || mkdir -p tmpreldir/$(tarname); } && \
	rm -rf tmpreldir/$(tarname)/* && \
	cp -r release/bin release/lib release/include ../scripts ../webui ../examples ../benchmarks tmpreldir/$(tarname) && \
	tar cvfz $(tarname).tgz -C ./tmpreldir $(tarname) && \
	rm -rf tmpreldir

test-debug: debug
	cd build/debug && ../../src/test-scripts/kfstest.sh

test-release: release
	cd build/release && ../../src/test-scripts/kfstest.sh

clean:
	rm -rf build/release build/debug build/$(tarname).tgz build/kfs*.jar build/classes
