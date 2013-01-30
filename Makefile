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

ifndef VERBOSE
MAKEFLAGS += --no-print-directory
endif

all: release

prep:
	test -d build || mkdir build

release: prep
	cd build && \
	{ test -d release || mkdir release; } && \
	cd release && \
	cmake -D CMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	$(MAKE) install
	./src/java/javabuild.sh clean
	if test -x "`which mvn 2>/dev/null`"; then \
		./src/java/javabuild.sh ; fi

debug: prep
	cd build && \
	{ test -d debug || mkdir debug; } && \
	cd debug && \
	cmake ../.. && \
	$(MAKE) install
	./src/java/javabuild.sh clean
	if test -x "`which mvn 2>/dev/null`"; then \
		./src/java/javabuild.sh ; fi

hadoop-jars: release
	./src/java/javabuild.sh clean
	if test -x "`which mvn 2>/dev/null`"; then \
		./src/java/javabuild.sh clean &&      \
		./src/java/javabuild.sh 0.23.4 &&     \
		./src/java/javabuild.sh 1.0.2  &&     \
		./src/java/javabuild.sh 1.0.4  &&     \
		./src/java/javabuild.sh 1.1.0  &&     \
		./src/java/javabuild.sh 2.0.2-alpha   \
	; fi

tarball: hadoop-jars
	cd build && \
	myuname=`uname -s`; \
	if [ x"$$myuname" = x'Linux' -a -f /etc/issue ]; then \
	    myflavor=`head -n 1 /etc/issue | cut -d' ' -f1` ; \
	    if [ x"$$myflavor" = x'Ubuntu' ]; then \
		myflavor="$$myflavor-`head -n 1 /etc/issue | cut -d' ' -f2`" ; \
	    elif [ x"$$myflavor" = x ]; then \
		myflavor=$$myuname ; \
	    else \
		myflavor="$$myflavor-`head -n 1 /etc/issue | cut -d' ' -f3`" ; \
	    fi ; \
	else \
	    if echo "$$myuname" | grep CYGWIN > /dev/null; then \
		myflavor=cygwin ; \
	    else \
		myflavor=$$myuname ; \
	    fi ; \
	fi ; \
	qfsversion=`../src/cc/common/buildversgit.sh -v 2> /dev/null | head -1` ; \
	tarname="qfs-$$myflavor-$$qfsversion-`uname -m`" ;\
	tarname=`echo "$$tarname" | tr A-Z a-z` ; \
	{ test -d tmpreldir || mkdir tmpreldir; } && \
	rm -rf "tmpreldir/$$tarname" && \
	mkdir "tmpreldir/$$tarname" && \
	cp -r release/bin release/lib release/include ../scripts ../webui \
	     ../examples ../benchmarks "tmpreldir/$$tarname/" && \
	if ls -1 ./java/qfs-access/qfs-access-*.jar > /dev/null 2>&1; then \
	    cp ./java/qfs-access/qfs-access*.jar "tmpreldir/$$tarname/lib/"; fi && \
	if ls -1 ./java/hadoop-qfs/hadoop-*.jar > /dev/null 2>&1; then \
	    cp ./java/hadoop-qfs/hadoop-*.jar "tmpreldir/$$tarname/lib/"; fi && \
	tar cvfz "$$tarname".tgz -C ./tmpreldir "$$tarname" && \
	rm -rf tmpreldir

python-release: release
	cd build/release && python ../../src/cc/access/kfs_setup.py build

python-debug: debug
	cd build/debug && python ../../src/cc/access/kfs_setup.py build

test-debug: debug
	cd build/debug && ../../src/test-scripts/qfstest.sh

test-release: release
	cd build/release && ../../src/test-scripts/qfstest.sh

clean:
	rm -rf build/release build/debug build/qfs-*.tgz build/java
