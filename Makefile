# $Id$
#
# Created 2012/07/27
# Author: Mike Ovsiannikov
#
# Copyright 2012-2016 Quantcast All rights reserved.
#
# This file is part of Quantcast File System.
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
# Do not assume GNU Make. Keep this makefile as simple as possible.

BUILD_TYPE=release
CMAKE_OPTIONS=-D CMAKE_BUILD_TYPE=RelWithDebInfo
CMAKE=cmake
MAKE_OPTIONS=
QFSTEST_OPTIONS=
JAVA_BUILD_OPTIONS=
QFSHADOOP_VERSIONS=0.23.11  1.0.4  1.1.2  2.5.1  2.7.2  2.7.7  2.8.5  2.9.2  2.10.1  3.1.4  3.2.2  3.3.1

QFS_PYTHON_DIR=python-qfs
QFS_PYTHON_WHEEL_DIR=${QFS_PYTHON_DIR}/dist
QFS_PYTHON_TEST_OPTION=test -d ${QFS_PYTHON_WHEEL_DIR} && echo -python-wheel-dir ${QFS_PYTHON_WHEEL_DIR}
QFS_MSTRESS_ON=true

.PHONY: all
all: build

.PHONY: dir
dir:
	mkdir -p build/${BUILD_TYPE}

.PHONY: run-cmake
run-cmake: dir
	cd build/${BUILD_TYPE} && ${CMAKE} ${CMAKE_OPTIONS} ../..

.PHONY: build
build: run-cmake
	cd build/${BUILD_TYPE} && $(MAKE) ${MAKE_OPTIONS} install \
	`${QFS_MSTRESS_ON} && \
		echo ${QFSHADOOP_VERSIONS} | grep 2.10.1 >/dev/null 2>&1 && \
		mvn --version >/dev/null 2>&1 && echo mstress-tarball`

.PHONY: java
java: build
	./src/java/javabuild.sh ${JAVA_BUILD_OPTIONS} clean
	./src/java/javabuild.sh ${JAVA_BUILD_OPTIONS}

.PHONY: hadoop-jars
hadoop-jars: java
	if mvn --version >/dev/null 2>&1 ; then \
	    ./src/java/javabuild.sh ${JAVA_BUILD_OPTIONS} clean && \
	    for hadoop_version in ${QFSHADOOP_VERSIONS}; do \
	        ./src/java/javabuild.sh \
	            ${JAVA_BUILD_OPTIONS} "$${hadoop_version}" \
	            || exit 1; \
	    done \
	; fi

.PHONY: go
go: build
	if go version >/dev/null 2>&1 ; then \
		QFS_BUILD_DIR=`pwd`/build/$(BUILD_TYPE) && \
		cd src/go && \
		CGO_CFLAGS="-I$${QFS_BUILD_DIR}/include" && \
		export CGO_CFLAGS && \
		CGO_LDFLAGS="-L$${QFS_BUILD_DIR}/lib" && \
		export CGO_LDFLAGS && \
		go clean -i && \
		go get -t -v && \
		go build -v || \
		exit 1; \
	else \
		echo "go is not available"; \
	fi

.PHONY: tarball
tarball: hadoop-jars python
	cd build && \
	myuname=`uname -s`; \
	myarch=`cc -dumpmachine 2>/dev/null | cut -d - -f 1` ; \
	[ x"$$myarch" = x ] && \
	    myarch=`gcc -dumpmachine 2>/dev/null | cut -d - -f 1` ; \
	[ x"$$myarch" = x ] && myarch=`uname -m` ; \
	if [ x"$$myuname" = x'Linux' -a \( -f /etc/issue -o -f /etc/system-release \) ]; then \
		if [ -f /etc/system-release ]; then \
			myflavor=`head -n 1 /etc/system-release | cut -d' ' -f1` ; \
			myflavor="$$myflavor-`head -n 1 /etc/system-release | sed -e 's/^.* *release *//' | cut -d' ' -f1 | cut -d. -f1`" ; \
		else \
			myflavor=`head -n 1 /etc/issue | cut -d' ' -f1` ; \
			if [ x"$$myflavor" = x'Ubuntu' ]; then \
				myflavor="$$myflavor-`head -n 1 /etc/issue | cut -d' ' -f2 | cut -d. -f1,2`" ; \
			elif [ x"$$myflavor" = x ]; then \
				myflavor=$$myuname ; \
			else \
				myflavor="$$myflavor-`head -n 1 /etc/issue | cut -d' ' -f3 | cut -d. -f1,2`" ; \
			fi ; \
		fi ; \
	else \
	    if echo "$$myuname" | grep CYGWIN > /dev/null; then \
		myflavor=cygwin ; \
	    else \
		myflavor=$$myuname ; \
	    fi ; \
	fi ; \
	qfsversion=`../src/cc/common/buildversgit.sh --release` ; \
	tarname="qfs-$$myflavor-$$qfsversion-$$myarch" ;\
	tarname=`echo "$$tarname" | tr A-Z a-z` ; \
	{ test -d tmpreldir || mkdir tmpreldir; } && \
	rm -rf "tmpreldir/$$tarname" && \
	mkdir "tmpreldir/$$tarname" && \
	cp -r ${BUILD_TYPE}/bin ${BUILD_TYPE}/lib ${BUILD_TYPE}/include ../scripts ../webui \
	     ../examples ../benchmarks "tmpreldir/$$tarname/" && \
	if ls -1 ./java/qfs-access/qfs-access-*.jar > /dev/null 2>&1; then \
	    cp ./java/qfs-access/qfs-access*.jar "tmpreldir/$$tarname/lib/"; fi && \
	if ls -1 ./java/hadoop-qfs/hadoop-*.jar > /dev/null 2>&1; then \
	    cp ./java/hadoop-qfs/hadoop-*.jar "tmpreldir/$$tarname/lib/"; fi && \
	if ls -1 ${BUILD_TYPE}/${QFS_PYTHON_WHEEL_DIR}/qfs*.whl > /dev/null 2>&1; then \
		cp ${BUILD_TYPE}/${QFS_PYTHON_WHEEL_DIR}/qfs*.whl \
			"tmpreldir/$$tarname/lib/"; fi && \
	if ls -1 ${BUILD_TYPE}/benchmarks/mstress.tgz > /dev/null 2>&1; then \
		cp ${BUILD_TYPE}/benchmarks/mstress.tgz \
			"tmpreldir/$$tarname/benchmarks/"; fi && \
	tar cvfz "$$tarname".tgz -C ./tmpreldir "$$tarname" && \
	rm -rf tmpreldir

.PHONY: python
python: build
	if python3 -c 'import sys; exit(0 if sys.version_info >= (3, 6) else 1)' \
			>/dev/null 2>&1 && \
			python3 -c 'import venv' >/dev/null 2>&1 ; then \
		cd build/${BUILD_TYPE} && \
		rm -rf ${QFS_PYTHON_DIR} && \
		mkdir ${QFS_PYTHON_DIR} && \
		cd ${QFS_PYTHON_DIR} && \
		ln -s .. qfs && \
		ln -s ../../../src/cc/access/kfs_setup.py setup.py && \
		python3 -m venv .venv && \
		. .venv/bin/activate && python -m pip install build && \
		python -m build -w . ; \
	else \
		echo 'python3 module venv is not available'; \
	fi

.PHONY: mintest
mintest: hadoop-jars python
	cd build/${BUILD_TYPE} && \
	../../src/test-scripts/qfstest.sh \
		`${QFS_PYTHON_TEST_OPTION}` \
		-install-prefix . -auth ${QFSTEST_OPTIONS}

.PHONY: test
test: mintest
	cd build/${BUILD_TYPE} && \
	installbindir=`pwd`/bin && \
	metadir=$$installbindir && \
	export metadir && \
	chunkdir=$$installbindir && \
	export chunkdir && \
	toolsdir=$$installbindir/tools && \
	export toolsdir && \
	devtoolsdir=$$installbindir/devtools && \
	export devtoolsdir && \
	echo '--------- QC RS recovery test ---------' && \
	../../src/test-scripts/recoverytest.sh && \
	echo '--------- Jerasure recovery test ------' && \
	filecreateparams='fs.createParams=1,6,3,1048576,3,15,15' \
	../../src/test-scripts/recoverytest.sh && \
	if [ -d qfstest/certs ]; then \
		echo '--------- Test without authentication --------' && \
		../../src/test-scripts/qfstest.sh \
			`${QFS_PYTHON_TEST_OPTION}` \
			-install-prefix . -noauth ${QFSTEST_OPTIONS} ; \
	fi

.PHONY: rat
rat: dir
	cd build/${BUILD_TYPE} && ../../scripts/rat.sh ../..

.PHONY: clean
clean:
	rm -rf build
