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

BUILD_TYPE ?= release
CMAKE_OPTIONS ?= -D CMAKE_BUILD_TYPE=RelWithDebInfo
MAKE_OPTIONS ?=
QFSTEST_OPTIONS ?=

.PHONY: all
all: build

.PHONY: dir
dir:
	export BUILD_TYPE=${BUILD_TYPE}
	mkdir -p build/${BUILD_TYPE}

.PHONY: build
build: dir
	cd build/${BUILD_TYPE} && cmake ${CMAKE_OPTIONS} ../..
	cd build/${BUILD_TYPE} && $(MAKE) ${MAKE_OPTIONS} install

.PHONY: java
java: build
	./src/java/javabuild.sh clean
	./src/java/javabuild.sh

.PHONY: hadoop-jars
hadoop-jars: build java
	./src/java/javabuild.sh clean
	if test -x "`which mvn 2>/dev/null`"; then \
		./src/java/javabuild.sh clean   && \
		./src/java/javabuild.sh 0.23.4  && \
		./src/java/javabuild.sh 0.23.11 && \
		./src/java/javabuild.sh 1.0.4   && \
		./src/java/javabuild.sh 1.1.2   && \
		./src/java/javabuild.sh 2.5.1   && \
		./src/java/javabuild.sh 2.7.2      \
	; fi

.PHONY: tarball
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
	qfsversion=`../src/cc/common/buildversgit.sh --release` ; \
	tarname="qfs-$$myflavor-$$qfsversion-`uname -m`" ;\
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
	tar cvfz "$$tarname".tgz -C ./tmpreldir "$$tarname" && \
	rm -rf tmpreldir

.PHONY: python
python: build
	cd build/${BUILD_TYPE} && python ../../src/cc/access/kfs_setup.py build

.PHONY: test
test: build
	cd build/${BUILD_TYPE} && ../../src/test-scripts/qfstest.sh ${QFSTEST_OPTIONS}

.PHONY: gtest
gtest: build
	build/${BUILD_TYPE}/src/cc/tests/test.t

.PHONY: rat
rat: dir
	cd build/${BUILD_TYPE} && cmake ${CMAKE_OPTIONS} ../..
	cd build/${BUILD_TYPE} && $(MAKE) ${MAKE_OPTIONS} rat

.PHONY: clean
clean:
	rm -rf build
