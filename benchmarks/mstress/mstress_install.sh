#!/bin/sh
#
# $Id$
#
# Copyright 2012 Quantcast Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
#
# To run mstress, the participating client hosts and the master host should all
# have the mstress files in the same path.
#
# This script, run with a comma-separated list of hostnames, will copy the
# tar + gz bundle of the mstress directory to the home directory of the hosts
# and untar + unzip them for usage.
#

TAR=${TAR:-"tar"}

if [ -z "$BOOTSTRAP" ]
then
	tarfile="mstress.tgz"
	target="mstress-tarball"
else
	tarfile="mstress-bootstrap.tgz"
	target="mstress-bootstrap"
fi

if [ $# -lt 1 ]
then
	echo "Usage: $0 <comma-separated hosts>"
	echo "  This copies the mstress bundle to master and client hosts."
	exit
fi

if [ ! -f "$tarfile" ]
then
	[ -d build ] || mkdir build
	(cd build && cmake ../../.. && make "$target" && cp "benchmarks/$tarfile" ..) || exit 1
fi

while [ $# -ne 0 ]
do
	echo "Deploying mstress tarball to $1"
	ssh $1 "$TAR xzv || echo >&2 failed to untar on '$1'" < "$tarfile"
	shift
done
