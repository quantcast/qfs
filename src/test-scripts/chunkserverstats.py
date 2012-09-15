#!/usr/bin/python
#
# $Id$
#
# Created 2006
# Author: Sriram Rao (Kosmix Corp)
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
#

import os,sys,os.path,getopt
from ConfigParser import ConfigParser

def pingChunkserversRpc(config, bindir):
    sections = config.sections()

    minv = {}
    maxv = {}
    sumv = {}
    numservers = 0
    for s in sections:
        if (s == 'metaserver'):
            continue
        node = config.get(s, 'node')
        port = config.getint(s, 'baseport')
        cmd = "%sqfsstats -c -n 0 -s %s -p %s -t" % (bindir, node, port)
        numservers += 1
        for line in os.popen(cmd).readlines():
            if (line[0] == '-'):
                continue
            v = line.split('=')
            if (len(v) < 2):
                continue
            op = v[0]
            count = v[1].split(',')
            value = int(count[0])
            if not sumv.has_key(op):
                sumv[op] = 0
                minv[op] = value
                maxv[op] = value
            # print "%s %d" % (op, value)
            if (numservers == 1):
                minv[op] = maxv[op] = value
                sumv[op] = value
            else:
                sumv[op] += value
                if (minv[op] > value):
                    minv[op] = value
                if (maxv[op] < value):
                    maxv[op] = value

    for k in sumv.keys():
        print "%s:\t%d\t%d\t%.3f" % (k, minv[k], maxv[k], sumv[k] / (numservers + 0.0))

def pingChunkservers(config, bindir):
    sections = config.sections()

    for s in sections:
        if (s == 'metaserver'):
            continue
        node = config.get(s, 'node')
        port = config.getint(s, 'baseport')
        cmd = "%sqfsstats -c -n 0 -s %s -p %s" % (bindir, node, port)
        for line in os.popen(cmd).readlines():
            if (line.find("Net Fds") == -1):
                # this is not the header we output
                v = line.split('\t')
                if (len(v) < 2):
                    continue
                # print out the # of open net/disk fds
                print "%s\t %s %s" % (node, v[0], v[3])

if __name__ == '__main__':
    (opts, args) = getopt.getopt(sys.argv[1:], "b:f:rh",
                                 ["bin=", "file=", "rpc", "help"])
    filename = ""
    bindir = ""
    rpcstats = 0

    for (o, a) in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(2)
        if o in ("-f", "--file"):
            filename = a
        elif o in ("-b", "--bin"):
            bindir = a + "/"
        elif o in ("-r", "--rpc"):
            rpcstats = 1

    if not os.path.exists(filename):
        print "%s : directory doesn't exist\n" % filename
        sys.exit(-1)

    config = ConfigParser()
    config.readfp(open(filename, 'r'))

    if (rpcstats == 1):
        pingChunkserversRpc(config, bindir)
    else:
        pingChunkservers(config, bindir)        
    
