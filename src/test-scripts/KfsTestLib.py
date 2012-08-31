#!/usr/bin/env python
#
# $Id$
#
# Created 2006
# Author: Sriram Rao (Kosmix Corp)
#
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
# Helper functions for writing KFS test scripts
#

import os,os.path,sys,getopt,time
import threading
from ConfigParser import ConfigParser

class ServerInfo:
    def __init__(self, n, p, rd, ra, ty):
        self.node = n
        self.port = p
        self.runDir = rd
        self.runArgs = ra
        self.serverType = ty
        self.state = "stopped"

    def Start(self):
        if (self.state == "started"):
            return
        cmd = "ssh %s 'cd %s; scripts/kfsrun.sh -s %s ' " % \
              (self.node, self.runDir, self.runArgs)
        print cmd
        os.system(cmd)
        self.state = "started"
        
    def Stop(self):
        if (self.state == "stopped"):
            return
        cmd = "ssh %s 'cd %s; scripts/kfsrun.sh -S %s ' " % \
              (self.node, self.runDir, self.runArgs)
        print cmd
        os.system(cmd)
        self.state = "stopped"

    def ListChunks(self):
        cmd = "ssh %s 'ls %s ' " % (self.node, self.chunkDir)
        entries = []
        for line in os.popen(cmd):
            entries.append(line.strip())
        return entries

    def CheckLostFound(self, chunkId):
        """Check lost+found directory on chunkserver for chunkId"""
        cmd = "ssh %s 'ls %s/lost+found ' " % (self.node, self.chunkDir)
        for line in os.popen(cmd):
            dirEntry = line.strip()
            if (dirEntry == chunkId):
                return
        raise FileNotFoundException, "Chunk %s not in lost+found" % (chunkId)

    def CheckLiveness(self):
        """Check if the node is alive"""

    def CorruptChunk(self, chunkId):
        """Corrupt chunk defined by chunkId on a chunkserver"""
        filename = "/tmp/%d" % chunkId
        cmd = "scp %s:%s/%d %s" % (self.node, self.chunkDir, chunkId, filename)
        os.system(cmd)
        f = open(filename, 'r+')
        f.seek(5)
        f.write('@#$%')
        f.close()
        cmd = "scp %s %s:%s" % (filename, self.node, self.chunkDir)
        os.system(cmd)

    def Format(self):
        """Delete chunks/log files on this server"""
        if (self.serverType == "metaserver"):
            cmd = "ssh %s 'rm -f %s/bin/kfscp/* %s/bin/kfslog/*' " % (self.node, self.runDir, self.runDir)
            os.system(cmd)
        else:
            cmd = "ssh %s 'rm -f %s/* %s/bin/kfslog/*' " % (self.node, self.chunkDir, self.runDir)
            os.system(cmd)

    def __cmp__(self, other):
        if (self.node == other.node):
            return cmp(self.port, other.port)
        return cmp(self.node, other.node)

    def __str__(self):
        return '[%s] %s:%d' % (self.serverType, self.node, self.port)

metaServer = ""
chunkServers = []
# this is the path to the directory containing test binaries
testBinDir = "."

def loadSetup(filename):
    config = ConfigParser()
    config.readfp(open(filename, 'r'))
    if not config.has_section('metaserver'):
        raise config.NoSectionError, "No metaserver section"
    
    sections = config.sections()

    # read the meta-server separately
    for s in sections:
        node = config.get(s, 'node')
        rundir = config.get(s, 'rundir')
        port = int(config.get(s, 'baseport'))
        if (s == 'metaserver'):
            runargs = "-m -f bin/MetaServer.prp"
            type = "metaserver"
            global metaServer
            metaServer = ServerInfo(node, port, rundir, runargs, type)
            continue
        else:
            runargs = "-c -f bin/ChunkServer.prp"
            type = "chunkserver"
            chunkDir = config.get(s, 'chunkDir')

        server = ServerInfo(node, port, rundir, runargs, type)
        server.chunkDir = chunkDir
        global chunkServers
        chunkServers.append(server)
    chunkServers.sort()

def setTestBinDir(path):
    global testBinDir
    testBinDir = path

def doWrite(kfsClntPrpFn, fn, numMB):
    cmd = "%s/writer_perftest -p %s -m %d -f %s" % (testBinDir, kfsClntPrpFn, numMB, fn)
    print "Executing cmd: %s" % cmd
    os.system(cmd)

def doRead(kfsClntPrpFn, fn, numMB):
    cmd = "%s/reader_perftest -p %s -m %d -f %s" % (testBinDir, kfsClntPrpFn, numMB, fn)
    os.system(cmd)

def pruneSetup(numServers):
    global chunkServers
    if (len(chunkServers) > numServers):
        chunkServers = chunkServers[:numServers]

def startServers(numServers):
    for i in range(numServers):
        chunkServers[i].Start()

def startAllServers():
    metaServer.Start()    
    startServers(len(chunkServers))

def startMetaServer():
    metaServer.Start()
    
def stopAllServers():
    metaServer.Stop()
    for i in xrange(len(chunkServers)):
        chunkServers[i].Stop()

def stopAllChunkServers():
    for i in xrange(len(chunkServers)):
        chunkServers[i].Stop()

def restartChunkServer(index):
    chunkServers[index].Stop()
    time.sleep(5)
    chunkServers[index].Start()    

def formatAllServers():
    metaServer.Format()
    for i in xrange(len(chunkServers)):
        chunkServers[i].Format()

        
    
class IOWorker(threading.Thread):
    """Worker thread that reads/writes data from a KFS file"""
    def __init__(self, c, f, n, ioF):
        threading.Thread.__init__(self)
        self.kfsClntPrpFn = c
        self.kfsFn = f
        self.numMB = n
        self.ioFunc = ioF

    def run(self):
        self.ioFunc(self.kfsClntPrpFn, self.kfsFn, self.numMB)
        
