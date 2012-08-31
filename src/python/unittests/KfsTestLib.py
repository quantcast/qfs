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
#
# Helper functions for writing KFS test scripts
#

from __future__ import with_statement
import os,os.path,sys,getopt,time
import threading, socket
import hashlib
import ConfigParser
import constants

class ServerInfo:
    def __init__(self, n, p, rd, ra, ty):
        self.node = n
        self.port = p
        self.runDir = rd
        self.runArgs = ra
        self.serverType = ty
        self.state = "stopped"
        self.ip = socket.gethostbyname(self.node)

    def Start(self):
        if (self.state == "started"):
            return
        cmd = "ssh %s 'cd %s; scripts/kfsrun.sh -s %s ' " % \
              (self.node, self.runDir, self.runArgs)
        print cmd
        os.system(cmd)
        self.state = "started"

    def Retire(self, sleepTime):
        global testBinDir, metaServer
        cmd = "%s/tools/kfsretire -m %s -p %d -c %s -d %d" % (testBinDir, metaServer.node, metaServer.port,
                                                        self.ip, self.port)
        if sleepTime > 0:
            cmd = "%s -s %d" % (cmd, sleepTime)
        os.system(cmd)
        self.state = "retiring"
        
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
testBinDir = constants.KFS_BIN_DIR

def processConfig(config):
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
            if config.has_option(s, 'chunkDir'):
                chunkDir = config.get(s, 'chunkDir')
            else:
                chunkDir = rundir + "/bin/kfschunk"

        server = ServerInfo(node, port, rundir, runargs, type)
        server.chunkDir = chunkDir
        global chunkServers
        chunkServers.append(server)
    chunkServers.sort()

def readChunkserversFile(config, machinesFn):
    '''Given a list of chunkserver node names, one per line, construct a config
    for each chunkserver and add that to the config based on the defaults'''
    if not config.has_section('chunkserver_defaults'):
        return
    defaultChunkOptions = config.options("chunkserver_defaults")
    for l in open(machinesFn, 'r'):
        line = l.strip()
        if (line.startswith('#')):
            # ignore commented out node names
            continue
        section_name = "chunkserver_" + line
        config.add_section(section_name)
        config.set(section_name, "node", line)
        for o in defaultChunkOptions:
            config.set(section_name, o, config.get("chunkserver_defaults", o))

    config.remove_section("chunkserver_defaults")

def loadSetup():
    config = ConfigParser.ConfigParser()
    config.readfp(open(constants.SERVER_CONFIG_FILE, 'r'))
    if not config.has_section('metaserver'):
        raise config.NoSectionError, "No metaserver section"

    readChunkserversFile(config, constants.SERVER_MACHINES_FILE)
    processConfig(config)

    
def setTestBinDir(path):
    global testBinDir
    testBinDir = path

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
    # wait for the servers to get going
    time.sleep(10)

def startMetaServer():
    metaServer.Start()

def stopChunkServer(idx):
    if idx < len(chunkServers):
        chunkServers[idx].Stop()
    
def stopAllServers():
    metaServer.Stop()
    for i in xrange(len(chunkServers)):
        chunkServers[i].Stop()

def stopAllChunkServers():
    for i in xrange(len(chunkServers)):
        chunkServers[i].Stop()

def constrainNumChunkServers(num):
    for i in xrange(num, len(chunkServers)):
        chunkServers[i].Stop()

def getChunkServerIp(index):
    if index >= len(chunkServers):
        return ""
    return chunkServers[index].ip
    
def restartChunkServer(index):
    if index >= len(chunkServers):
        return
    chunkServers[index].Stop()
    time.sleep(5)
    chunkServers[index].Start()    

def retireChunkServer(index, sleepTime, shouldRestart):
    '''Retire a chunkserver; if sleepTime is specified, then it is an uppper bound on how long the
    server will be down.   shouldRestart specifies whether we should a hibernating server.'''
    if index >= len(chunkServers):
        return
    chunkServers[index].Retire(sleepTime)
    if shouldRestart is True:
        print "Retire message sent; waiting for %d" % sleepTime
        time.sleep(sleepTime - 30)
        chunkServers[index].Start()

def formatAllServers():
    metaServer.Format()
    for i in xrange(len(chunkServers)):
        chunkServers[i].Format()

def Write(kfsClnt, kfsFn, srcFn, numMB):
    """Open a data file and copy the specified # of bytes to a file in KFS"""
    kfsFp = kfsClnt.create(kfsFn)
    md5 = hashlib.md5()    
    with open(srcFn, "r") as f:
        numDone = 0
        while True:
            if numMB > 0 and numDone >= numMB:
                break
            # keep going until EOF
            data = f.read(65536)
            if len(data) == 0:
                break
            md5.update(data)            
            res = kfsFp.write(data)
            if res is not None:
                break
            numDone = numDone + len(data)
    return (numDone, md5.hexdigest())            

def Read(kfsClnt, kfsFn, srcFn, numMB):
    """Open a data file and read the specified # of bytes from a file in KFS"""
    kfsFp = kfsClnt.open(kfsFn, "r")
    numDone = 0
    md5 = hashlib.md5()
    while True:
        if numMB > 0 and numDone >= numMB:
            break
        # keep going until EOF
        data = kfsFp.read(65536)
        if len(data) == 0:
            break
        md5.update(data)
        numDone = numDone + len(data)
        if numDone % (32 * 1024 * 1024) == 0:
            # take a breather half-way into a chunk, so that we can kill processes 
            time.sleep(1)
    return (numDone, md5.hexdigest())

def dataVerify(kfsClnt, kfsFn, srcFn):
    pass
    
class IOWorker(threading.Thread):
    """Worker thread that reads/writes data from a KFS file"""
    def __init__(self, c, f, s, n, ioF):
        threading.Thread.__init__(self)
        self.kfsClnt = c
        self.kfsFn = f
        self.srcFn = s
        self.numMB = n
        self.ioFunc = ioF
        self.nDone = 0
        self.md5digest = ""

    def getMd5(self):
        return self.md5digest
    
    def getNumDone(self):
        return self.nDone
    
    def run(self):
        (self.nDone, self.md5digest) = self.ioFunc(self.kfsClnt, self.kfsFn, self.srcFn, self.numMB)
