#!/usr/bin/env python

#
# $Id$
#
# Author: Thilee Subramaniam
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
"""

This scrips helps to setup a simple, local deployment of the KFS servers. One
can define the servers' configuration through a setup file (eg:sample_setup.cfg
below) and run this setup to install, uninstall or upgrade the sample KFS
deployment.

eg:
./sample_setup.py -c sample_setup.cfg -a install -b ~/code/kfs/build -s ~/code/kfs
    -c: config file
    -a: action (one of install, stop, uninstall)
    -b: dist dir
    -s: source dir

Contents of sample_setup.cfg, that sets up a metaserver and two chunk servers.
---------------------------------
[metaserver]
hostname    = localhost
rundir      = ~/kfsbase/meta
clientport  = 20000
chunkport   = 20100
clusterkey  = myTestCluster

[chunkserver1]
hostname    = localhost
rundir      = ~/kfsbase/chunk1
chunkport   = 21001
space       = 700m

[chunkserver2]
hostname    = localhost
rundir      = ~/kfsbase/chunk2
chunkport   = 21002
space       = 500m

[webui]
hostname    = localhost
rundir      = ~/kfsbase/web
webport     = 22000
---------------------------------

The script sets up the servers' config files as follows:

meta-run-dir/checkpoints/
                        /logs/
                        /conf/MetaServer.prp
                        /metaserver.log
                        /metaserver.out

chunk-run-dir/chunkserver1/chunks/
                                                    /conf/ChunkServer.prp
                                                    /chunkserver.log
                                                    /chunkserver.out
                          /chunkserver2/chunks/
                                                    /conf/ChunkServer.prp
                                                    /chunkserver.log
                                                    /chunkserver.out

webui-run-dir/docroot/
                          /conf/WebUI.cfg
                          /webui.log
"""

import sys, os, os.path, shutil, errno, signal, posix, re
import ConfigParser
import subprocess

from optparse import OptionParser, OptionGroup, IndentedHelpFormatter

class Globals():
    METASERVER  = None
    CHUNKSERVER = None
    WEBSERVER   = None

def get_size_in_bytes(str):
    if not str:
        return 0
    pos = 0
    while pos < len(str) and not str[pos].isalpha():
        pos = pos + 1
    if pos >= len(str):
        return int(str)
    val = int(str[0:pos])
    unit = str[pos]
    mul = 1
    if unit in ('k', 'K'):
        mul = 1000
    elif unit in ('m', 'M'):
        mul = 1000000
    elif unit in ('g', 'G'):
        mul = 1000000000
    return val * mul

def check_binaries(releaseDir, sourceDir):
    if not os.path.exists(releaseDir + '/bin/metaserver'):
        sys.exit('Metaserver missing in build directory')
    Globals.METASERVER = releaseDir + '/bin/metaserver'

    if not os.path.exists(releaseDir + '/bin/chunkserver'):
        sys.exit('Chunkserver missing in build directory')
    Globals.CHUNKSERVER = releaseDir + '/bin/chunkserver'

    if os.path.exists(releaseDir + '/webui/kfsstatus.py'):
        Globals.WEBSERVER = releaseDir + '/webui/kfsstatus.py'
    elif os.path.exists(sourceDir + '/webui/kfsstatus.py'):
        Globals.WEBSERVER = sourceDir + '/webui/kfsstatus.py'
    else:
        sys.exit('Webserver missing in build and source directories')
    print 'Binaries presence checking - OK.'

def kill_running_program(binaryPath):
    #print 'Trying to kill instances of [ %s ] ..' % binaryPath

    if binaryPath.find('kfsstatus') >= 0:
        cmd = 'ps -ef | grep %s | grep -v grep | awk \'{print $2}\'' % binaryPath
        res = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
        pid = res[0].strip()
        if pid != '':
            os.kill(int(pid), signal.SIGTERM)
        return

    pids = subprocess.Popen(['pidof', binaryPath], stdout=subprocess.PIPE).communicate()
    for pid in pids[0].strip().split():
        os.kill(int(pid), signal.SIGTERM)

def run_command(cmd):
    return subprocess.check_call(cmd, shell=True)

# copy files & directories from src directory to dst directory. if dst does
# not exist, create it. if dst's children with same src children names exist
# then overwrite them.
def duplicate_tree(src, dst):
    if os.path.exists(dst) and not os.path.isdir(dst):
        sys.exit('Cannot duplicate directory to a non-directory')

    if not os.path.exists(dst):
        os.makedirs(dst)

    for li in os.listdir(src):
        srcPath = os.path.join(src, li)
        dstPath = os.path.join(dst, li)

        if os.path.isdir(dstPath):
            shutil.rmtree(dstPath)
        else:
            if os.path.exists(dstPath):
                os.unlink(dstPath)

        if os.path.isdir(srcPath):
            shutil.copytree(srcPath, dstPath)
        else:
            shutil.copyfile(srcPath, dstPath)

def mkdir_p(dirname):
    try:
        os.makedirs(dirname)
    except OSError, err:
        if err.errno != errno.EEXIST:
            sys.exit('Failed to create directory')
        else:
            if not os.path.isdir(dirname):
                sys.exit('% exists, but is not a directory!' % dirname)

def parse_command_line():
    action_keys = { 'install'   : True,
                    'start'     : True,
                    'stop'      : True,
                    'uninstall' : True }

    argv0Dir = os.path.dirname(sys.argv[0])

    defaultConfig = os.path.join(argv0Dir, 'sample_setup.cfg')
    defaultConfig = os.path.abspath(defaultConfig)

    defaultSrcDir = os.path.join(argv0Dir, '../..')
    defaultSrcDir = os.path.abspath(defaultSrcDir)

    defaultRelDir = os.path.join(argv0Dir, '../../build/release')
    defaultRelDir = os.path.abspath(defaultRelDir)

    if not os.path.exists(defaultRelDir):
        defaultRelDir = os.path.join(argv0Dir, '../..')

    formatter = IndentedHelpFormatter(max_help_position=50, width=120)
    usage = "usage: ./%prog [options] -a <ACTION>"
    parser = OptionParser(usage, formatter=formatter, add_help_option=False)

    parser.add_option('-c', '--config-file', action='store',
        default=defaultConfig, metavar='FILE', help='Setup config file.')

    parser.add_option('-a', '--action', action='store', default=None,
        metavar='ACTION', help='One of install, uninstall, or stop.')

    parser.add_option('-b', '--release-dir', action='store',
        default=defaultRelDir, metavar='DIR', help='QFS release directory.')

    parser.add_option('-s', '--source-dir', action='store',
        default=defaultSrcDir, metavar='DIR', help='QFS source directory.')

    parser.add_option('-h', '--help', action='store_true',
        help="Print this help message and exit.")

    actions = """
Actions:
  install   - setup meta and chunk server directories, restarting/starting them
  start     - start meta and chunk servers
  stop      - stop meta and chunk servers
  uninstall - remove meta and chunk server directories after stopping them"""

    # an install sets up all config files and (re)starts the servers.
    # an uninstall stops the servers and removes the config files.
    # a stop stops the servers.
    opts, args = parser.parse_args()

    if opts.help:
        parser.print_help()
        print actions
        print
        posix._exit(0)

    e = []
    if not os.path.isfile(opts.config_file):
        e.append("specified 'config-file' does not exist: %s" % opts.config_file)

    if not opts.action:
        e.append("'action' must be specified")
    elif not action_keys.has_key(opts.action):
        e.append("invalid 'action' specified: %s" % opts.action)

    if not os.path.isdir(opts.release_dir):
        e.append("specified 'release-dir' does not exist: %s" % opts.release_dir)

    if not os.path.isdir(opts.source_dir):
        e.append("specified 'source-dir' does not exist: %s" % opts.source_dir)

    if len(e) > 0:
        parser.print_help()
        print actions
        print
        for error in e:
            print "*** %s" % error
        print
        posix._exit(1)

    return opts

def do_cleanup(config, doUninstall):
    if config.has_section('metaserver'):
        metaDir = config.get('metaserver', 'rundir')
        if metaDir:
            kill_running_program(Globals.METASERVER)
            if doUninstall and os.path.isdir(metaDir):
                shutil.rmtree(metaDir)

    for section in config.sections():
        if section.startswith('chunkserver'):
            chunkDir = config.get(section, 'rundir')
            if chunkDir:
                kill_running_program(Globals.CHUNKSERVER)
                if doUninstall and os.path.isdir(chunkDir):
                    shutil.rmtree(chunkDir)

    if config.has_section('webui'):
        webDir = config.get('webui', 'rundir')
        if webDir:
            kill_running_program(Globals.WEBSERVER)
            if doUninstall and os.path.isdir(webDir):
                shutil.rmtree(webDir)
    if doUninstall:
        print 'Uninstall - OK.'
    else:
        print 'Stop servers - OK.'

def setup_directories(config):
    if config.has_section('metaserver'):
        metaDir = config.get('metaserver', 'rundir')
        if metaDir:
            mkdir_p(metaDir);
            mkdir_p(metaDir + '/conf')
            mkdir_p(metaDir + '/checkpoints')
            mkdir_p(metaDir + '/logs')

    for section in config.sections():
        if section.startswith('chunkserver'):
            chunkDir = config.get(section, 'rundir')
            if chunkDir:
                mkdir_p(chunkDir);
                mkdir_p(chunkDir + '/conf')
                mkdir_p(chunkDir + '/chunkdir_1')
                mkdir_p(chunkDir + '/chunkdir_2')
                mkdir_p(chunkDir + '/logs')

    if config.has_section('webui'):
        webDir = config.get('webui', 'rundir')
        if webDir:
            mkdir_p(webDir);
            mkdir_p(webDir + '/conf')
            mkdir_p(webDir + '/docroot')
    print 'Setup directories - OK.'

def setup_config_files(config):
    if 'metaserver' not in config.sections():
        sys.exit('Required metaserver section not found in config')
    metaDir = config.get('metaserver', 'rundir')
    if not metaDir:
        sys.exit('Required metaserver rundir not found in config')

    metaserverHostname = config.get('metaserver', 'hostname')
    metaserverClientPort = config.getint('metaserver', 'clientport')
    metaserverChunkPort = config.getint('metaserver', 'chunkport')
    clusterKey = config.get('metaserver', 'clusterkey')

    #metaserver
    metaFile = open(metaDir + '/conf/MetaServer.prp', 'w')
    print >> metaFile, "metaServer.clientPort = %d" % metaserverClientPort
    print >> metaFile, "metaServer.chunkServerPort = %d" % metaserverChunkPort
    print >> metaFile, "metaServer.clusterKey = %s" % clusterKey
    print >> metaFile, "metaServer.cpDir = %s/checkpoints" % metaDir
    print >> metaFile, "metaServer.logDir = %s/logs" % metaDir
    print >> metaFile, "metaServer.createEmptyFs = 1"
    print >> metaFile, "metaServer.recoveryInterval = 1"
    print >> metaFile, "metaServer.msgLogWriter.logLevel = DEBUG"
    print >> metaFile, "metaServer.msgLogWriter.maxLogFileSize = 1e6"
    print >> metaFile, "metaServer.msgLogWriter.maxLogFiles = 10"
    print >> metaFile, "metaServer.minChunkservers = 1"
    print >> metaFile, "metaServer.clientThreadCount = 4"
    print >> metaFile, "metaServer.rootDirUser = %d" % os.getuid()
    print >> metaFile, "metaServer.rootDirGroup = %d" % os.getgid()
    print >> metaFile, "metaServer.rootDirMode = 0777"
    metaFile.close()

    # chunkservers
    for section in config.sections():
        if section.startswith('chunkserver'):
            chunkClientPort = config.getint(section, 'chunkport')
            spaceStr = config.get(section, 'space')
            chunkDir = config.get(section, 'rundir')
            if chunkDir:
                chunkFile = open(chunkDir + '/conf/ChunkServer.prp', 'w')
                print >> chunkFile, "chunkServer.metaServer.hostname = %s" % metaserverHostname
                print >> chunkFile, "chunkServer.metaServer.port = %d" % metaserverChunkPort
                print >> chunkFile, "chunkServer.clientPort = %d" % chunkClientPort
                print >> chunkFile, "chunkServer.clusterKey = %s" % clusterKey
                print >> chunkFile, "chunkServer.rackId = 0"
                print >> chunkFile, "chunkServer.chunkDir = %s/chunkdir_1 %s/chunkdir_2" % (chunkDir, chunkDir)
                print >> chunkFile, "chunkServer.diskIo.crashOnError = 1"
                print >> chunkFile, "chunkServer.abortOnChecksumMismatchFlag = 1"
                print >> chunkFile, "chunkServer.msgLogWriter.logLevel = DEBUG"
                print >> chunkFile, "chunkServer.msgLogWriter.maxLogFileSize = 1e6"
                print >> chunkFile, "chunkServer.msgLogWriter.maxLogFiles = 2"
                chunkFile.close()

    # webserver
    if 'webui' not in config.sections():
        return
    webDir = config.get('webui', 'rundir')
    if not webDir:
        return
    webFile = open(webDir + '/conf/WebUI.cfg', 'w')
    print >> webFile, "[webserver]"
    print >> webFile, "webServer.metaserverHost = %s" % metaserverHostname
    print >> webFile, "webServer.metaserverPort = %d" % metaserverClientPort
    print >> webFile, "webServer.port = %d" % config.getint('webui', 'webport')
    print >> webFile, "webServer.docRoot = %s/docroot" % webDir
    print >> webFile, "webServer.allmachinesfn = /dev/null"
    print >> webFile, "webServer.displayPorts = True"
    print >> webFile, "[chunk]"
    print >> webFile, "refreshInterval = 5"
    print >> webFile, "currentSize = 30"
    print >> webFile, "currentSpan = 10"
    print >> webFile, "hourlySize = 30"
    print >> webFile, "hourlySpan =120"
    print >> webFile, "daylySize = 24"
    print >> webFile, "daylySpan = 3600"
    print >> webFile, "monthlySize = 30"
    print >> webFile, "monthlySpan = 86400"
    print >> webFile, "displayPorts = True"
    print >> webFile, "predefinedHeaders = Buffer-req-wait-usec&D-Timer-overrun-count&D-Timer-overrun-sec&XMeta-server-location&Client-active&D-Buffer-req-denied-bytes&D-CPU-sys&D-CPU-user&D-Disk-read-bytes&D-Disk-read-count&D-Disk-write-bytes&D-Disk-write-count&Write-appenders&D-Disk-read-errors&D-Disk-write-errors"
    webFile.close()
    print 'Setup config files - OK.'

def copy_files(config, sourceDir):
    # currently, only the web CSS stuff need be copied.
    if 'webui' in config.sections():
        webDir = config.get('webui', 'rundir')
        if webDir:
            webDst = webDir + '/docroot'
            webSrc = sourceDir + '/webui/files'
            duplicate_tree(webSrc, webDst)

def start_servers(config, whichServers = 'all'):
    startMeta  = whichServers in ('meta', 'all')
    startChunk = whichServers in ('chunk', 'all')
    startWeb   = whichServers in ('web', 'all')

    errors = 0

    if startMeta:
        startWeb = True
        kill_running_program(Globals.METASERVER)
        metaDir = config.get('metaserver', 'rundir')
        if metaDir:
            metaConf = metaDir + '/conf/MetaServer.prp'
            metaLog  = metaDir + '/MetaServer.log'
            metaOut  = metaDir + '/MetaServer.out'
            command = '%s -c %s %s > %s 2>&1 &' % (
                                    Globals.METASERVER,
                                    metaConf,
                                    metaLog,
                                    metaOut)
            if run_command(command) > 0:
                print "*** metaserver failed to start"
                error = 1

    if startChunk:
        kill_running_program(Globals.CHUNKSERVER)
        for section in config.sections():
            if section.startswith('chunkserver'):
                chunkDir = config.get(section, 'rundir')
                if chunkDir:
                    chunkConf = chunkDir + '/conf/ChunkServer.prp'
                    chunkLog  = chunkDir + '/ChunkServer.log'
                    chunkOut  = chunkDir + '/ChunkServer.out'
                    command = '%s %s %s > %s 2>&1 &' % (
                                            Globals.CHUNKSERVER,
                                            chunkConf,
                                            chunkLog,
                                            chunkOut)
                    if run_command(command) > 0:
                        print "*** chunkserver failed to start"
                        error = 1

    if startWeb:
        kill_running_program(Globals.WEBSERVER)
        webDir = config.get('webui', 'rundir')
        if webDir:
            webConf = webDir + '/conf/WebUI.cfg'
            webLog  = webDir + '/webui.log'
            command = '%s %s > %s 2>&1 &' % (Globals.WEBSERVER, webConf, webLog)
            if run_command(command) > 0:
                print "*** chunkserver failed to start"
                error = 1

    if errors > 0:
        print 'Started servers - FAILED.'
    else:
        print 'Started servers - OK.'

def parse_config(configFile):
    config = ConfigParser.ConfigParser()
    config.read(configFile);
    for section in config.sections():
        dir = config.get(section, 'rundir')
        config.set(section, 'rundir', os.path.expanduser(dir))
    return config

if __name__ == '__main__':
    opts = parse_command_line()
    config = parse_config(opts.config_file)

    check_binaries(opts.release_dir, opts.source_dir)

    if opts.action in ('uninstall', 'stop'):
        do_cleanup(config, opts.action == 'uninstall')
        posix._exit(0)

    setup_directories(config)
    setup_config_files(config)
    copy_files(config, opts.source_dir)
    start_servers(config)
