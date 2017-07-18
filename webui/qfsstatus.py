#!/usr/bin/env python
#
# $Id$
#
# Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
#
# Author: Sriram Rao (Quantcast Corp.)
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
# A simple webserver that displays KFS status by pinging the metaserver.
#

import os,sys,os.path,getopt
import socket,threading,calendar,time
from datetime import datetime
import SimpleHTTPServer
import SocketServer
from cStringIO import StringIO
from ConfigParser import ConfigParser
import urllib
import platform
from chunks import ChunkThread, ChunkDataManager, HtmlPrintData, HtmlPrintMetaData, ChunkArrayData, ChunkServerData
from chart import ChartData, ChartServerData, ChartHTML
from browse import QFSBrowser
import threading

gHasCollections = True
try:
    import collections
    collections.OrderedDict()
except:
    sys.stderr.write("Warning: '%s'. Proceeding without collections.\n" % str(sys.exc_info()[1]))
    gHasCollections = False

gJsonSupported = True
try:
    import json
    class SetEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, set):
                return list(obj)
            return json.JSONEncoder.default(self, obj)
except ImportError:
    sys.stderr.write("Warning: '%s'. Proceeding without query support.\n" % str(sys.exc_info()[1]))
    gJsonSupported = False

metaserverPort = 20000
metaserverHost='127.0.0.1'
spaceTotal=0
docRoot = '.'
displayName = ''
autoRefresh = 60
displayPorts = False
displayChunkServerStorageTiers = True
myWebserverPort=20001
objectStoreMode = False

kServerName="XMeta-location" #todo - put it to config file
kChunkDirName="Chunk-server-dir"
kChunks=1
kMeta=2
kChart=3
kBrowse=4
kChunkDirs=5
cMeta=6
kConfig=7
kVrStatus=8

kHtmlEscapeTable = {
    "&": "&amp;",
    '"': "&quot;",
    "'": "&apos;",
    ">": "&gt;",
    "<": "&lt;",
}

def htmlEscape(text):
    """Produce entities within text."""
    return "".join(kHtmlEscapeTable.get(c,c) for c in text)

def showRate(rate, div):
    if rate < 100 * div:
        return '%.2f' % (rate / float(div))
    return splitThousands(rate / div)

def showUptime(uptime):
    seconds = uptime % 60
    rem     = uptime / 60
    minutes = rem % 60
    rem     = rem / 60
    hours   = rem % 24
    days    = rem / 24
    return '%d&nbsp;days,&nbsp;%02d:%02d:%02d' % (days, hours, minutes, seconds)

class ServerLocation:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)
        self.status = 0

class SystemInfo:
    def __init__(self):
        self.startedAt = ""
        self.totalSpace = 0
        self.usedSpace = 0
        self.wormMode = "Disabled"
        self.buildVersion = ''
        self.sourceVersion = ''
        self.replications = -1;
        self.pendingRecovery = -1
        self.uptime = -1
        self.replicationsCheck = -1
        self.pendingReplication = -1
        self.usedBuffers = -1
        self.clients = -1
        self.chunkServers = -1
        self.allocatedRequests = -1
        self.sockets = -1
        self.chunks = -1
        self.internalNodes = -1
        self.internalNodeSize =  -1
        self.internalNodeAllocSize =  -1
        self.dentries =  -1
        self.dentrySize =  -1
        self.dentryAllocSize =  -1
        self.fattrs =  -1
        self.fattrSize =  -1
        self.fattrAllocSize =  -1
        self.cinfos =  -1
        self.cinfoSize =  -1
        self.cinfoAllocSize = -1
        self.csmapNodes = -1
        self.csmapNodeSize = -1
        self.csmapAllocSize = -1
        self.csmapEntryAllocs = -1
        self.csmapEntryBytes = -1
        self.delayedRecovery = -1
        self.replicationBacklog = -1
        self.isInRecovery = False
        self.csToRestart = -1
        self.csMastersToRestart = -1
        self.csMaxGoodCandidateLoadAvg = -1
        self.csMaxGoodMasterLoadAvg = -1
        self.csMaxGoodSlaveLoadAvg = -1
        self.hibernatedServerCount = -1
        self.freeFsSpace = -1
        self.goodMasters = -1
        self.goodSlaves = -1
        self.totalDrives = -1
        self.writableDrives = 1
        self.appendCacheSize = -1
        self.maxClients = -1
        self.maxChunkServers = -1
        self.totalBuffers = -1
        self.objStoreEnabled = -1
        self.objStoreDeletes = -1
        self.objStoreDeletesInFlight = -1
        self.objStoreDeletesRetry = -1
        self.objStoreDeletesStartedAgo = -1
        self.fileCount = -1
        self.dirCount = -1
        self.sumOfLogicalFileSizes = -1
        self.fileSystemId = -1
        self.vrPrimaryFlag = 0
        self.vrNodeId = -1
        self.vrPrimaryNodeId = -1
        self.vrActiveFlag = 0
        self.logTimeUsec = -1
        self.logTimeOpsCount = -1
        self.logPendingOpsCount = -1
        self.log5SecAvgUsec = -1
        self.log10SecAvgUsec = -1
        self.log15SecAvgUsec = -1
        self.log5SecAvgReqRate = -1
        self.log10SecAvgReqRate = -1
        self.log15SecAvgReqRate = -1
        self.logAvgReqRateDiv = 1
        self.bTreeHeight = -1
        self.logDiskWriteUsec = -1
        self.logDiskWriteByteCount = -1
        self.logDiskWriteCount = -1
        self.logOpWrite5SecAvgUsec = -1
        self.logOpWrite10SecAvgUsec = -1
        self.logOpWrite15SecAvgUsec = -1
        self.logExceedQueueDepthFailedCount = 0
        self.logPendingAckByteCount = 0
        self.logTotalRequestCount = -1
        self.logExceedLogQueueDepthFailureCount300SecAvg = 0

class Status:
    def __init__(self):
        self.upServers = {}
        self.downServers = {}
        self.retiringServers = {}
        self.evacuatingServers = {}
        self.serversByRack = {}
        self.numReallyDownServers = 0
        self.freeFsSpace = 0
        self.canNotBeUsedForPlacment = 0
        self.goodNoRackAssignedCount = 0
        self.tiersColumnNames = {}
        self.tiersInfo = {}
        self.config = {}
        self.vrStatus = {}
        self.systemInfo = SystemInfo()

    def systemStatus(self, buffer):
        self.display(
            buffer,
            self.upServers,
            self.downServers,
            self.retiringServers,
            self.evacuatingServers,
            self.serversByRack,
            self.numReallyDownServers,
            self.freeFsSpace,
            self.canNotBeUsedForPlacment,
            self.goodNoRackAssignedCount,
            self.systemInfo,
            self.tiersColumnNames,
            self.tiersInfo,
            self.config,
            self.vrStatus
        )

    def display(
            self,
            buffer,
            upServers,
            downServers,
            retiringServers,
            evacuatingServers,
            serversByRack,
            numReallyDownServers,
            freeFsSpace,
            canNotBeUsedForPlacment,
            goodNoRackAssignedCount,
            systemInfo,
            tiersColumnNames,
            tiersInfo,
            config,
            vrStatus
        ) :
        global gQfsBrowser
        rows = ''
        if gQfsBrowser.browsable:
            browseLink = '<A href="/browse-it">Browse Filesystem</A>'
        else:
            browseLink = ''
        print >> buffer, '''
    <body class="oneColLiqCtr">
    <div id="container">
      <div id="mainContent">
        <h1> QFS Status ''', displayName, '''</h1>
        <P>
            <A href="/chunk-it">Chunk Servers Status</A>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            <A href="/meta-it">Meta Server Status</A>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            <A href="/chunkdir-it">Chunk Directories Status</A>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            <A href="/meta-conf-html">Meta Server Configuration</A>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'''
        if 0 <= systemInfo.vrNodeId:
            print >> buffer, '''
            <A href="/meta-vr-status-html">Meta Server Viewstamped Replication Status</A>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'''
        print >> buffer, '''
            %s
        </P>
        <div class="info-table">
        <table cellspacing="0" cellpadding="0.1em">
        <tbody>''' % browseLink

        if systemInfo.isInRecovery and 0 != systemInfo.vrPrimaryFlag:
            print >> buffer, '''<tr><td>Recovery status: </td><td>:</td><td>IN RECOVERY</td></tr>'''
        fsFree = systemInfo.freeFsSpace
        if fsFree < 0:
            fsFree = freeFsSpace
        if systemInfo.totalSpace > 0:
            freePct = fsFree * 100. / float(systemInfo.totalSpace)
        else:
            freePct = 0.
        serverCount = len(upServers)
        print >> buffer, '''
        <tr> <td> Updated </td><td>:</td><td> ''', time.strftime("%a %b %d %H:%M:%S %Y"), ''' </td></tr>
        <tr> <td> Started at </td><td>:</td><td> ''', systemInfo.startedAt + \
                '&nbsp;uptime:&nbsp;' + showUptime(systemInfo.uptime), ''' </td></tr>'''
        if 0 != systemInfo.vrPrimaryFlag:
            print >> buffer, '''
            <tr> <td> Space </td><td>:</td><td> total:&nbsp;''' + bytesToReadable(systemInfo.totalSpace) + \
                '&nbsp;used:&nbsp;' + bytesToReadable(systemInfo.usedSpace) + \
                '&nbsp;free:&nbsp;' + bytesToReadable(fsFree) + ('&nbsp;%.2f%%' % freePct) + ' </td></tr>'
        print >> buffer, '''
        <tr> <td> WORM mode </td><td>:</td><td> ''', systemInfo.wormMode, '''</td></tr>'''
        if 0 < systemInfo.fileSystemId:
            print >> buffer, '<tr> <td> File system </td><td>:</td><td>directories:&nbsp;' + \
                splitThousands(systemInfo.dirCount) + \
                '&nbsp;files:&nbsp;' + splitThousands(systemInfo.fileCount) + \
                '&nbsp;sum&nbsp;of&nbsp;logical&nbsp;file&nbsp;sizes:&nbsp;' + \
                     bytesToReadable(systemInfo.sumOfLogicalFileSizes) + \
                '&nbsp;ID:&nbsp;' + str(systemInfo.fileSystemId) + \
                '</td></tr>'
        if 0 < systemInfo.objStoreEnabled:
            print >> buffer, '<tr> <td> Object store delete queue</td><td>:</td><td>size:&nbsp;' + \
                splitThousands(systemInfo.objStoreDeletes) + \
                '&nbsp;in&nbsp;flight:&nbsp;' + splitThousands(systemInfo.objStoreDeletesInFlight) + \
                '&nbsp;re-queue:&nbsp;' + splitThousands(systemInfo.objStoreDeletesRetry) + \
                '&nbsp;frst&nbsp;queued:&nbsp;' + str(systemInfo.objStoreDeletesStartedAgo) + \
                '&nbsp;seconds&nbsp;ago</td></tr>'
        if 0 <= systemInfo.logPendingOpsCount:
            if systemInfo.logTimeOpsCount:
                avg = systemInfo.logTimeUsec / systemInfo.logTimeOpsCount
                opWriteAvg = systemInfo.logDiskWriteUsec / systemInfo.logTimeOpsCount
            else:
                avg = 0
                opWriteAvg = 0
            if 0 < systemInfo.uptime:
                rate = systemInfo.logTimeOpsCount * systemInfo.logAvgReqRateDiv / systemInfo.uptime
            else:
                rate = 0
            if 0 < systemInfo.logTotalRequestCount:
                droppedPct = 100. * systemInfo.logExceedQueueDepthFailedCount / systemInfo.logTotalRequestCount
            else:
                droppedPct = 0.
            print >> buffer, '<tr> <td> Transaction log </td><td>:</td><td>' + \
                'queue&nbsp;depth:&nbsp;' + splitThousands(systemInfo.logPendingOpsCount) + \
                "/" + bytesToReadable(systemInfo.logPendingAckByteCount) + \
                ";&nbsp;dropped:&nbsp;" + splitThousands(systemInfo.logExceedLogQueueDepthFailureCount300SecAvg) + \
                "/%.2e%%" % droppedPct + \
                ';&nbsp;request&nbsp;rate&nbsp;&amp;&nbsp;time&nbsp;usec.&nbsp;total/disk' + \
                '&nbsp;[5;&nbsp;10;&nbsp;15&nbsp;sec.;&nbsp;total&nbsp;averages]:' + \
                '&nbsp;'    + showRate(systemInfo.log5SecAvgReqRate, systemInfo.logAvgReqRateDiv) + \
                '&nbsp;'    + splitThousands(systemInfo.log5SecAvgUsec) + \
                '/'         + splitThousands(systemInfo.logOpWrite5SecAvgUsec) + \
                ';&nbsp;'   + showRate(systemInfo.log10SecAvgReqRate, systemInfo.logAvgReqRateDiv) + \
                '&nbsp;'    + splitThousands(systemInfo.log10SecAvgUsec) + \
                '/'         + splitThousands(systemInfo.logOpWrite10SecAvgUsec) + \
                ';&nbsp;'   + showRate(systemInfo.log15SecAvgReqRate, systemInfo.logAvgReqRateDiv) + \
                '&nbsp;'    + splitThousands(systemInfo.log15SecAvgUsec) + \
                '/'         + splitThousands(systemInfo.logOpWrite15SecAvgUsec) + \
                ';&nbsp;'   + showRate(rate, systemInfo.logAvgReqRateDiv) + \
                '&nbsp;'    + splitThousands(avg) + \
                '/'         + splitThousands(opWriteAvg) + \
                '</td></tr>'
        print >> buffer, '''<tr> <td> Meta server viewstamped replication (VR) </td><td>:</td><td> '''
        if systemInfo.vrNodeId < 0 or len(vrStatus) <= 0:
            print >> buffer, '''not&nbsp;configured'''
        else:
            textBuf = ''
            try:
                textBuf = 'node&nbsp;id:&nbsp;' + vrStatus['vr.nodeId'] + \
                   '&nbsp;state:&nbsp;' + htmlEscape(vrStatus['vr.state'])
                if '0' == vrStatus['vr.active']:
                   textBuf = textBuf + '&nbsp;inactive'
                else:
                    status = long(vrStatus['vr.status'])
                    if 0 != status:
                        primaryId = vrStatus['vr.primaryId']
                        textBuf = textBuf + '&nbsp;primary&nbsp;node&nbsp;id:&nbsp;' + primaryId
                        for k in vrStatus:
                            if k.startswith('configuration.node.') and k.endswith('.id') and vrStatus[k] == primaryId:
                                try:
                                    host = vrStatus[k.replace('.id', '.listener')].split()[0]
                                    textBuf += '&nbsp;host:&nbsp;<A href="http://'
                                    textBuf += host
                                    textBuf += ':'
                                    textBuf += str(myWebserverPort)
                                    textBuf += '/">'
                                    textBuf +=  host
                                    textBuf += '</A>'
                                except:
                                    pass
                    try:
                        viewTime = long(vrStatus['vr.currentTime']) - long(vrStatus['vr.viewChangeStartTime'])
                        textBuf += '&nbsp;view started:&nbsp;'
                        textBuf += showUptime(viewTime)
                        textBuf += '&nbsp;ago&nbsp;reason:&nbsp;'
                        textBuf += htmlEscape(vrStatus['vr.viewChangeReason'])
                    except:
                        pass
                    if 0 == status:
                        try:
                            textBuf += '&nbsp;up&nbsp;nodes:&nbsp;'
                            textBuf += vrStatus['logTransmitter.activeUpNodesCount']
                            textBuf += '&nbsp;channels:&nbsp;'
                            textBuf += vrStatus['logTransmitter.activeUpChannelsCount']
                        except:
                            pass
                print >> buffer, textBuf
            except:
                print >> buffer, '''VR&nbsp;status&nbsp;parse&nbsp;errror'''
        print >> buffer, '''</td></tr>
        <tr> <td> Chunk servers</td><td>:</td><td> alive:&nbsp;''' + splitThousands(serverCount) + \
                '''&nbsp;dead:&nbsp;''' + splitThousands(numReallyDownServers) + \
                '''&nbsp;retiring:&nbsp;''' + splitThousands(len(retiringServers))
        if systemInfo.hibernatedServerCount >= 0:
            print >> buffer, '''&nbsp;hibernated:&nbsp;''' + splitThousands(systemInfo.hibernatedServerCount)
        print >> buffer, '''</td></tr>'''
        if systemInfo.replications >= 0:
            print >> buffer, '''<tr> <td> Replications </td><td>:</td><td>in&nbsp;flight:&nbsp;''' + \
                str(systemInfo.replications) + \
                '''&nbsp;check:&nbsp;''' + splitThousands(systemInfo.replicationsCheck) + \
                '''&nbsp;pending:&nbsp;''' + splitThousands(systemInfo.pendingReplication) + \
                '''&nbsp;recovery:&nbsp;''' + splitThousands(systemInfo.pendingRecovery) + \
                '''&nbsp;delayed:&nbsp;''' + splitThousands(systemInfo.delayedRecovery) + \
                '''&nbsp;backlog:&nbsp;''' + splitThousands(systemInfo.replicationBacklog) + \
                '''</td></tr>'''
        if systemInfo.clients >= 0:
            print >> buffer, \
                '''<tr> <td> Allocations </td><td>:</td><td>clients:&nbsp;''' + \
                splitThousands(systemInfo.clients)
            if 0 <= systemInfo.maxClients:
                print >> buffer, \
                    '''&nbsp;(max:&nbsp;''' + splitThousands(systemInfo.maxClients) + ')'
            print >> buffer, \
                '''&nbsp;chunk&nbsp;servers:&nbsp;''' + splitThousands(systemInfo.chunkServers)
            if 0 <= systemInfo.maxChunkServers:
                print >> buffer, \
                    '''&nbsp;(max:&nbsp;''' + splitThousands(systemInfo.maxChunkServers) + ')'
            print >> buffer, \
                '''&nbsp;requests:&nbsp;''' + splitThousands(systemInfo.allocatedRequests) + \
                '''&nbsp;buffers:&nbsp;''' + splitThousands(systemInfo.usedBuffers)
            if 0 <= systemInfo.totalBuffers:
                print >> buffer, \
                    '''&nbsp;(max:&nbsp;''' + splitThousands(systemInfo.totalBuffers) + ')'
            print >> buffer, \
                '''&nbsp;sockets:&nbsp;''' + splitThousands(systemInfo.sockets) + \
                '''&nbsp;chunks:&nbsp;''' + splitThousands(systemInfo.chunks)
            if systemInfo.appendCacheSize >= 0:
                print >> buffer, \
                    '''&nbsp;append cache:&nbsp;''' + splitThousands(systemInfo.appendCacheSize)
            print >> buffer, '''</td></tr>'''
        if systemInfo.internalNodes >= 0:
            print >> buffer, '''<tr> <td> Allocations&nbsp;b+tree</td><td>:</td><td>internal:&nbsp;''' + \
                splitThousands(systemInfo.internalNodes) + \
                '''x''' + splitThousands(systemInfo.internalNodeSize) + \
                '''&nbsp;''' + bytesToReadable(systemInfo.internalNodeAllocSize) + \
                '''&nbsp;dent:&nbsp;''' + splitThousands(systemInfo.dentries) + \
                '''x''' + splitThousands(systemInfo.dentrySize) + \
                '''&nbsp;''' + bytesToReadable(systemInfo.dentryAllocSize) + \
                '''&nbsp;fattr:&nbsp;''' + splitThousands(systemInfo.fattrs) + \
                '''x''' + splitThousands(systemInfo.fattrSize) + \
                '''&nbsp;''' + bytesToReadable(systemInfo.fattrAllocSize) + \
                '''&nbsp;cinfo:&nbsp;''' + splitThousands(systemInfo.cinfos) + \
                '''x''' + splitThousands(systemInfo.cinfoSize) + \
                '''&nbsp;''' + bytesToReadable(systemInfo.cinfoAllocSize) + \
                '''&nbsp;tree&nbsp;height:&nbsp;''' + splitThousands(systemInfo.bTreeHeight) + \
                '''</td></tr>'''
        if systemInfo.csmapNodes >= 0:
            print >> buffer, '''<tr> <td> Allocations&nbsp;chunk2server</td><td>:</td><td>nodes:&nbsp;''' + \
                splitThousands(systemInfo.csmapNodes) + \
                '''x''' + splitThousands(systemInfo.csmapNodeSize) + \
                '''&nbsp;''' + bytesToReadable(systemInfo.csmapAllocSize) + \
                '''&nbsp;srv&nbsp;list:&nbsp;''' + splitThousands(systemInfo.csmapEntryAllocs) + \
                '''&nbsp;''' + bytesToReadable(systemInfo.csmapEntryBytes) + \
                '''</td></tr>'''
        allGood = 0
        if 0 != systemInfo.vrPrimaryFlag:
            if systemInfo.csMaxGoodCandidateLoadAvg >= 0:
                print >> buffer, '''<tr> <td>Chunk&nbsp;placement&nbsp;load&nbsp;threshold</td><td>:</td><td>''' + \
                    'avg:&nbsp;%5.2e' % systemInfo.csMaxGoodCandidateLoadAvg + '&nbsp;' + \
                    '&nbsp;master:&nbsp;%5.2e' % systemInfo.csMaxGoodMasterLoadAvg + \
                    '&nbsp;slave:&nbsp;%5.2e' % systemInfo.csMaxGoodSlaveLoadAvg + \
                    '''</td></tr>'''
            if serverCount <= 0:
                mult = 0
            else:
                mult = 100. / float(serverCount)
            print >> buffer, '''<tr> <td>Chunk&nbsp;placement&nbsp;candidates</td><td>:</td><td>'''
            if systemInfo.goodMasters >= 0 and systemInfo.goodSlaves >= 0:
                allGood = systemInfo.goodMasters + systemInfo.goodSlaves
                print >> buffer, \
                    'all:&nbsp;' + splitThousands(allGood) + \
                        '&nbsp;%.2f%%' % (float(allGood) * mult) + \
                    '&nbsp;masters:&nbsp;' + splitThousands(systemInfo.goodMasters) + \
                        '&nbsp;%.2f%%' % (float(systemInfo.goodMasters) * mult) + \
                    '&nbsp;slaves:&nbsp' + splitThousands(systemInfo.goodSlaves) + \
                        '&nbsp;%.2f%%' % (float(systemInfo.goodSlaves) * mult)
            else:
                allGood = serverCount - canNotBeUsedForPlacment
                print >> buffer, \
                    'all:&nbsp;' + splitThousands(allGood) + \
                        '&nbsp;%.2f%%' % (float(allGood) * mult)
            if goodNoRackAssignedCount < allGood:
                all = allGood - goodNoRackAssignedCount
                print >> buffer, \
                    '&nbsp;in&nbsp;racks:&nbsp;' + splitThousands(all) + \
                        '&nbsp;%.2f%%' % (float(all) * mult)
            print >> buffer, '''</td></tr>'''
            if systemInfo.totalDrives >= 0 and systemInfo.writableDrives >= 0:
                if systemInfo.totalDrives > 0:
                    mult = 100. / systemInfo.totalDrives
                else:
                    mult = 0.
                print >> buffer, \
                    '''<tr> <td>Storage devices&nbsp;</td><td>:</td><td>''' + \
                   'total:&nbsp;' + splitThousands(systemInfo.totalDrives) + \
                    '&nbsp;writable:&nbsp;' + splitThousands(systemInfo.writableDrives) + \
                        '&nbsp;%.2f%%' % (float(systemInfo.writableDrives) * mult) + \
                    '&nbsp;avg&nbsp;capacity:&nbsp;' + \
                        bytesToReadable(systemInfo.totalSpace * mult / 100.), '''</td></tr>'''

        print >> buffer, '''
        <tr><td>Version </td><td>:</td><td> ''', systemInfo.buildVersion, '''</td></tr>
        <tr><td>Source </td><td>:</td><td> ''',  systemInfo.sourceVersion, '''</td></tr>
        </tbody>
        </table>
        </div>
        <br />
        '''

        if len(evacuatingServers) > 0:
            print >> buffer, '''
            <div class="floatleft">
             <table class="sortable status-table" id="tableEvacuating" cellspacing="0" cellpadding="0.1em"
                summary="Status of evacuating nodes in the system">
             <caption> <a name="EvacuatingNodes">Evacuating Nodes Status</a> </caption>
             <thead>
             <tr>
             <th>Chunkserver</th>
             <th>Blocks Done</th>
             <th>Bytes Done</th>
             <th>Blocks Left</th>
             <th>Bytes Left</th>
             <th>Queue Chunk Srv</th>
             <th>Queue Meta Srv</th>
             <th>Blocks/Sec</th>
             <th>Bytes/Sec</th>
             <th>ETA Min</th>
             </tr>
             </thead>
             <tbody>
            '''
            count = 0
            for v in evacuatingServers:
                v.printStatusHTML(buffer)
                count = count + 1
            print >> buffer, '''
            </tbody>
            </table></div>'''

        colCount = len(tiersColumnNames)
        if colCount > 0 and len(tiersInfo) >= colCount:
            print >> buffer, '''
            <div class="floatleft">
             <table class="sortable status-table" id="tiersInfo" cellspacing="0" cellpadding="0.1em"
                summary="Status of storage tiers in the system">
             <caption> <a name="StorageTiers">Storage Tiers Available For Placement Status</a> </caption>
             <thead>
             <tr>
            '''
            conv = {}
            colCnt = 0
            for col in tiersColumnNames:
                conv[colCnt] = ''
                if col == '%util.':
                    col = 'used%'
                    conv[colCnt] = '%.2f'
                elif col == 'space-available':
                    col = 'free'
                    conv[colCnt] = '%.2e'
                elif col == 'total-space':
                    col = 'total'
                    conv[colCnt] = '%.2e'
                elif col == 'devices':
                    col = 'writable dev.'
                elif col == 'wr-chunks':
                    col = 'writable blocks'
                elif col == 'chunks':
                    col = 'blocks'
                colCnt = colCnt + 1
                print >> buffer, '''<th>''', col.capitalize(), '''</th>'''
            print >> buffer, '''
             </tr>
             </thead>
             <tbody>
            '''
            rowCnt  = 0
            colCnt  = 0
            trclass = ''
            for val in tiersInfo:
                if colCnt == 0:
                    print >> buffer, '''<tr>'''
                if  conv[colCnt] == 's':
                    v = bytesToReadable(val)
                elif conv[colCnt] == '':
                    v = val
                else:
                    v = conv[colCnt] % float(val)
                print >> buffer, '''<td align="right">''', v, '''</td>'''
                colCnt = colCnt + 1
                if colCnt >= colCount:
                    print >> buffer, '''</tr>'''
                    colCnt = 0
                    rowCnt = rowCnt + 1
            if colCnt > 0:
                while colCnt < colCount:
                    colCnt = colCnt + 1
                    print >> buffer, '''<td> </td>'''
                print >> buffer, '''</tr>'''
            print >> buffer, '''
            </tbody>
            </table>
            </div>
            '''

        print >> buffer, '''
        <div class="floatleft">
         <table class="sortable status-table" id="table1" cellspacing="0" cellpadding="0.1em"
            summary="Status of nodes in the system: who is up/down and when we last heard from them">
         <caption> All Nodes </caption>
         <thead>
         <tr>
         <th>Chunkserver</th>
         <th># dev.</th>
         <th>Writable dev.</th>
         <th>Wr. blocks</th>
         <th>Tiers</th>
         <th>Used</th>
         <th>Free</th>
         <th>Total</th>
         <th>Used%</th>
         <th>Blocks</th>
         <th>Chunks</th>
         <th>Last heard</th>
         <th>Repl. in</th>
         <th>Repl. out</th>
         <th>IOErr</th>
         <th>Load</th>
         <th>Rack</th>
         </tr>
         </thead>
         <tbody>
        '''
        count = 0
        showNoRack = goodNoRackAssignedCount < allGood
        for v in upServers:
            v.printStatusHTML(buffer, count, showNoRack)
            count += 1
        print >> buffer, '''
        </tbody>
        </table></div>'''

        if len(retiringServers) > 0:
            print >> buffer, '''
            <div class="floatleft">
             <table class="status-table" cellspacing="0" cellpadding="0.1em" summary="Status of retiring nodes in the system">
             <caption> <a name="RetiringNodes">Retiring Nodes Status</a> </caption>
             <thead>
             <tr><th> Chunkserver </th> <th> Start </th> <th>  # blks done </th> <th> # blks left </th> </tr>
             </thead>
             <tbody>
            '''
            count = 0
            for v in retiringServers:
                v.printStatusHTML(buffer, count)
                count = count + 1
            print >> buffer, '''
            </tbody>
            </table></div>'''

        if len(downServers) > 0:
            print >> buffer, '''<div class="floatleft">
            <table class="status-table" cellspacing="0" cellpadding="0.1em" summary="Status of down nodes in the system">
            <caption> <a name="DeadNodes">Dead Nodes History</a></caption>
         <thead>
            <tr><th> Chunkserver </th> <th> Down Since </th> <th> Reason </th> </tr>
         </thead>
         <tbody>
            '''
            count = 0
            for v in downServers:
                v.printStatusHTML(buffer, count)
                count = count + 1
            print >> buffer, '''
            </tbody>
            </table></div>'''

        print >> buffer, '''
        </div>
        </div>
        </body>
        </html>'''

# beginning of html
def printStyle(buffer, title):
    print >> buffer, '''
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<link rel="stylesheet" type="text/css" href="files/qfsstyle.css">
<script type="text/javascript" src="files/sorttable/sorttable.js"></script>
<title>''',  title, displayName, '''</title>
</head>
'''

class DownServer:
    """Keep track of a potentially down server"""
    def __init__(self, info):
        serverInfo = info.split(',')
        for i in xrange(len(serverInfo)):
            s = serverInfo[i].split('=')
            setattr(self, s[0].strip(), s[1].strip())

        if hasattr(self, 's'):
            setattr(self, 'host', self.s)
            delattr(self, 's')

        if hasattr(self, 'p'):
            setattr(self, 'port', self.p)
            delattr(self, 'p')

        if hasattr(self, 'host'):
            self.displayName = self.host
        else:
            self.displayName = 'unknown'
        if displayPorts and hasattr(self, 'port'):
            self.displayName += ':' + str(self.port)

        self.stillDown = 0

    def __cmp__(self, other):
        """Order by down date"""
        return cmp(time.strptime(other.down), time.strptime(self.down))

    def setStillDown(self):
        self.stillDown = 1

    def printStatusHTML(self, buffer, count):
        if count % 2 == 0:
            trclass = ""
        else:
            trclass = "class=odd"

        if self.stillDown:
            trclass = "class=dead"

        print >> buffer, '''<tr ''', trclass, '''><td align="center">''', self.displayName, '''</td>'''
        print >> buffer, '''<td>''', self.down, '''</td>'''
        print >> buffer, '''<td>''', self.reason, '''</td>'''
        print >> buffer, '''</tr>'''

class RetiringServer:
    """Keep track of a retiring server"""
    def __init__(self, info):
        serverInfo = info.split(',')
        for i in xrange(len(serverInfo)):
            s = serverInfo[i].split('=')
            setattr(self, s[0].strip(), s[1].strip())

        if hasattr(self, 's'):
            setattr(self, 'host', self.s)
            delattr(self, 's')

        if hasattr(self, 'p'):
            setattr(self, 'port', self.p)
            delattr(self, 'p')

        if hasattr(self, 'host'):
            self.displayName = self.host
        else:
            self.displayName = 'unknown'
        if displayPorts and hasattr(self, 'port'):
            self.displayName += ':' + str(self.port)

    def __cmp__(self, other):
        """Order by start date"""
        return cmp(time.strptime(other.started), time.strptime(self.started))

    def printStatusHTML(self, buffer, count):
        if count % 2 == 0:
            trclass = ""
        else:
            trclass = "class=odd"

        print >> buffer, '''<tr ''', trclass, '''><td align="center">''', self.displayName, '''</td>'''
        print >> buffer, '''<td>''', self.started, '''</td>'''
        print >> buffer, '''<td align="right">''', self.numDone, '''</td>'''
        print >> buffer, '''<td align="right">''', self.numLeft, '''</td>'''
        print >> buffer, '''</tr>'''

class EvacuatingServer:
    """Keep track of a evacuating server"""
    def __init__(self, info):
        serverInfo = info.split(',')
        for i in xrange(len(serverInfo)):
            s = serverInfo[i].split('=')
            setattr(self, s[0].strip(), s[1].strip())

        if hasattr(self, 's'):
            setattr(self, 'host', self.s)
            delattr(self, 's')

        if hasattr(self, 'p'):
            setattr(self, 'port', self.p)
            delattr(self, 'p')

        if hasattr(self, 'host'):
            self.displayName = self.host
        else:
            self.displayName = 'unknown'
        if displayPorts and hasattr(self, 'port'):
            self.displayName += ':' + str(self.port)

    def __cmp__(self, other):
        """ Order by IP"""
        return cmp(socket.inet_aton(self.host), socket.inet_aton(other.host))

    def printStatusHTML(self, buffer):
        print >> buffer, '''
        <tr><td align="right">''', self.displayName, '''</td>
        <td align="right">''', self.cDone, '''</td>
        <td align="right">''', '%.2e' % float(self.bDone), '''</td>
        <td align="right">''', self.c, '''</td>
        <td align="right">''', '%.2e' % float(self.b), '''</td>
        <td align="right">''', self.cFlight, '''</td>
        <td align="right">''', self.cPend, '''</td>
        <td align="right">''', '%.2e' % float(self.cSec), '''</td>
        <td align="right">''', '%.2e' % float(self.bSec), '''</td>
        <td align="right">''', '%.2f' % (float(self.eta) / 60), '''</td>
        </tr>'''

def formatConv(val):
    v = val.split("(")
    ret = float(v[0])
    if len(v) > 1:
        if v[1] == 'KB)':
            ret = ret * 1024
        elif v[1] == 'MB)':
            ret = ret * 1024 * 1024
        elif v[1] == 'GB)':
            ret = ret * 1024 * 1024 * 1024
        elif v[1] == 'TB)':
            ret = ret * 1024 * 1024 * 1024 * 1024
    return ret;

class UpServer:
    """Keep track of an up server state"""
    def __init__(self, status, info):
        if isinstance(info, str):
            serverInfo = info.split(',')
            # order here is host, port, rack, used, free, util, nblocks, last
            # heard, nblks corrupt, numDrives
            for i in xrange(len(serverInfo)):
                s = serverInfo[i].split('=')
                setattr(self, s[0].strip(), s[1].strip())

            if hasattr(self, 'numDrives'):
                self.numDrives = int(self.numDrives)
            else:
                setattr(self, 'numDrives', 0)

            if hasattr(self, 'ncorrupt'):
                self.ncorrupt = int(self.ncorrupt)
            else:
                setattr(self, 'ncorrupt', 0)

            if hasattr(self, 's'):
                setattr(self, 'host', self.s)
                setattr(self, 'ip', socket.gethostbyname(self.s))
                delattr(self, 's')

            if hasattr(self, 'p'):
                setattr(self, 'port', self.p)
                delattr(self, 'p')

            if hasattr(self, 'overloaded'):
                self.overloaded = int(self.overloaded) != 0
            else:
                setattr(self, 'overloaded', False)

            if hasattr(self, 'nevacuate'):
                self.nevacuate = int(self.nevacuate)
            else:
                setattr(self, 'nevacuate', 0)

            if hasattr(self, 'bytesevacuate'):
                self.bytesevacuate = long(self.bytesevacuate)
            else:
                setattr(self, 'bytesevacuate', long(0))

            if hasattr(self, 'good'):
                self.overloaded = int(self.overloaded) != 0
            else:
                setattr(self, 'good', True)

            if not hasattr(self, 'total'):
                setattr(self, 'total', '0')

            if hasattr(self, 'numReplications'):
                self.numReplications = int(self.numReplications)
            else:
                setattr(self, 'numReplications', 0)

            if hasattr(self, 'numReadReplications'):
                self.numReadReplications = int(self.numReadReplications)
            else:
                setattr(self, 'numReadReplications', 0)

            if hasattr(self, 'numWritableDrives'):
                self.numWritableDrives = int(self.numWritableDrives)
            else:
                setattr(self, 'numWritableDrives', self.numDrives)

            if hasattr(self, 'rack'):
                self.rack = int(self.rack)
            else:
                setattr(self, 'rack', -1)

            if hasattr(self, 'nwrites'):
                self.nwrites = int(self.nwrites)
            else:
                setattr(self, 'nwrites', -1)

            if hasattr(self, 'load'):
                self.load = int(self.load)
            else:
                setattr(self, 'load', -1)

            if hasattr(self, 'tiers'):
                self.tiers = self.tiers
            else:
                setattr(self, 'tiers', '')

            try:
                self.connected = int(self.connected)
            except:
                self.connected = 1

            try:
                self.replay = int(self.replay)
            except:
                self.replay = 0

            try:
                self.stopped = int(self.stopped)
            except:
                self.stopped = 0

            try:
                self.chunks = long(self.chunks)
            except:
                self.chunks = -1

            self.tiersCount = self.tiers.count(';') + 1
            if self.tiersCount <= 1 and self.tiers.find(':') < 0:
                self.tiersCount = 0

            self.lastheard = int(self.lastheard.split('(')[0])

            self.util  = float(self.util.split('%')[0])
            self.used  = formatConv(self.used)
            self.free  = formatConv(self.free)
            self.total = formatConv(self.total)
            if self.total <= 0 and self.free > 0 and self.util > 0:
                if self.util < 99.9:
                    self.total = self.free * 100. / (100. - self.util)
                else:
                    self.total = self.used + self.free

            self.down = 0
            self.retiring = 0
            if hasattr(self, 'host'):
                self.displayName = self.host
            else:
                self.displayName = 'unknown'
            if displayPorts and hasattr(self, 'port'):
                self.displayName += ':' + str(self.port)

            status.freeFsSpace += self.free
            if not self.good or self.overloaded:
                status.canNotBeUsedForPlacment += 1
            elif self.rack < 0:
                status.goodNoRackAssignedCount += 1

        if isinstance(info, DownServer):
            self.host = info.host
            self.port = info.port
            self.down = 1
            self.retiring = 0

    def __cmp__(self, other):
        """ Order by IP"""
        return cmp(socket.inet_aton(self.ip), socket.inet_aton(other.ip))

    def setRetiring(self, status):
        self.retiring = 1
        if not self.overloaded and self.good:
            status.canNotBeUsedForPlacment += 1
            if self.rack < 0:
                status.goodNoRackAssignedCount -= 1

    def printStatusHTML(self, buffer, count, showNoRack):
        if 0 == self.connected:
            if self.stopped:
                trclass = 'class="stopped"'
            elif self.replay:
                trclass = 'class="replay"'
            else:
                trclass = 'class="disconnected"'
        elif self.retiring:
            trclass = 'class="retiring"'
        elif self.nevacuate > 0:
            trclass = 'class="evacuating"'
        elif not objectStoreMode and self.overloaded:
            trclass = 'class="overloaded"'
        elif not self.good or self.down:
            trclass = 'class="notgood"'
        elif showNoRack and self.rack < 0:
            trclass = 'class="norack"'
        else:
            trclass = ''

        print >> buffer, '''<tr ''', trclass, '''><td align="right">''', self.displayName, '''</td>'''
        print >> buffer, '''<td align="right">''', self.numDrives, '''</td>'''
        print >> buffer, '''<td align="right">''', self.numWritableDrives, '''</td>'''
        print >> buffer, '''<td align="right">''', self.nwrites, '''</td>'''
        if self.tiersCount > 0 and displayChunkServerStorageTiers :
            print >> buffer, '''
                <td align="right">
                    <div id="linkspandetailtable">
                        <a href="#">''', self.tiersCount, '''
                            <span>
                            <div class="floatleft">
                            <table class="sortable status-table-span" id="cs%stiers">''' % count, '''
                            <thead><tr>
                                <th>Tier</th>
                                <th>Wr. dev.</th>
                                <th>Wr. blocks</th>
                                <th>Blocks</th>
                                <th>Free</th>
                                <th>Total</th>
                                <th>%Used</th>
                             </tr></thead><tbody><tr><td>
                                ''', self.tiers.replace(
                                        ';', '</td></tr><tr><td>'
                                    ).replace(
                                        ':', '</td><td>'
                                    ), '''
                            </td></tr></tbody></table></div></span>
                        </a>
                    </div>
                </td>
            '''
        else:
            print >> buffer, '''<td align="right">''', self.tiersCount, '''</td>'''
        print >> buffer, '''<td>''', '%.2e' % self.used, '''</td>'''
        print >> buffer, '''<td>''', '%.2e' % self.free, '''</td>'''
        print >> buffer, '''<td>''', '%.2e' % self.total, '''</td>'''
        print >> buffer, '''<td align="right">''', '%.2f' % self.util, '''</td>'''
        print >> buffer, '''<td align="right">''', self.nblocks, '''</td>'''
        print >> buffer, '''<td align="right">''', self.chunks, '''</td>'''
        print >> buffer, '''<td align="right">''', self.lastheard, '''</td>'''
        print >> buffer, '''<td align="right">''', self.numReplications, '''</td>'''
        print >> buffer, '''<td align="right">''', self.numReadReplications, '''</td>'''
        print >> buffer, '''<td align="right">''', self.ncorrupt, '''</td>'''
        print >> buffer, '''<td>''', '%.2e' % self.load, '''</td>'''
        print >> buffer, '''<td align="right">''', self.rack, '''</td></tr>'''

class RackNode:
    def __init__(self, host, rackId):
        self.host = host
        self.rackId = rackId
        self.wasStarted = 0
        self.isDown = 0
        self.overloaded = 0
        self.displayName = host

    def printHTML(self, buffer, count):
        if count % 2 == 0:
            trclass = ""
        else:
            trclass = "class=odd"

        if self.isDown:
            trclass = "class=dead"

        if self.overloaded == 1:
            trclass = "class=overloaded"

        if not self.wasStarted:
            trclass = "class=notstarted"

        print >> buffer, '''<tr ''', trclass, '''><td align="center">''', self.displayName, '''</td> </tr>'''

def nodeIsNotUp(status, d):
    x = [u for u in status.upServers if u.host == d.host and u.port == d.port]
    return len(x) == 0

def nodeIsRetiring(status, u):
    x = [r for r in status.retiringServers if u.host == r.host and u.port == r.port]
    return len(x) > 0

def mergeDownUpNodes(status):
    ''' in the set of down-nodes, mark those that are still down in red'''
    reallyDown = [d for d in status.downServers if nodeIsNotUp(status, d)]
    uniqueServers = set()
    for d in reallyDown:
        d.setStillDown()
        s = '%s:%s' % (d.host, d.port)
        uniqueServers.add(s)
    status.numReallyDownServers = len(uniqueServers)

def mergeRetiringUpNodes(status):
    ''' merge retiring nodes with up nodes'''
    [u.setRetiring(status) for u in status.upServers if nodeIsRetiring(status, u)]

def processUpNodes(status, nodes):
    servers = nodes.split('\t')
    status.upServers = [UpServer(status, c) for c in servers if c != '']

def processDownNodes(status, nodes):
    servers = nodes.split('\t')
    if servers != "":
        status.downServers = [DownServer(c) for c in servers if c != '']
        status.downServers.sort()

def processRetiringNodes(status, nodes):
    servers = nodes.split('\t')
    if servers != "":
        status.retiringServers = [RetiringServer(c) for c in servers if c != '']
        status.retiringServers.sort()

def processEvacuatingNodes(status, nodes):
    servers = nodes.split('\t')
    if servers != "":
        status.evacuatingServers = [EvacuatingServer(c) for c in servers if c != '']
        status.evacuatingServers.sort()

def bytesToReadable(b):
    v = long(b)
    if (v > (long(1) << 50)):
        return "%.2f&nbsp;PB" % (float(v) / (long(1) << 50))
    if (v > (long(1) << 40)):
        return "%.2f&nbsp;TB" % (float(v) / (long(1) << 40))
    if (v > (long(1) << 30)):
        return "%.2f&nbsp;GB" % (float(v) / (long(1) << 30))
    if (v > (long(1) << 20)):
        return "%.2f&nbsp;MB" % (float(v) / (long(1) << 20))
    return "%.2f&nbsp;bytes" % (v)

def processSystemInfo(systemInfo, sysInfo):
    info = sysInfo.split('\t')
    if len(info) < 3:
        return
    systemInfo.startedAt = info[0].split('=')[1]
    systemInfo.totalSpace = long(info[1].split('=')[1])
    systemInfo.usedSpace = long(info[2].split('=')[1])
    if len(info) < 4:
        return
    systemInfo.replications = long(info[3].split('=')[1])
    if len(info) < 5:
        return
    systemInfo.replicationsCheck = long(info[4].split('=')[1])
    if len(info) < 6:
        return
    systemInfo.pendingRecovery = long(info[5].split('=')[1])
    if len(info) < 10:
        return
    systemInfo.uptime = long(info[9].split('=')[1])
    if len(info) < 11:
        return
    systemInfo.usedBuffers = long(info[10].split('=')[1])
    if len(info) < 12:
        return
    systemInfo.clients = long(info[11].split('=')[1])
    if len(info) < 13:
        return
    systemInfo.chunkServers = long(info[12].split('=')[1])
    if len(info) < 14:
        return
    systemInfo.allocatedRequests = long(info[13].split('=')[1])
    if len(info) < 15:
        return
    systemInfo.sockets = long(info[14].split('=')[1])
    if len(info) < 16:
        return
    systemInfo.chunks = long(info[15].split('=')[1])
    if len(info) < 17:
        return
    systemInfo.pendingReplication = long(info[16].split('=')[1])
    if len(info) < 18:
        return
    systemInfo.internalNodes = long(info[17].split('=')[1])
    if len(info) < 19:
        return
    systemInfo.internalNodeSize = long(info[18].split('=')[1])
    if len(info) < 20:
        return
    systemInfo.internalNodeAllocSize = long(info[19].split('=')[1])
    if len(info) < 21:
        return
    systemInfo.dentries = long(info[20].split('=')[1])
    if len(info) < 22:
        return
    systemInfo.dentrySize = long(info[21].split('=')[1])
    if len(info) < 23:
        return
    systemInfo.dentryAllocSize = long(info[22].split('=')[1])
    if len(info) < 24:
        return
    systemInfo.fattrs = long(info[23].split('=')[1])
    if len(info) < 25:
        return
    systemInfo.fattrSize = long(info[24].split('=')[1])
    if len(info) < 26:
        return
    systemInfo.fattrAllocSize = long(info[25].split('=')[1])
    if len(info) < 27:
        return
    systemInfo.cinfos = long(info[26].split('=')[1])
    if len(info) < 28:
        return
    systemInfo.cinfoSize = long(info[27].split('=')[1])
    if len(info) < 29:
        return
    systemInfo.cinfoAllocSize = long(info[28].split('=')[1])
    if len(info) < 30:
        return
    systemInfo.csmapNodes = long(info[29].split('=')[1])
    if len(info) < 31:
        return
    systemInfo.csmapNodeSize = long(info[30].split('=')[1])
    if len(info) < 32:
        return
    systemInfo.csmapAllocSize = long(info[31].split('=')[1])
    if len(info) < 33:
        return
    systemInfo.csmapEntryAllocs = long(info[32].split('=')[1])
    if len(info) < 34:
        return
    systemInfo.csmapEntryBytes = long(info[33].split('=')[1])
    if len(info) < 35:
        return
    systemInfo.delayedRecovery = long(info[34].split('=')[1])
    if len(info) < 36:
        return
    systemInfo.replicationBacklog = long(info[35].split('=')[1])
    if len(info) < 37:
        return
    systemInfo.isInRecovery = long(info[36].split('=')[1]) != 0
    if len(info) < 38:
        return
    systemInfo.csToRestart = long(info[37].split('=')[1])
    if len(info) < 39:
        return
    systemInfo.csMastersToRestart = long(info[38].split('=')[1])
    if len(info) < 40:
        return
    systemInfo.csMaxGoodCandidateLoadAvg = long(info[39].split('=')[1])
    if len(info) < 41:
        return
    systemInfo.csMaxGoodMasterLoadAvg = long(info[40].split('=')[1])
    if len(info) < 42:
        return
    systemInfo.csMaxGoodSlaveLoadAvg = long(info[41].split('=')[1])
    if len(info) < 43:
        return
    systemInfo.hibernatedServerCount = long(info[42].split('=')[1])
    if len(info) < 44:
        return
    systemInfo.freeFsSpace = long(info[43].split('=')[1])
    if len(info) < 45:
        return
    systemInfo.goodMasters = long(info[44].split('=')[1])
    if len(info) < 46:
        return
    systemInfo.goodSlaves = long(info[45].split('=')[1])
    if len(info) < 47:
        return
    systemInfo.totalDrives = long(info[46].split('=')[1])
    if len(info) < 48:
        return
    systemInfo.writableDrives = long(info[47].split('=')[1])
    if len(info) < 49:
        return
    systemInfo.appendCacheSize = long(info[48].split('=')[1])
    if len(info) < 50:
        return
    systemInfo.maxClients = long(info[49].split('=')[1])
    if len(info) < 51:
        return
    systemInfo.maxChunkServers = long(info[50].split('=')[1])
    if len(info) < 52:
        return
    systemInfo.totalBuffers = long(info[51].split('=')[1])
    if len(info) < 53:
        return
    systemInfo.objStoreEnabled = long(info[52].split('=')[1])
    if len(info) < 54:
        return
    systemInfo.objStoreDeletes = long(info[53].split('=')[1])
    if len(info) < 55:
        return
    systemInfo.objStoreDeletesInFlight = long(info[54].split('=')[1])
    if len(info) < 56:
        return
    systemInfo.objStoreDeletesRetry = long(info[55].split('=')[1])
    if len(info) < 57:
        return
    systemInfo.objStoreDeletesStartedAgo = long(info[56].split('=')[1])
    if len(info) < 58:
        return
    systemInfo.fileCount = long(info[57].split('=')[1])
    if len(info) < 59:
        return
    systemInfo.dirCount = long(info[58].split('=')[1])
    if len(info) < 60:
        return
    systemInfo.sumOfLogicalFileSizes = long(info[59].split('=')[1])
    if len(info) < 61:
        return
    systemInfo.fileSystemId = long(info[60].split('=')[1])
    if len(info) < 62:
        return
    systemInfo.vrPrimaryFlag = long(info[61].split('=')[1])
    if len(info) < 63:
        return
    systemInfo.vrNodeId = long(info[62].split('=')[1])
    if len(info) < 64:
        return
    systemInfo.vrPrimaryNodeId = long(info[63].split('=')[1])
    if len(info) < 65:
        return
    systemInfo.vrActiveFlag = long(info[64].split('=')[1])
    if len(info) < 66:
        return
    systemInfo.logTimeUsec = long(info[65].split('=')[1])
    if len(info) < 67:
        return
    systemInfo.logTimeOpsCount = long(info[66].split('=')[1])
    if len(info) < 68:
        return
    systemInfo.logPendingOpsCount = long(info[67].split('=')[1])
    if len(info) < 69:
        return
    systemInfo.log5SecAvgUsec = long(info[68].split('=')[1])
    if len(info) < 70:
        return
    systemInfo.log10SecAvgUsec = long(info[69].split('=')[1])
    if len(info) < 71:
        return
    systemInfo.log15SecAvgUsec = long(info[70].split('=')[1])
    if len(info) < 72:
        return
    systemInfo.log5SecAvgReqRate = long(info[71].split('=')[1])
    if len(info) < 73:
        return
    systemInfo.log10SecAvgReqRate = long(info[72].split('=')[1])
    if len(info) < 74:
        return
    systemInfo.log15SecAvgReqRate = long(info[73].split('=')[1])
    if len(info) < 75:
        return
    systemInfo.logAvgReqRateDiv = long(info[74].split('=')[1])
    if 0 == systemInfo.logAvgReqRateDiv:
        systemInfo.logAvgReqRateDiv = 1
    if len(info) < 76:
        return
    systemInfo.bTreeHeight = long(info[75].split('=')[1])
    if len(info) < 77:
        return
    systemInfo.logDiskWriteUsec = long(info[76].split('=')[1])
    if len(info) < 78:
        return
    systemInfo.logDiskWriteByteCount = long(info[77].split('=')[1])
    if len(info) < 79:
        return
    systemInfo.logDiskWriteCount = long(info[78].split('=')[1])
    if len(info) < 80:
        return
    systemInfo.logOpWrite5SecAvgUsec = long(info[79].split('=')[1])
    if len(info) < 81:
        return
    systemInfo.logOpWrite10SecAvgUsec = long(info[80].split('=')[1])
    if len(info) < 82:
        return
    systemInfo.logOpWrite15SecAvgUsec = long(info[81].split('=')[1])
    if len(info) < 83:
        return
    systemInfo.logExceedQueueDepthFailedCount = long(info[82].split('=')[1])
    if len(info) < 84:
        return
    systemInfo.logPendingAckByteCount = long(info[83].split('=')[1])
    if len(info) < 85:
        return
    systemInfo.logTotalRequestCount = long(info[84].split('=')[1])
    if len(info) < 86:
        return
    systemInfo.logExceedLogQueueDepthFailureCount300SecAvg = long(info[85].split('=')[1])

def updateServerState(status, rackId, host, server):
    if rackId in status.serversByRack:
        # we really need a find_if()
        for r in serversByRack[rackId]:
            if r.host == host:
                if isinstance(server, UpServer(status)):
                    r.overloaded = server.overloaded
                r.wasStarted = 1
                if hasattr(server, 'stillDown'):
                    r.isDown = server.stillDown
                    if r.isDown:
                        r.overloaded = 0

def splitServersByRack(status):
    for u in status.upServers:
        s = socket.gethostbyname(u.host)
        rackId = int(s.split('.')[2])
        updateServerState(status, rackId, s, u)

    for u in status.downServers:
        s = socket.gethostbyname(u.host)
        rackId = int(s.split('.')[2])
        updateServerState(status, rackId, s, u)


def ping(status, metaserver):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((metaserver.node, metaserver.port))
    req = "PING\r\nVersion: KFS/1.0\r\nCseq: 1\r\nClient-Protocol-Version: 116\r\n\r\n"
    sock.send(req)
    sockIn = sock.makefile('r')
    status.tiersColumnNames = {}
    status.tiersInfo = {}
    for line in sockIn:
        line = line.lstrip()
        if line == '':
            break
        if line.startswith('Down Servers:'):
            processDownNodes(status, line[line.find(':') + 1:].strip())
            continue

        if line.startswith('Retiring Servers:'):
            processRetiringNodes(status, line[line.find(':') + 1:].strip())
            continue

        if line.startswith('Evacuating Servers:'):
            processEvacuatingNodes(status, line[line.find(':') + 1:].strip())
            continue

        if line.startswith('WORM:'):
            try:
                wormMode = line[line.find(':') + 1:].strip()
                if int(wormMode) == 1:
                    status.systemInfo.wormMode = "Enabled"
                else:
                    status.systemInfo.wormMode = "Disabled"
            except:
                pass

        if line.startswith('Build-version:'):
            status.systemInfo.buildVersion = line[line.find(':') + 1:].strip()
            continue

        if line.startswith('Source-version:'):
            status.systemInfo.sourceVersion = line[line.find(':') + 1:].strip()
            continue

        if line.startswith('System Info:'):
            processSystemInfo(status.systemInfo, line[line.find(':') + 1:].strip())
            continue

        if line.startswith('Servers:'):
            processUpNodes(status, line[line.find(':') + 1:].strip())
            continue

        if line.startswith('Storage tiers info names:'):
            status.tiersColumnNames = line[line.find(':') + 1:].strip().split('\t')
            continue

        if line.startswith('Storage tiers info:'):
            status.tiersInfo = line[line.find(':') + 1:].strip().split('\t')
            continue

        config = line.startswith('Config:')
        if config or line.startswith('VR Status:'):
            if gHasCollections:
                res = collections.OrderedDict()
            else:
                res = {}
            for keyval in line[line.find(':') + 1:].strip().split(';'):
                try:
                    [ key, value ] = keyval.split('=')
                    res[key] = value
                except ValueError:
                    continue
            if config:
                status.config = res
            else:
                status.vrStatus = res;
            continue

    mergeDownUpNodes(status)
    mergeRetiringUpNodes(status)
    status.upServers.sort()

    sock.close()

def splitThousands( s, tSep=',', dSep='.'):
    '''Splits a general float on thousands. GIGO on general input'''
    if s == None:
        return 0
    if not isinstance( s, str ):
        s = str( s )

    cnt=0
    numChars=dSep+'0123456789'
    ls=len(s)
    while cnt < ls and s[cnt] not in numChars: cnt += 1

    lhs = s[ 0:cnt ]
    s = s[ cnt: ]
    if dSep == '':
        cnt = -1
    else:
        cnt = s.rfind( dSep )
    if cnt > 0:
        rhs = dSep + s[ cnt+1: ]
        s = s[ :cnt ]
    else:
        rhs = ''

    splt=''
    while s != '':
        splt= s[ -3: ] + tSep + splt
        s = s[ :-3 ]

    return lhs + splt[ :-1 ] + rhs

def printRackViewHTML(rack, servers, buffer):
    '''Print out all the servers in the specified rack'''
    print >> buffer, '''
    <div class="floatleft">
     <table class="network-status-table" cellspacing="0" cellpadding="0.1em" summary="Status of nodes in the rack ''', rack, ''' ">
     <tbody><tr><td><b>Rack : ''', rack,'''</b></td></tr>'''
    count = 0
    for s in servers:
        s.printHTML(buffer, count)
        count = count + 1
    print >> buffer, '''</tbody></table></div>'''

def rackView(buffer, status):
    splitServersByRack(status)
    numNodes = sum([len(v) for v in status.serversByRack.itervalues()])
    print >> buffer, '''
<body class="oneColLiqCtr">
<div id="container">
  <div id="mainContent">
    <table width=100%>
      <tr>
      <td>
      <p> Number of nodes: ''', numNodes, ''' </p>
      </td>
      <td align="right">
      <table class="network-status-table" font-size=14>
      <tbody>
        <tr class=notstarted><td></td><td>Not Started</td></tr>
        <tr class=dead><td></td><td>Dead Node</td></tr>
        <tr class=retiring><td></td><td>Retiring Node</td></tr>
        <tr class=><td ></td><td>Healthy</td></tr>
        <tr class=overloaded><td></td><td>Healthy, but not enough space for writes</td></tr>
      </tbody>
      </table>
      </td>
      </tr>
    </table>
    <hr>'''
    for rack, servers in status.serversByRack.iteritems():
        printRackViewHTML(rack, servers, buffer)

    print >> buffer, '''
    </div>
    </div>
    </body>
    </html>'''


class ChunkHandler:

    def __init__(self):
        self.chunkDataManager = None
        self.countersDataManager = None
        self.chunkDirDataManager = None
        self.thread = None

        self.interval = 5

    def processInput(self, inputBody):

        if inputBody.find("GETCOUNTERS") != -1:
            theType =  kMeta
        elif inputBody.find("GETCHART") != -1:
            theType =  kChart
        elif inputBody.find("GETDIRCOUNTERS") != -1:
            theType = kChunkDirs
        else:
            theType = kChunks

#        refresh=60&delta=60&&dividedelta=dividedelta
        self.setDeltaValues(inputBody, theType)

        index = inputBody.find("MUMU")
        if(index < 0):
             return -1
        newInputBody = inputBody[index:]
        headers = newInputBody.split('&')
        newHeaders = [header.strip()[5:].replace("%25","%") for header in headers]
#        newHeaders = [header.strip()[5:] for header in headers]
        # data MUMU=header1&MUMU=header22&MUMU=header3

        if theType != kMeta:
            if theType == kChunkDirs:
                self.setChunkDirsSelectedHeaders(newHeaders)
            else:
                self.setChunkSelectedHeaders(newHeaders)
        else:
            self.setCountersSelectedHeaders(newHeaders)
        return theType


    def setIntervalData(self, refreshInterval, predefinedHeaders, predefinedChunkDirHeaders, monthly, dayly, hourly, current):
        self.interval = refreshInterval
        headers = []
        if predefinedHeaders != "":
            headers = predefinedHeaders.split('&')
        dirHeaders = []
        if predefinedChunkDirHeaders != "":
            dirHeaders = predefinedChunkDirHeaders.split('&')
        self.chunkDataManager = ChunkDataManager(kServerName, headers, monthly, dayly, hourly, current)
        self.countersDataManager = ChunkDataManager(None, None, monthly, dayly, hourly, current)
        self.chunkDirDataManager = ChunkDataManager(kChunkDirName, dirHeaders, monthly, dayly, hourly, current)

    def startThread(self, serverHost, serverPort):
        if self.chunkDataManager == None or self.countersDataManager == None or self.chunkDirDataManager == None:
            print "ERROR - need to set the chunk intervals data first"
            return;
        if self.thread != None:
            return;

        self.thread = ChunkThread(serverHost, serverPort, self.interval,
            self.chunkDataManager, self.countersDataManager, self.chunkDirDataManager)
        self.thread.start()

    def chunksToHTML(self, buffer):

        if self.chunkDataManager == None:
            return 0
        self.chunkDataManager.lock.acquire()
#        print "deltaInterval", self.deltaInterval
        deltaList = self.chunkDataManager.getDelta()

        iRet = 0
        if(deltaList != None):
            HtmlPrintData(
                kServerName,
                deltaList,
                self.chunkDataManager,
                "no",
                "Chunk Servers Status",
                "servers"
            ).printToHTML(buffer)
            iRet = 1
        self.chunkDataManager.lock.release()
        return iRet

    def chunkDirsToHTML(self, buffer):

        if self.chunkDirDataManager == None:
            return 0
        self.chunkDirDataManager.lock.acquire()
#        print "deltaInterval", self.deltaInterval
        deltaList = self.chunkDirDataManager.getDelta()

        iRet = 0
        if(deltaList != None):
            HtmlPrintData(
                kChunkDirName,
                deltaList,
                self.chunkDirDataManager,
                "GETDIRCOUNTERS",
                "Chunk Directories Status",
                "directories"
            ).printToHTML(buffer)
            iRet = 1
        self.chunkDirDataManager.lock.release()
        return iRet

    def countersToHTML(self, buffer):

        if self.countersDataManager == None:
            return 0
        self.countersDataManager.lock.acquire()
#        print "deltaInterval", self.deltaInterval
        deltaList = self.countersDataManager.getDelta()

        iRet = 0
        if(deltaList != None):
            HtmlPrintMetaData(deltaList,self.countersDataManager).printToHTML(buffer)
            iRet = 1
        self.countersDataManager.lock.release()
        return iRet

    def chartsToHTML(self, buffer):

        if self.chunkDataManager == None:
            return 0
        chartData = ChartData()
        self.chunkDataManager.lock.acquire()
        self.chunkDataManager.getChartData(chartData)
        self.chunkDataManager.lock.release()
        ChartHTML(chartData).printToHTML(buffer)
        return 1

    def parseMinusTime(self,str1):
        if(str1 == ""):
            return(0)
        theTime = 0;
        index = str1.find('d')
        if index > 0:
            theTime = int(str1[:index])*86400
            str2 = str1[index+1:].strip()
        else:
            str2 = str1
        if(str2 == ""):
            return(theTime)

        index = str2.find('h')
        if index > 0:
            theTime = theTime + int(str2[:index])*3600
            str3 = str2[index+1:].strip()
        else:
            str3 = str2
        if(str3 == ""):
            return(theTime)

        index = str3.find('m')
        if index > 0:
            theTime = theTime + int(str3[:index])*60
            str4 = str3[index+1:].strip()
        else:
            str4 = str3
        if(str4 == ""):
            return(theTime)

        index = str4.find('s')
        if index > 0:
            theTime = theTime + int(str4[:index])
        else:
            theTime = theTime + int(str4.strip())
        return(theTime)

    def setDeltaValues(self, inputStr,theType):
        if theType ==  kMeta:
            dataManager = self.countersDataManager
        else:
            if theType == kChunkDirs:
                dataManager = self.chunkDirDataManager
            else:
                dataManager = self.chunkDataManager
        if(dataManager == None):
            return

        value = self.getIntValue(inputStr,"refresh")
        if( value > 0) :
            dataManager.refreshInterval = value

        str1 = self.getValue(inputStr,"startTime")
        if( str1 != None) :
            dataManager.minusLatestTime = self.parseMinusTime(str1)
        else:
            dataManager.minusLatestTime = 0

        value = self.getIntValue(inputStr,"delta")
        if( value > 0) :
            dataManager.deltaInterval = value
        doDivide = self.getIntValue(inputStr,"dividedelta")
        if( doDivide > 0) :
            dataManager.doDivide = 1
        else:
            dataManager.doDivide = 0

    def  getIntValue(self, inputStr, keyword):
        str1 = self.getValue(inputStr, keyword)
        if str1 == None:
            return -1
        else:
            return int(str1)

    def  getValue(self, inputStr, keyword):
        keyLength = len(keyword)
        index = inputStr.find(keyword);
        if index < 0:
            return None
        indexEnd = inputStr.find("&",index);
        if indexEnd < 0:
            newStr = inputStr[index+keyLength+1:]
        else:
            newStr = inputStr[index+keyLength+1:indexEnd]
        return newStr.strip();

    def  setChunkSelectedHeaders(self, headers):
        self.chunkDataManager.lock.acquire()
        self.chunkDataManager.setSelectedHeaders(headers)
        self.chunkDataManager.lock.release()

    def  setCountersSelectedHeaders(self, headers):
        self.countersDataManager.lock.acquire()
        self.countersDataManager.setSelectedHeaders(headers)
        self.countersDataManager.lock.release()

    def  setChunkDirsSelectedHeaders(self, headers):
        self.chunkDirDataManager.lock.acquire()
        self.chunkDirDataManager.setSelectedHeaders(headers)
        self.chunkDirDataManager.lock.release()

class QueryCache:
    # avoid hitting the metaserver with GET_CHUNK_SERVER_DIRS_COUNTERS query
    # more than once per 30 sec.
    TIME = time.time()
    REFRESH_INTERVAL = 30
    DIR_COUNTERS = ChunkServerData()

    @staticmethod
    def GetMatchingCounters(chunkserverHosts):
        result = {}
        chunkserverIndex = QueryCache.DIR_COUNTERS.chunkHeaders.index('Chunk-server')
        chunkDirIndex = QueryCache.DIR_COUNTERS.chunkHeaders.index('Chunk-dir')
        for entry in QueryCache.DIR_COUNTERS.chunkServers:
            dirResult = {}
            aResult = {}
            chunkserver = entry.nodes[chunkserverIndex].split(':')[0]
            chunkdir = entry.nodes[chunkDirIndex]
            if chunkserver in chunkserverHosts:
                for i in xrange(len(QueryCache.DIR_COUNTERS.chunkHeaders)):
                    key = QueryCache.DIR_COUNTERS.chunkHeaders[i]
                    val = entry.nodes[i]
                    aResult[key] = val
                dirResult[chunkdir] = aResult
                result.setdefault(chunkserver, {}).update(dirResult)
        return result


    @staticmethod
    def GetChunkServerCounters(chunkserverHosts):
        global metaserverPort, metaserverHost
        if time.time() - QueryCache.TIME < QueryCache.REFRESH_INTERVAL:
            if len(QueryCache.DIR_COUNTERS.chunkServers) > 0:
                #print "Using cached numbers:", QueryCache.DIR_COUNTERS.printDebug()
                return QueryCache.GetMatchingCounters(chunkserverHosts)
        dir_counters = ChunkServerData()
        req = "GET_CHUNK_SERVER_DIRS_COUNTERS\r\nVersion: KFS/1.0\r\nCseq: 1\r\nClient-Protocol-Version: 116\r\n\r\n"
        isConnected = False
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((metaserverHost, metaserverPort))
            isConnected = True
            sock.send(req)
            sockIn = sock.makefile('r')

            contentLength = -1
            gotHeader = 0
            sizeRead = 0

            for line in sockIn:
                if contentLength == -1:
                    infoData = line.strip().split(':')
                    if len(infoData) > 1:
                        if infoData[0].lower() == 'content-length':
                            contentLength = int(infoData[1].strip())
                    continue
                sizeRead += len(line)
                if len(line.strip()) == 0:
                    if sizeRead >= contentLength:
                        break
                    else:
                        continue
                nodes = line.strip().split(',')
                if gotHeader == 0:
                    dir_counters.initHeader(nodes)
                    gotHeader = 1
                else:
                    dir_counters.addChunkServer(nodes)
                if contentLength >= 0 and sizeRead >= contentLength:
                    break
            sock.close()
        except socket.error, msg:
            print msg, datetime.now().ctime()
            if isConnected:
                sock.close()
            return 0
        QueryCache.TIME = time.time()
        QueryCache.DIR_COUNTERS = dir_counters
        #print "Using fresh numbers:", QueryCache.DIR_COUNTERS.printDebug()
        if len(QueryCache.DIR_COUNTERS.chunkServers) > 0:
            return QueryCache.GetMatchingCounters(chunkserverHosts)


class QFSQueryHandler:
    @staticmethod
    def HandleQuery(queryPath, metaserver, buffer):
        if not gJsonSupported:
            return (501, 'Server does not support query')

        if queryPath.startswith('/query/chunkservers'):
            status = Status()
            try:
                ping(status, metaserver)
                upServers = set()
                for u in status.upServers:
                    upServers.add(socket.gethostbyname(u.host))
                downServers = set()
                for d in status.downServers:
                    downServers.add(socket.gethostbyname(d.host))
                downServers -= upServers

                output = {}
                output['up_servers'] = upServers
                output['down_servers'] = downServers
                print >> buffer, json.dumps(output, cls=SetEncoder)
                return (200, '')
            except IOError:
                return (504, 'Unable to ping metaserver')
        elif queryPath.startswith('/query/chunkserverdirs/'):
            try:
                hostsToMatch = queryPath[len('/query/chunkserverdirs/'):].split('&')
                print >> buffer, json.dumps(QueryCache.GetChunkServerCounters(set(hostsToMatch)))
                return (200, '')
            except IOError:
                return (504, 'Unable to ping metaserver')
        return (404, 'Not Found')


class Pinger(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def __init__(self, request, client_address, server):
        SimpleHTTPServer.SimpleHTTPRequestHandler.__init__(self, request, client_address, server)

    def setMeta(self, meta):
        self.metaserver = meta

    def sendErrorResponse(self, code, msg):
        self.send_response(code)

        body = "error %d", msg
        #Send standard HTP headers
        self.send_header('Content-type','text/html; charset=utf-8')
        self.send_header("Connection", "close")
        self.send_header("Accept-Ranges", "bytes")
        self.send_header('Content-length', len(body)-1)
        self.end_headers()
        self.wfile.write(body)
        return

    def do_POST(self):
        global gChunkHandler
        interval=60 #todo

        clen = int(self.headers.getheader('Content-Length').strip())
        if(clen <= 0):
            self.send_response(400)
            return

        inputBody = self.rfile.read(clen)

        theType = gChunkHandler.processInput(inputBody)

        txtStream = StringIO()

        if theType == kMeta:
            if gChunkHandler.countersToHTML(txtStream) == 0:
                print "NOT working!"
                self.send_error(404, 'Not data')
                return
        elif  theType == kChart:
            if gChunkHandler.chartsToHTML(txtStream) == 0:
                print "NOT working!"
                self.send_error(404, 'Not data')
                return
        elif  theType == kChunks:
            if gChunkHandler.chunksToHTML(txtStream) == 0:
                print "NOT working!"
                self.send_error(404, 'Not data')
                return
        elif  theType == kChunkDirs:
            if gChunkHandler.chunkDirsToHTML(txtStream) == 0:
                print "NOT working!"
                self.send_error(404, 'Not data')
                return
        else:
            self.send_response(400)
            return

        reqHost = self.headers.getheader('Host')
        refresh = '%d ; URL=http://%s%s' %(interval, reqHost, self.path)

        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.send_header('Content-length', txtStream.tell())
        self.end_headers()
        self.wfile.write(txtStream.getvalue())



    def do_GET(self):
        global metaserverPort, metaserverHost, docRoot
        global gChunkHandler
        try:
            if self.path.startswith('/favicon.ico'):
                self.send_response(200)
                return
            if self.path.startswith('/files'):
                # skip over '/files/
                fpath = os.path.join(docRoot, self.path[7:])
                try:
                    self.send_response(200)
                    self.send_header('Content-length', str(os.path.getsize(fpath)))
                    self.end_headers()
                    self.copyfile(urllib.urlopen(fpath), self.wfile)
                except IOError:
                    self.send_error(404, 'Not found')
                return

            if self.path.startswith('/charts'):
                fpath = self.path[1:]
                try:
                    self.send_response(200)
                    self.send_header('Content-length', str(os.path.getsize(fpath)))
                    self.end_headers()
                    self.copyfile(urllib.urlopen(fpath), self.wfile)
                except IOError:
                    self.send_error(404, 'Not found')
                return

            metaserver = ServerLocation(node=metaserverHost,
                                        port=metaserverPort)
            txtStream = StringIO()

            if self.path.startswith('/query/'):
                (ret, msg) = QFSQueryHandler.HandleQuery(self.path,
                                                         metaserver,
                                                         txtStream)
                if ret != 200:
                    self.send_error(ret, msg)
                    return
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-length', txtStream.tell())
                self.end_headers()
                self.wfile.write(txtStream.getvalue())
                return

            if(gChunkHandler.thread == None):
                gChunkHandler.startThread(metaserverHost, metaserverPort)

            status  = None
            reqType = None
            getVrStatusHtml = self.path.startswith('/meta-vr-status-html')
            if getVrStatusHtml or self.path.startswith('/meta-conf-html') :
                status = Status()
                ping(status, metaserver)
                if getVrStatusHtml:
                    title         = 'Meta Server Viewstamped Replication Status'
                    keyColumnName = 'Name'
                    keyvals       = status.vrStatus
                    reqType       = kVrStatus
                else:
                    title         = 'Meta Server Configuration'
                    keyColumnName = 'Parameter Name'
                    keyvals       = status.config
                    reqType       = kConfig
                printStyle(txtStream, title)
                print >> txtStream, '''
                    <body class="oneColLiqCtr">
                    <div id="container">
                    <div id="mainContent">
                        <h1>''', title, displayName, '''</h1>
                        <P> <A href="/">Back</A>
                        </P>
                        <div class="floatleft">
                        <table class="sortable network-status-table" id="configtable">
                        <caption> ''', title, ''' </caption>
                        <thead>
                        <tr>
                        <th>''', keyColumnName, '''</th>
                        <th>Value</th>
                        </tr>
                        </thead>
                        <tbody>
                        '''
                for k in keyvals:
                    print >> txtStream, '<tr><td>', htmlEscape(k), '</td><td>',\
                        htmlEscape(keyvals[k]), '</td></tr>'
                print >> txtStream, '''
                        </tbody>
                        </table>
                        </div>
                    </div>
                    </div>
                    </body>
                    </html>
                '''
                self.path = '/'
            elif self.path.startswith('/meta-conf') :
                status = Status()
                ping(status, metaserver)
                if gJsonSupported:
                    print >> txtStream, json.dumps(
                        status.config, sort_keys=True, indent=0)
                else:
                    print >> txtStream, status.config
                self.path = '/'
                reqType = cMeta
            elif self.path.startswith('/meta-vr-status') :
                status = Status()
                ping(status, metaserver)
                if gJsonSupported:
                    print >> txtStream, json.dumps(
                        status.vrStatus, sort_keys=True, indent=0)
                else:
                    print >> txtStream, status.vrStatus
                self.path = '/'
                reqType = kVrStatus
            elif self.path.startswith('/chunk-it') :
                self.path = '/'
                reqType = kChunks
            elif self.path.startswith('/meta-it') :
                self.path = '/'
                reqType = kMeta
            elif self.path.startswith('/browse-it') :
                self.path = self.path[len('/browse-it'):]
                if self.path == '':
                    self.path = '/'
                reqType = kBrowse
            elif self.path.startswith('/chunkdir-it') :
                self.path = '/'
                reqType = kChunkDirs

            if reqType == kChunks:
                if gChunkHandler.chunksToHTML(txtStream) == 0:
                    self.send_error(404, 'Not found')
                    return
            elif reqType == kMeta:
                if gChunkHandler.countersToHTML(txtStream) == 0:
                    self.send_error(404, 'Not found')
                    return
            elif reqType == kChunkDirs:
                if gChunkHandler.chunkDirsToHTML(txtStream) == 0:
                    self.send_error(404, 'Not found')
                    return
            elif reqType == kBrowse and gQfsBrowser.browsable:
                if gQfsBrowser.printToHTML(self.path,
                                           metaserverHost,
                                           metaserverPort,
                                           txtStream) == 0:
                    self.send_error(404, 'Not found')
                    return
            elif reqType != cMeta and reqType != kConfig and reqType != kVrStatus:
                status = Status()
                ping(status, metaserver)
                printStyle(txtStream, 'QFS Status')

            refresh = None

            if self.path.startswith('/cluster-view'):
                rackView(txtStream, status)
            else:
                if reqType == None:
                    status.systemStatus(txtStream)
                reqHost = self.headers.getheader('Host')
                if reqHost is not None and autoRefresh > 0:
                    if reqType != None:
                        refresh = None
                    else:
                        refresh = str(autoRefresh) + ' ; URL=http://' + reqHost + self.path

            self.send_response(200)
            if reqType != cMeta:
                self.send_header('Content-type', 'text/html')
            else:
                self.send_header('Content-type', 'application/json')
            self.send_header('Content-length', txtStream.tell())
            if refresh is not None:
                self.send_header('Refresh', refresh)
            self.end_headers()
            self.wfile.write(txtStream.getvalue())

        except IOError:
            self.send_error(504, 'Unable to ping metaserver')

def parseChunkConfig(config):
    refreshInterval = 10
    predefinedHeaders = ""
    predefinedChunkDirHeaders = ""
    try:
        refreshInterval = config.get('chunk', 'refreshInterval')
    except:
        pass
    try:
        predefinedHeaders = config.get('chunk', 'predefinedHeaders')
    except:
        pass
    try:
        predefinedChunkDirHeaders = config.get('chunk', 'predefinedChunkDirHeaders')
    except:
        pass

    theSize = 10
    timespan = 10
    try:
        theSize = config.get('chunk', 'currentSize')
    except:
        pass
    try:
        timespan = config.get('chunk', 'currentSpan')
    except:
        pass
    current = ChunkArrayData(timespan,theSize)

    theSize = 10
    timespan = 120
    try:
        theSize = config.get('chunk', 'hourlySize')
    except:
        pass
    try:
        timespan = config.get('chunk', 'hourlySpan')
    except:
        pass
    hourly = ChunkArrayData(timespan,theSize)

    theSize = 10
    timespan = 120
    try:
        theSize = config.get('chunk', 'daylySize')
    except:
        pass
    try:
        timespan = config.get('chunk', 'daylySpan')
    except:
        pass
    dayly = ChunkArrayData(timespan,theSize)


    theSize = 10
    timespan = 120
    try:
        theSize = config.get('chunk', 'monthlySize')
    except:
        pass
    try:
        timespan = config.get('chunk', 'monthlySpan')
    except:
        pass
    monthly = ChunkArrayData(timespan,theSize)


    gChunkHandler.setIntervalData(int(refreshInterval),
        predefinedHeaders, predefinedChunkDirHeaders, monthly, dayly, hourly, current)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

if __name__ == '__main__':
    global gChunkHandler
    global gQfsBrowser
    allMachinesFile = ""
    if len(sys.argv) != 2:
        print "Usage : ./qfsstatus.py <server.conf>"
        sys.exit()

    if not os.path.exists(sys.argv[1]):
        print "Unable to open ", sys.argv[1]
        sys.exit()

    gChunkHandler = ChunkHandler()
    gQfsBrowser = QFSBrowser()

    config = ConfigParser()
    config.readfp(open(sys.argv[1], 'r'))
    metaserverPort = config.getint('webserver', 'webServer.metaserverPort')
    try:
        metaserverHost = config.get('webserver', 'webServer.metaserverHost')
    except:
        pass
    try:
        autoRefresh = int(config.get('webserver', 'webServer.autoRefresh'))
    except:
        pass
    try:
        displayPorts = config.get('webserver', 'webServer.displayPorts')
    except:
        pass
    try:
        socketTimeout = config.get('webserver', 'webServer.socketTimeout')
    except:
        socketTimeout = 90
        pass
    try:
        displayChunkServerStorageTiers = config.getboolean('webserver', 'webServer.displayChunkServerStorageTiers')
    except:
        displayChunkServerStorageTiers = True
        pass
    docRoot = config.get('webserver', 'webServer.docRoot')
    try:
        HOST = config.get('webserver', 'webServer.host')
    except:
        HOST = "0.0.0.0"
        pass
    myWebserverPort = config.getint('webserver', 'webServer.port')
    allMachinesFile = config.get('webserver', 'webServer.allMachinesFn')
    try:
        objectStoreMode = config.getboolean('webserver', 'webServer.objectStoreMode')
    except:
        objectStoreMode = False
        pass
    if metaserverHost != '127.0.0.1' and metaserverHost != 'localhost':
        displayName = metaserverHost
    else:
        displayName = platform.node()
    displayName += ':' + str(metaserverPort)

    parseChunkConfig(config)

    if not os.path.exists(allMachinesFile):
        print "Unable to open all machines file: ", allMachinesFile
    else:
        # Read in the list of nodes that we should be running a chunkserver on
        print "Starting HttpServer..."
        for line in open(allMachinesFile, 'r'):
            s = socket.gethostbyname(line.strip())
            rackId = int(s.split('.')[2])
            if rackId in serversByRack:
                serversByRack[rackId].append(RackNode(s, rackId))
            else:
                serversByRack[rackId] = [RackNode(s, rackId)]

    socket.setdefaulttimeout(socketTimeout)
    SocketServer.TCPServer.allow_reuse_address = True
    httpd = ThreadedTCPServer((HOST, myWebserverPort), Pinger)
    pidf = ''
    try:
        pidf = config.get('webserver', 'webServer.pidFile')
    except:
        pass
    if 0 < len(pidf):
        f = open(pidf, 'w')
        f.write('%d\n' % os.getpid())
        f.close()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print '^C received, exiting'
        os._exit(1)
