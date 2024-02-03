#!/usr/bin/env python3
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

import configparser
import os
import platform
import socket
import socketserver
import sys
import time
from datetime import datetime
from http.server import SimpleHTTPRequestHandler
from io import StringIO
from urllib.request import urlopen

from browse import QFSBrowser
from chart import ChartData, ChartHTML
from chunks import (ChunkArrayData, ChunkDataManager, ChunkServerData,
                    ChunkThread, HtmlPrintData, HtmlPrintMetaData)

REQUEST_PING = (
    "PING\r\nVersion: KFS/1.0\r\n"
    "Cseq: 1\r\nClient-Protocol-Version: 116\r\n\r\n"
).encode("utf-8")
REQUEST_GET_DIRS_COUNTERS = (
    "GET_CHUNK_SERVER_DIRS_COUNTERS\r\n"
    "Version: KFS/1.0\r\nCseq: 1\r\nClient-Protocol-Version: 116\r\n\r\n"
).encode("utf-8")

gHasCollections = True
try:
    from collections import OrderedDict
except Exception:
    sys.stderr.write(
        "Warning: '%s'. Proceeding without collections.\n"
        % str(sys.exc_info()[1])
    )
    gHasCollections = False

gJsonSupported = True
try:
    import json

    class SetEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, set):
                return list(obj)
            return json.JSONEncoder.default(self, obj)

    import inspect

    class ObjectEncoder(json.JSONEncoder):
        def default(self, obj):
            if hasattr(obj, "to_json"):
                return self.default(obj.to_json())
            elif hasattr(obj, "__dict__"):
                d = dict(
                    (key, value)
                    for key, value in inspect.getmembers(obj)
                    if not key.startswith("__")
                    and not inspect.isabstract(value)
                    and not inspect.isbuiltin(value)
                    and not inspect.isfunction(value)
                    and not inspect.isgenerator(value)
                    and not inspect.isgeneratorfunction(value)
                    and not inspect.ismethod(value)
                    and not inspect.ismethoddescriptor(value)
                    and not inspect.isroutine(value)
                )
                return self.default(d)
            return obj

except ImportError:
    sys.stderr.write(
        "Warning: '%s'. Proceeding without query support.\n"
        % str(sys.exc_info()[1])
    )
    gJsonSupported = False

metaserverPort = 20000
metaserverHost = "127.0.0.1"
spaceTotal = 0
docRoot = "."
displayName = ""
autoRefresh = 60
displayPorts = False
displayChunkServerStorageTiers = True
myWebserverPort = 20001
objectStoreMode = False

kServerName = "XMeta-location"  # todo - put it to config file
kChunkDirName = "Chunk-server-dir"
kChunks = 1
kMeta = 2
kChart = 3
kBrowse = 4
kChunkDirs = 5
cMeta = 6
kConfig = 7
kVrStatus = 8

kHtmlEscapeTable = {
    "&": "&amp;",
    '"': "&quot;",
    "'": "&apos;",
    ">": "&gt;",
    "<": "&lt;",
}


def split_ip(ip):
    """Split a IP address given as string into a 4-tuple of integers."""
    return tuple(int(part) for part in ip.split("."))


def htmlEscape(text):
    """Produce entities within text."""
    return "".join(kHtmlEscapeTable.get(c, c) for c in text)


def showRate(rate, div):
    if rate < 100 * div:
        return "%.2f" % (rate / float(div))
    return splitThousands(rate / div)


def showUptime(uptime):
    seconds = uptime % 60
    rem = uptime / 60
    minutes = rem % 60
    rem = rem / 60
    hours = rem % 24
    days = rem / 24
    return "%d&nbsp;days,&nbsp;%02d:%02d:%02d" % (
        days,
        hours,
        minutes,
        seconds,
    )


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
        self.buildVersion = ""
        self.sourceVersion = ""
        self.replications = -1
        self.pendingRecovery = -1
        self.openFilesCount = -1
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
        self.internalNodeSize = -1
        self.internalNodeAllocSize = -1
        self.dentries = -1
        self.dentrySize = -1
        self.dentryAllocSize = -1
        self.fattrs = -1
        self.fattrSize = -1
        self.fattrAllocSize = -1
        self.cinfos = -1
        self.cinfoSize = -1
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
        self.watchDogPolls = -1
        self.watchDogTimeouts = -1
        self.watchDogTimerOverruns = -1
        self.watchDogTimerOverrunUsecs = -1
        self.watchDogTimeSinseLastTimerOverrunUsecs = -1
        self.checkpointTimeSinceLastRunStart = -1
        self.checkpointTimeSinceLastRunEnd = -1
        self.checkpointConsecutiveFailures = -1
        self.checkpointInterval = -1
        self.objectStoreDeleteNoTier = -1


class Status:
    def __init__(self):
        self.upServers = []
        self.downServers = []
        self.retiringServers = {}
        self.evacuatingServers = {}
        self.serversByRack = {}
        self.numReallyDownServers = 0
        self.freeFsSpace = 0
        self.canNotBeUsedForPlacement = 0
        self.goodNoRackAssignedCount = 0
        self.rebalanceStatus = {}
        self.tiersColumnNames = {}
        self.tiersInfo = {}
        self.config = {}
        self.vrStatus = {}
        self.watchdog = {}
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
            self.canNotBeUsedForPlacement,
            self.goodNoRackAssignedCount,
            self.systemInfo,
            self.tiersColumnNames,
            self.tiersInfo,
            self.config,
            self.vrStatus,
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
        vrStatus,
    ):
        global gQfsBrowser
        if gQfsBrowser.browsable:
            browseLink = '<A href="/browse-it">Browse Filesystem</A>'
        else:
            browseLink = ""
        print(
            """
    <body class="oneColLiqCtr">
    <div id="container">
      <div id="mainContent">
        <h1> QFS Status """,
            displayName,
            """</h1>
        <P>
            <A href="/chunk-it">Chunk Servers Status</A>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            <A href="/meta-it">Meta Server Status</A>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            <A href="/chunkdir-it">Chunk Directories Status</A>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            <A href="/meta-conf-html">Meta Server Configuration</A>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;""",
            file=buffer,
        )
        if 0 <= systemInfo.vrNodeId:
            print(
                """
            <A href="/meta-vr-status-html">
            Meta Server Viewstamped Replication Status</A>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;""",
                file=buffer,
            )
        print(
            """
            %s
        </P>
        <div class="info-table">
        <table cellspacing="0" cellpadding="0.1em">
        <tbody>"""
            % browseLink,
            file=buffer,
        )

        if systemInfo.isInRecovery and 0 != systemInfo.vrPrimaryFlag:
            print(
                """<tr><td>Recovery status: </td><td>:</td>
                <td>IN RECOVERY</td></tr>""",
                file=buffer,
            )
        fsFree = systemInfo.freeFsSpace
        if fsFree < 0:
            fsFree = freeFsSpace
        if systemInfo.totalSpace > 0:
            freePct = fsFree * 100.0 / float(systemInfo.totalSpace)
        else:
            freePct = 0.0
        serverCount = len(upServers)
        print(
            """
        <tr> <td> Updated </td><td>:</td><td> """,
            time.strftime("%a %b %d %H:%M:%S %Y"),
            """ </td></tr>
        <tr> <td> Started at </td><td>:</td><td> """,
            systemInfo.startedAt
            + "&nbsp;uptime:&nbsp;"
            + showUptime(systemInfo.uptime),
            """ </td></tr>""",
            file=buffer,
        )
        if 0 != systemInfo.vrPrimaryFlag:
            print(
                """
            <tr> <td> Space </td><td>:</td><td> total:&nbsp;"""
                + bytesToReadable(systemInfo.totalSpace)
                + "&nbsp;used:&nbsp;"
                + bytesToReadable(systemInfo.usedSpace)
                + "&nbsp;free:&nbsp;"
                + bytesToReadable(fsFree)
                + ("&nbsp;%.2f%%" % freePct)
                + " </td></tr>",
                file=buffer,
            )
        print(
            """
        <tr> <td> WORM mode </td><td>:</td><td> """,
            systemInfo.wormMode,
            """</td></tr>""",
            file=buffer,
        )
        if 0 < systemInfo.fileSystemId:
            print(
                "<tr> <td> File system </td><td>:</td><td>directories:&nbsp;"
                + splitThousands(systemInfo.dirCount)
                + "&nbsp;files:&nbsp;"
                + splitThousands(systemInfo.fileCount)
                + "&nbsp;open:&nbsp;"
                + splitThousands(systemInfo.openFilesCount)
                + "&nbsp;sum&nbsp;of&nbsp;logical&nbsp;file&nbsp;sizes:&nbsp;"
                + bytesToReadable(systemInfo.sumOfLogicalFileSizes)
                + "&nbsp;ID:&nbsp;"
                + str(systemInfo.fileSystemId)
                + "</td></tr>",
                file=buffer,
            )
        if 0 < systemInfo.objStoreEnabled:
            print(
                (
                    "<tr> <td> Object store delete queue</td><td>:</td>"
                    "<td>size:&nbsp;"
                )
                + splitThousands(systemInfo.objStoreDeletes)
                + "&nbsp;in&nbsp;flight:&nbsp;"
                + splitThousands(systemInfo.objStoreDeletesInFlight)
                + "&nbsp;re-queue:&nbsp;"
                + splitThousands(systemInfo.objStoreDeletesRetry)
                + "&nbsp;no&nbsp;tier&nbsp;errors:&nbsp;"
                + splitThousands(systemInfo.objectStoreDeleteNoTier)
                + "&nbsp;frst&nbsp;queued:&nbsp;"
                + str(systemInfo.objStoreDeletesStartedAgo)
                + "&nbsp;seconds&nbsp;ago</td></tr>",
                file=buffer,
            )
        if 0 <= systemInfo.logPendingOpsCount:
            if systemInfo.logTimeOpsCount:
                avg = systemInfo.logTimeUsec / systemInfo.logTimeOpsCount
                opWriteAvg = (
                    systemInfo.logDiskWriteUsec / systemInfo.logTimeOpsCount
                )
            else:
                avg = 0
                opWriteAvg = 0
            if 0 < systemInfo.uptime:
                rate = (
                    systemInfo.logTimeOpsCount
                    * systemInfo.logAvgReqRateDiv
                    / systemInfo.uptime
                )
            else:
                rate = 0
            if 0 < systemInfo.logTotalRequestCount:
                droppedPct = (
                    100.0
                    * systemInfo.logExceedQueueDepthFailedCount
                    / systemInfo.logTotalRequestCount
                )
            else:
                droppedPct = 0.0
            print(
                "<tr> <td> Transaction log </td><td>:</td><td>"
                + "queue&nbsp;depth:&nbsp;"
                + splitThousands(systemInfo.logPendingOpsCount)
                + "/"
                + bytesToReadable(systemInfo.logPendingAckByteCount)
                + ";&nbsp;dropped:&nbsp;"
                + splitThousands(
                    systemInfo.logExceedLogQueueDepthFailureCount300SecAvg
                )
                + "/%.2e%%" % droppedPct
                + (
                    ";&nbsp;request&nbsp;rate&nbsp;&amp;&nbsp;time&nbsp;"
                    "usec.&nbsp;total/disk"
                    "&nbsp;[5;&nbsp;10;&nbsp;15&nbsp;sec.;"
                    "&nbsp;total&nbsp;averages]:&nbsp;"
                )
                + showRate(
                    systemInfo.log5SecAvgReqRate, systemInfo.logAvgReqRateDiv
                )
                + "&nbsp;"
                + splitThousands(systemInfo.log5SecAvgUsec)
                + "/"
                + splitThousands(systemInfo.logOpWrite5SecAvgUsec)
                + ";&nbsp;"
                + showRate(
                    systemInfo.log10SecAvgReqRate, systemInfo.logAvgReqRateDiv
                )
                + "&nbsp;"
                + splitThousands(systemInfo.log10SecAvgUsec)
                + "/"
                + splitThousands(systemInfo.logOpWrite10SecAvgUsec)
                + ";&nbsp;"
                + showRate(
                    systemInfo.log15SecAvgReqRate, systemInfo.logAvgReqRateDiv
                )
                + "&nbsp;"
                + splitThousands(systemInfo.log15SecAvgUsec)
                + "/"
                + splitThousands(systemInfo.logOpWrite15SecAvgUsec)
                + ";&nbsp;"
                + showRate(rate, systemInfo.logAvgReqRateDiv)
                + "&nbsp;"
                + splitThousands(avg)
                + "/"
                + splitThousands(opWriteAvg)
                + "</td></tr>",
                file=buffer,
            )
        print(
            """<tr> <td> Meta server viewstamped replication (VR)
            </td><td>:</td><td> """,
            file=buffer,
        )
        if systemInfo.vrNodeId < 0 or len(vrStatus) <= 0:
            print("""not&nbsp;configured""", file=buffer)
        else:
            textBuf = ""
            try:
                textBuf = (
                    "node&nbsp;id:&nbsp;"
                    + vrStatus["vr.nodeId"]
                    + "&nbsp;state:&nbsp;"
                    + htmlEscape(vrStatus["vr.state"])
                )
                if "0" == vrStatus["vr.active"]:
                    textBuf = textBuf + "&nbsp;inactive"
                else:
                    status = int(vrStatus["vr.status"])
                    if 0 != status:
                        primaryId = vrStatus["vr.primaryId"]
                        textBuf = (
                            textBuf
                            + "&nbsp;primary&nbsp;node&nbsp;id:&nbsp;"
                            + primaryId
                        )
                        for k in vrStatus:
                            if (
                                k.startswith("configuration.node.")
                                and k.endswith(".id")
                                and vrStatus[k] == primaryId
                            ):
                                try:
                                    host = vrStatus[
                                        k.replace(".id", ".listener")
                                    ].split()[0]
                                    textBuf += (
                                        '&nbsp;host:&nbsp;<A href="http://'
                                    )
                                    textBuf += host
                                    textBuf += ":"
                                    textBuf += str(myWebserverPort)
                                    textBuf += '/">'
                                    textBuf += host
                                    textBuf += "</A>"
                                except Exception:
                                    pass
                    try:
                        viewTime = int(vrStatus["vr.currentTime"]) - int(
                            vrStatus["vr.viewChangeStartTime"]
                        )
                        textBuf += "&nbsp;view started:&nbsp;"
                        textBuf += showUptime(viewTime)
                        textBuf += "&nbsp;ago&nbsp;reason:&nbsp;"
                        textBuf += htmlEscape(vrStatus["vr.viewChangeReason"])
                    except Exception:
                        pass
                    if 0 == status:
                        try:
                            textBuf += "&nbsp;up&nbsp;nodes:&nbsp;"
                            textBuf += vrStatus[
                                "logTransmitter.activeUpNodesCount"
                            ]
                            textBuf += "&nbsp;channels:&nbsp;"
                            textBuf += vrStatus[
                                "logTransmitter.activeUpChannelsCount"
                            ]
                        except Exception:
                            pass
                print(textBuf, file=buffer)
            except Exception:
                print("""VR&nbsp;status&nbsp;parse&nbsp;errror""", file=buffer)
        print(
            """</td></tr>
        <tr> <td> Chunk servers</td><td>:</td><td> alive:&nbsp;"""
            + splitThousands(serverCount)
            + """&nbsp;dead:&nbsp;"""
            + splitThousands(numReallyDownServers)
            + """&nbsp;retiring:&nbsp;"""
            + splitThousands(len(retiringServers)),
            file=buffer,
        )
        if systemInfo.hibernatedServerCount >= 0:
            print(
                """&nbsp;hibernated:&nbsp;"""
                + splitThousands(systemInfo.hibernatedServerCount),
                file=buffer,
            )
        print("""</td></tr>""", file=buffer)
        if systemInfo.replications >= 0:
            print(
                """<tr> <td> Replications </td><td>:</td><td>in&nbsp;flight:
                &nbsp;"""
                + str(systemInfo.replications)
                + """&nbsp;check:&nbsp;"""
                + splitThousands(systemInfo.replicationsCheck)
                + """&nbsp;pending:&nbsp;"""
                + splitThousands(systemInfo.pendingReplication)
                + """&nbsp;recovery:&nbsp;"""
                + splitThousands(systemInfo.pendingRecovery)
                + """&nbsp;delayed:&nbsp;"""
                + splitThousands(systemInfo.delayedRecovery)
                + """&nbsp;backlog:&nbsp;"""
                + splitThousands(systemInfo.replicationBacklog)
                + """</td></tr>""",
                file=buffer,
            )
        if systemInfo.clients >= 0:
            print(
                """<tr> <td> Allocations </td><td>:</td><td>clients:&nbsp;"""
                + splitThousands(systemInfo.clients),
                file=buffer,
            )
            if 0 <= systemInfo.maxClients:
                print(
                    """&nbsp;(max:&nbsp;"""
                    + splitThousands(systemInfo.maxClients)
                    + ")",
                    file=buffer,
                )
            print(
                """&nbsp;chunk&nbsp;servers:&nbsp;"""
                + splitThousands(systemInfo.chunkServers),
                file=buffer,
            )
            if 0 <= systemInfo.maxChunkServers:
                print(
                    """&nbsp;(max:&nbsp;"""
                    + splitThousands(systemInfo.maxChunkServers)
                    + ")",
                    file=buffer,
                )
            print(
                """&nbsp;requests:&nbsp;"""
                + splitThousands(systemInfo.allocatedRequests)
                + """&nbsp;buffers:&nbsp;"""
                + splitThousands(systemInfo.usedBuffers),
                file=buffer,
            )
            if 0 <= systemInfo.totalBuffers:
                print(
                    """&nbsp;(max:&nbsp;"""
                    + splitThousands(systemInfo.totalBuffers)
                    + ")",
                    file=buffer,
                )
            print(
                """&nbsp;sockets:&nbsp;"""
                + splitThousands(systemInfo.sockets)
                + """&nbsp;chunks:&nbsp;"""
                + splitThousands(systemInfo.chunks),
                file=buffer,
            )
            if systemInfo.appendCacheSize >= 0:
                print(
                    """&nbsp;append cache:&nbsp;"""
                    + splitThousands(systemInfo.appendCacheSize),
                    file=buffer,
                )
            print("""</td></tr>""", file=buffer)
        if systemInfo.internalNodes >= 0:
            print(
                """<tr> <td> Allocations&nbsp;b+tree</td><td>:</td><td>
                internal:&nbsp;"""
                + splitThousands(systemInfo.internalNodes)
                + """x"""
                + splitThousands(systemInfo.internalNodeSize)
                + """&nbsp;"""
                + bytesToReadable(systemInfo.internalNodeAllocSize)
                + """&nbsp;dent:&nbsp;"""
                + splitThousands(systemInfo.dentries)
                + """x"""
                + splitThousands(systemInfo.dentrySize)
                + """&nbsp;"""
                + bytesToReadable(systemInfo.dentryAllocSize)
                + """&nbsp;fattr:&nbsp;"""
                + splitThousands(systemInfo.fattrs)
                + """x"""
                + splitThousands(systemInfo.fattrSize)
                + """&nbsp;"""
                + bytesToReadable(systemInfo.fattrAllocSize)
                + """&nbsp;cinfo:&nbsp;"""
                + splitThousands(systemInfo.cinfos)
                + """x"""
                + splitThousands(systemInfo.cinfoSize)
                + """&nbsp;"""
                + bytesToReadable(systemInfo.cinfoAllocSize)
                + """&nbsp;tree&nbsp;height:&nbsp;"""
                + splitThousands(systemInfo.bTreeHeight)
                + """</td></tr>""",
                file=buffer,
            )
        if systemInfo.csmapNodes >= 0:
            print(
                """<tr> <td> Allocations&nbsp;chunk2server</td><td>:</td><td>
                nodes:&nbsp;"""
                + splitThousands(systemInfo.csmapNodes)
                + """x"""
                + splitThousands(systemInfo.csmapNodeSize)
                + """&nbsp;"""
                + bytesToReadable(systemInfo.csmapAllocSize)
                + """&nbsp;srv&nbsp;list:&nbsp;"""
                + splitThousands(systemInfo.csmapEntryAllocs)
                + """&nbsp;"""
                + bytesToReadable(systemInfo.csmapEntryBytes)
                + """</td></tr>""",
                file=buffer,
            )
        allGood = 0
        if 0 != systemInfo.vrPrimaryFlag:
            if systemInfo.csMaxGoodCandidateLoadAvg >= 0:
                print(
                    """<tr> <td>Chunk&nbsp;placement&nbsp;load&nbsp;
                    threshold</td><td>:</td><td>"""
                    + "avg:&nbsp;%5.2e" % systemInfo.csMaxGoodCandidateLoadAvg
                    + "&nbsp;"
                    + "&nbsp;master:&nbsp;%5.2e"
                    % systemInfo.csMaxGoodMasterLoadAvg
                    + "&nbsp;slave:&nbsp;%5.2e"
                    % systemInfo.csMaxGoodSlaveLoadAvg
                    + """</td></tr>""",
                    file=buffer,
                )
            if serverCount <= 0:
                mult = 0
            else:
                mult = 100.0 / float(serverCount)
            print(
                """<tr> <td>Chunk&nbsp;placement&nbsp;candidates</td>
                <td>:</td><td>""",
                file=buffer,
            )
            if systemInfo.goodMasters >= 0 and systemInfo.goodSlaves >= 0:
                allGood = systemInfo.goodMasters + systemInfo.goodSlaves
                print(
                    "all:&nbsp;"
                    + splitThousands(allGood)
                    + "&nbsp;%.2f%%" % (float(allGood) * mult)
                    + "&nbsp;masters:&nbsp;"
                    + splitThousands(systemInfo.goodMasters)
                    + "&nbsp;%.2f%%" % (float(systemInfo.goodMasters) * mult)
                    + "&nbsp;slaves:&nbsp"
                    + splitThousands(systemInfo.goodSlaves)
                    + "&nbsp;%.2f%%" % (float(systemInfo.goodSlaves) * mult),
                    file=buffer,
                )
            else:
                allGood = serverCount - canNotBeUsedForPlacment
                print(
                    "all:&nbsp;"
                    + splitThousands(allGood)
                    + "&nbsp;%.2f%%" % (float(allGood) * mult),
                    file=buffer,
                )
            if goodNoRackAssignedCount < allGood:
                all = allGood - goodNoRackAssignedCount
                print(
                    "&nbsp;in&nbsp;racks:&nbsp;"
                    + splitThousands(all)
                    + "&nbsp;%.2f%%" % (float(all) * mult),
                    file=buffer,
                )
            print("""</td></tr>""", file=buffer)
            if systemInfo.totalDrives >= 0 and systemInfo.writableDrives >= 0:
                if systemInfo.totalDrives > 0:
                    mult = 100.0 / systemInfo.totalDrives
                else:
                    mult = 0.0
                print(
                    """<tr> <td>Storage devices&nbsp;</td><td>:</td><td>"""
                    + "total:&nbsp;"
                    + splitThousands(systemInfo.totalDrives)
                    + "&nbsp;writable:&nbsp;"
                    + splitThousands(systemInfo.writableDrives)
                    + "&nbsp;%.2f%%"
                    % (float(systemInfo.writableDrives) * mult)
                    + "&nbsp;avg&nbsp;capacity:&nbsp;"
                    + bytesToReadable(systemInfo.totalSpace * mult / 100.0),
                    """</td></tr>""",
                    file=buffer,
                )

        print(
            """
        <tr><td>Version </td><td>:</td><td> """,
            systemInfo.buildVersion,
            """</td></tr>
        <tr><td>Source </td><td>:</td><td> """,
            systemInfo.sourceVersion,
            """</td></tr>
        </tbody>
        </table>
        </div>
        <br />
        """,
            file=buffer,
        )

        if len(evacuatingServers) > 0:
            print(
                """
            <div class="floatleft">
             <table class="sortable status-table" id="tableEvacuating"
                cellspacing="0" cellpadding="0.1em"
                summary="Status of evacuating nodes in the system">
             <caption> <a name="EvacuatingNodes">Evacuating Nodes Status</a>
             </caption>
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
            """,
                file=buffer,
            )
            count = 0
            for v in evacuatingServers:
                v.printStatusHTML(buffer)
                count = count + 1
            print(
                """
            </tbody>
            </table></div>""",
                file=buffer,
            )

        colCount = len(tiersColumnNames)
        if colCount > 0 and len(tiersInfo) >= colCount:
            print(
                """
            <div class="floatleft">
             <table class="sortable status-table" id="tiersInfo"
                cellspacing="0" cellpadding="0.1em"
                summary="Status of storage tiers in the system">
             <caption> <a name="StorageTiers">
                Storage Tiers Available For Placement Status</a> </caption>
             <thead>
             <tr>
            """,
                file=buffer,
            )
            conv = {}
            colCnt = 0
            for col in tiersColumnNames:
                conv[colCnt] = ""
                if col == "%util.":
                    col = "used%"
                    conv[colCnt] = "%.2f"
                elif col == "space-available":
                    col = "free"
                    conv[colCnt] = "%.2e"
                elif col == "total-space":
                    col = "total"
                    conv[colCnt] = "%.2e"
                elif col == "devices":
                    col = "writable dev."
                elif col == "wr-chunks":
                    col = "writable blocks"
                elif col == "chunks":
                    col = "blocks"
                colCnt = colCnt + 1
                print("""<th>""", col.capitalize(), """</th>""", file=buffer)
            print(
                """
             </tr>
             </thead>
             <tbody>
            """,
                file=buffer,
            )
            rowCnt = 0
            colCnt = 0
            for val in tiersInfo:
                if colCnt == 0:
                    print("""<tr>""", file=buffer)
                if conv[colCnt] == "s":
                    v = bytesToReadable(val)
                elif conv[colCnt] == "":
                    v = val
                else:
                    v = conv[colCnt] % float(val)
                print("""<td align="right">""", v, """</td>""", file=buffer)
                colCnt = colCnt + 1
                if colCnt >= colCount:
                    print("""</tr>""", file=buffer)
                    colCnt = 0
                    rowCnt = rowCnt + 1
            if colCnt > 0:
                while colCnt < colCount:
                    colCnt = colCnt + 1
                    print("""<td> </td>""", file=buffer)
                print("""</tr>""", file=buffer)
            print(
                """
            </tbody>
            </table>
            </div>
            """,
                file=buffer,
            )

        print(
            """
        <div class="floatleft">
         <table class="sortable status-table" id="table1" cellspacing="0"
            cellpadding="0.1em"
            summary="Status of nodes in the system: who is up/down and when we
            last heard from them">
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
        """,
            file=buffer,
        )
        count = 0
        showNoRack = goodNoRackAssignedCount < allGood
        for v in upServers:
            v.printStatusHTML(buffer, count, showNoRack)
            count += 1
        print(
            """
        </tbody>
        </table></div>""",
            file=buffer,
        )

        if len(retiringServers) > 0:
            print(
                """
            <div class="floatleft">
             <table class="status-table" cellspacing="0" cellpadding="0.1em"
             summary="Status of retiring nodes in the system">
             <caption> <a name="RetiringNodes">Retiring Nodes Status</a>
             </caption>
             <thead>
             <tr><th> Chunkserver </th> <th> Start </th> <th>  # blks done
             </th> <th> # blks left </th> </tr>
             </thead>
             <tbody>
            """,
                file=buffer,
            )
            count = 0
            for v in retiringServers:
                v.printStatusHTML(buffer, count)
                count = count + 1
            print(
                """
            </tbody>
            </table></div>""",
                file=buffer,
            )

        if len(downServers) > 0:
            print(
                """<div class="floatleft">
            <table class="status-table" cellspacing="0" cellpadding="0.1em"
                summary="Status of down nodes in the system">
            <caption> <a name="DeadNodes">Dead Nodes History</a></caption>
         <thead>
            <tr><th> Chunkserver </th> <th> Down Since </th> <th> Reason </th>
            </tr>
         </thead>
         <tbody>
            """,
                file=buffer,
            )
            count = 0
            for v in reversed(downServers):
                v.printStatusHTML(buffer, count)
                count = count + 1
            print(
                """
            </tbody>
            </table></div>""",
                file=buffer,
            )

        print(
            """
        </div>
        </div>
        </body>
        </html>""",
            file=buffer,
        )


# beginning of html
def printStyle(buffer, title):
    print(
        """
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
    "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<link rel="stylesheet" type="text/css" href="files/qfsstyle.css">
<script type="text/javascript" src="files/sorttable/sorttable.js"></script>
<title>""",
        title,
        displayName,
        """</title>
</head>
""",
        file=buffer,
    )


class DownServer:
    """Keep track of a potentially down server"""

    def __init__(self, info):
        serverInfo = info.split(",")
        for i in range(len(serverInfo)):
            s = serverInfo[i].split("=")
            setattr(self, s[0].strip(), s[1].strip())

        if hasattr(self, "s"):
            setattr(self, "host", self.s)
            delattr(self, "s")

        if hasattr(self, "p"):
            setattr(self, "port", self.p)
            delattr(self, "p")

        if hasattr(self, "host"):
            self.displayName = self.host
        else:
            self.displayName = "unknown"
        if displayPorts and hasattr(self, "port"):
            self.displayName += ":" + str(self.port)

        self.stillDown = 0

    def setStillDown(self):
        self.stillDown = 1

    def printStatusHTML(self, buffer, count):
        if count % 2 == 0:
            trclass = ""
        else:
            trclass = "class=odd"

        if self.stillDown:
            trclass = "class=dead"

        print(
            """<tr """,
            trclass,
            """><td align="center">""",
            self.displayName,
            """</td>""",
            file=buffer,
        )
        print("""<td>""", self.down, """</td>""", file=buffer)
        print("""<td>""", self.reason, """</td>""", file=buffer)
        print("""</tr>""", file=buffer)


class RetiringServer:
    """Keep track of a retiring server"""

    def __init__(self, info):
        serverInfo = info.split(",")
        for i in range(len(serverInfo)):
            s = serverInfo[i].split("=")
            setattr(self, s[0].strip(), s[1].strip())

        self.sort_key = ()
        # tbd order by started
        # if hasattr(self, 'started'):
        #   self.sort_key = (0,0,0,0,0,0,self.started)

        if hasattr(self, "s"):
            setattr(self, "host", self.s)
            if not self.sort_key:
                self.sort_key = split_ip(socket.gethostbyname(self.host))
            delattr(self, "s")

        if hasattr(self, "p"):
            setattr(self, "port", self.p)
            delattr(self, "p")
            self.sort_key += (self.port,)

        if hasattr(self, "host"):
            self.displayName = self.host
        else:
            self.displayName = "unknown"
        if displayPorts and hasattr(self, "port"):
            self.displayName += ":" + str(self.port)

    # Order by IP
    def __lt__(self, other):
        return self.sort_key < other.sort_key

    def __gt__(self, other):
        return self.sort_key > other.sort_key

    def __le__(self, other):
        return self.sort_key <= other.sort_key

    def __ge__(self, other):
        return self.sort_key >= other.sort_key

    def printStatusHTML(self, buffer, count):
        if count % 2 == 0:
            trclass = ""
        else:
            trclass = "class=odd"

        print(
            """<tr """,
            trclass,
            """><td align="center">""",
            self.displayName,
            """</td>""",
            file=buffer,
        )
        print("""<td>""", self.started, """</td>""", file=buffer)
        print("""<td align="right">""", self.numDone, """</td>""", file=buffer)
        print("""<td align="right">""", self.numLeft, """</td>""", file=buffer)
        print("""</tr>""", file=buffer)


class EvacuatingServer:
    """Keep track of a evacuating server"""

    def __init__(self, info):
        serverInfo = info.split(",")
        for i in range(len(serverInfo)):
            s = serverInfo[i].split("=")
            setattr(self, s[0].strip(), s[1].strip())

        if hasattr(self, "s"):
            setattr(self, "host", self.s)
            self.sort_key = split_ip(socket.gethostbyname(self.host))
            delattr(self, "s")
        else:
            self.sort_key = ()

        if hasattr(self, "p"):
            setattr(self, "port", self.p)
            delattr(self, "p")
            self.sort_key += (self.port,)

        if hasattr(self, "host"):
            self.displayName = self.host
        else:
            self.displayName = "unknown"
        if displayPorts and hasattr(self, "port"):
            self.displayName += ":" + str(self.port)

    # Order by IP
    def __lt__(self, other):
        return self.sort_key < other.sort_key

    def __gt__(self, other):
        return self.sort_key > other.sort_key

    def __le__(self, other):
        return self.sort_key <= other.sort_key

    def __ge__(self, other):
        return self.sort_key >= other.sort_key

    def printStatusHTML(self, buffer):
        print(
            """
        <tr><td align="right">""",
            self.displayName,
            """</td>
        <td align="right">""",
            self.cDone,
            """</td>
        <td align="right">""",
            "%.2e" % float(self.bDone),
            """</td>
        <td align="right">""",
            self.c,
            """</td>
        <td align="right">""",
            "%.2e" % float(self.b),
            """</td>
        <td align="right">""",
            self.cFlight,
            """</td>
        <td align="right">""",
            self.cPend,
            """</td>
        <td align="right">""",
            "%.2e" % float(self.cSec),
            """</td>
        <td align="right">""",
            "%.2e" % float(self.bSec),
            """</td>
        <td align="right">""",
            "%.2f" % (float(self.eta) / 60),
            """</td>
        </tr>""",
            file=buffer,
        )


def formatConv(val):
    v = val.split("(")
    ret = float(v[0])
    if len(v) > 1:
        if v[1] == "KB)":
            ret = ret * 1024
        elif v[1] == "MB)":
            ret = ret * 1024 * 1024
        elif v[1] == "GB)":
            ret = ret * 1024 * 1024 * 1024
        elif v[1] == "TB)":
            ret = ret * 1024 * 1024 * 1024 * 1024
    return ret


class UpServer:
    """Keep track of an up server state"""

    def __init__(self, status, info):
        if isinstance(info, str):
            serverInfo = info.split(",")
            # order here is host, port, rack, used, free, util, nblocks, last
            # heard, nblks corrupt, numDrives
            for i in range(len(serverInfo)):
                s = serverInfo[i].split("=")
                setattr(self, s[0].strip(), s[1].strip())

            if hasattr(self, "numDrives"):
                self.numDrives = int(self.numDrives)
            else:
                setattr(self, "numDrives", 0)

            if hasattr(self, "ncorrupt"):
                self.ncorrupt = int(self.ncorrupt)
            else:
                setattr(self, "ncorrupt", 0)

            if hasattr(self, "s"):
                setattr(self, "host", self.s)
                setattr(self, "ip", socket.gethostbyname(self.s))
                self.sort_key = split_ip(self.ip)
                delattr(self, "s")
            else:
                self.sort_key = ()

            if hasattr(self, "p"):
                setattr(self, "port", self.p)
                delattr(self, "p")
                self.sort_key += (self.port,)

            if hasattr(self, "overloaded"):
                self.overloaded = int(self.overloaded) != 0
            else:
                setattr(self, "overloaded", False)

            if hasattr(self, "nevacuate"):
                self.nevacuate = int(self.nevacuate)
            else:
                setattr(self, "nevacuate", 0)

            if hasattr(self, "bytesevacuate"):
                self.bytesevacuate = int(self.bytesevacuate)
            else:
                setattr(self, "bytesevacuate", int(0))

            if hasattr(self, "good"):
                self.overloaded = int(self.overloaded) != 0
            else:
                setattr(self, "good", True)

            if not hasattr(self, "total"):
                setattr(self, "total", "0")

            if hasattr(self, "numReplications"):
                self.numReplications = int(self.numReplications)
            else:
                setattr(self, "numReplications", 0)

            if hasattr(self, "numReadReplications"):
                self.numReadReplications = int(self.numReadReplications)
            else:
                setattr(self, "numReadReplications", 0)

            if hasattr(self, "numWritableDrives"):
                self.numWritableDrives = int(self.numWritableDrives)
            else:
                setattr(self, "numWritableDrives", self.numDrives)

            if hasattr(self, "rack"):
                self.rack = int(self.rack)
            else:
                setattr(self, "rack", -1)

            if hasattr(self, "nwrites"):
                self.nwrites = int(self.nwrites)
            else:
                setattr(self, "nwrites", -1)

            if hasattr(self, "load"):
                self.load = int(self.load)
            else:
                setattr(self, "load", -1)

            if hasattr(self, "tiers"):
                self.tiers = self.tiers
            else:
                setattr(self, "tiers", "")

            try:
                self.connected = int(self.connected)
            except Exception:
                self.connected = 1

            try:
                self.replay = int(self.replay)
            except Exception:
                self.replay = 0

            try:
                self.stopped = int(self.stopped)
            except Exception:
                self.stopped = 0

            try:
                self.chunks = int(self.chunks)
            except Exception:
                self.chunks = -1

            self.tiersCount = self.tiers.count(";") + 1
            if self.tiersCount <= 1 and self.tiers.find(":") < 0:
                self.tiersCount = 0

            self.lastheard = int(self.lastheard.split("(")[0])

            self.util = float(self.util.split("%")[0])
            self.used = formatConv(self.used)
            self.free = formatConv(self.free)
            self.total = formatConv(self.total)
            if self.total <= 0 and self.free > 0 and self.util > 0:
                if self.util < 99.9:
                    self.total = self.free * 100.0 / (100.0 - self.util)
                else:
                    self.total = self.used + self.free

            self.is_down = 0
            self.retiring = 0
            if hasattr(self, "host"):
                self.displayName = self.host
            else:
                self.displayName = "unknown"
            if displayPorts and hasattr(self, "port"):
                self.displayName += ":" + str(self.port)

            status.freeFsSpace += self.free
            if not self.good or self.overloaded:
                status.canNotBeUsedForPlacement += 1
            elif self.rack < 0:
                status.goodNoRackAssignedCount += 1

        if isinstance(info, DownServer):
            self.host = info.host
            self.port = info.port
            self.is_down = 1
            self.retiring = 0

    # Order by IP
    def __lt__(self, other):
        return self.sort_key < other.sort_key

    def __gt__(self, other):
        return self.sort_key > other.sort_key

    def __le__(self, other):
        return self.sort_key <= other.sort_key

    def __ge__(self, other):
        return self.sort_key >= other.sort_key

    def setRetiring(self, status):
        self.retiring = 1
        if not self.overloaded and self.good:
            status.canNotBeUsedForPlacement += 1
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
        elif not self.good or self.is_down:
            trclass = 'class="notgood"'
        elif showNoRack and self.rack < 0:
            trclass = 'class="norack"'
        else:
            trclass = ""

        print(
            """<tr """,
            trclass,
            """><td align="right">""",
            self.displayName,
            """</td>""",
            file=buffer,
        )
        print(
            """<td align="right">""", self.numDrives, """</td>""", file=buffer
        )
        print(
            """<td align="right">""",
            self.numWritableDrives,
            """</td>""",
            file=buffer,
        )
        print("""<td align="right">""", self.nwrites, """</td>""", file=buffer)
        if self.tiersCount > 0 and displayChunkServerStorageTiers:
            print(
                """
                <td align="right">
                    <div id="linkspandetailtable">
                        <a href="#">""",
                self.tiersCount,
                """
                            <span>
                            <div class="floatleft">
                            <table class="sortable status-table-span"
                                id="cs%stiers">"""
                % count,
                """
                            <thead><tr>
                                <th>Tier</th>
                                <th>Wr. dev.</th>
                                <th>Wr. blocks</th>
                                <th>Blocks</th>
                                <th>Free</th>
                                <th>Total</th>
                                <th>%Used</th>
                             </tr></thead><tbody><tr><td>
                                """,
                self.tiers.replace(";", "</td></tr><tr><td>").replace(
                    ":", "</td><td>"
                ),
                """
                            </td></tr></tbody></table></div></span>
                        </a>
                    </div>
                </td>
            """,
                file=buffer,
            )
        else:
            print(
                """<td align="right">""",
                self.tiersCount,
                """</td>""",
                file=buffer,
            )
        print("""<td>""", "%.2e" % self.used, """</td>""", file=buffer)
        print("""<td>""", "%.2e" % self.free, """</td>""", file=buffer)
        print("""<td>""", "%.2e" % self.total, """</td>""", file=buffer)
        print(
            """<td align="right">""",
            "%.2f" % self.util,
            """</td>""",
            file=buffer,
        )
        print("""<td align="right">""", self.nblocks, """</td>""", file=buffer)
        print("""<td align="right">""", self.chunks, """</td>""", file=buffer)
        print(
            """<td align="right">""", self.lastheard, """</td>""", file=buffer
        )
        print(
            """<td align="right">""",
            self.numReplications,
            """</td>""",
            file=buffer,
        )
        print(
            """<td align="right">""",
            self.numReadReplications,
            """</td>""",
            file=buffer,
        )
        print(
            """<td align="right">""", self.ncorrupt, """</td>""", file=buffer
        )
        print("""<td>""", "%.2e" % self.load, """</td>""", file=buffer)
        print(
            """<td align="right">""", self.rack, """</td></tr>""", file=buffer
        )


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

        print(
            """<tr """,
            trclass,
            """><td align="center">""",
            self.displayName,
            """</td> </tr>""",
            file=buffer,
        )


def nodeIsNotUp(status, d):
    x = [u for u in status.upServers if u.host == d.host and u.port == d.port]
    return len(x) == 0


def nodeIsRetiring(status, u):
    x = [
        r
        for r in status.retiringServers
        if u.host == r.host and u.port == r.port
    ]
    return len(x) > 0


def mergeDownUpNodes(status):
    """in the set of down-nodes, mark those that are still down in red"""
    reallyDown = [d for d in status.downServers if nodeIsNotUp(status, d)]
    uniqueServers = set()
    for d in reallyDown:
        d.setStillDown()
        s = "%s:%s" % (d.host, d.port)
        uniqueServers.add(s)
    status.numReallyDownServers = len(uniqueServers)


def mergeRetiringUpNodes(status):
    """merge retiring nodes with up nodes"""
    [
        u.setRetiring(status)
        for u in status.upServers
        if nodeIsRetiring(status, u)
    ]


def processUpNodes(status, nodes):
    servers = nodes.split("\t")
    status.upServers = [UpServer(status, c) for c in servers if c != ""]


def processDownNodes(status, nodes):
    servers = nodes.split("\t")
    if not servers:
        return
    status.downServers = [DownServer(c) for c in servers if c != ""]


def processRetiringNodes(status, nodes):
    servers = nodes.split("\t")
    if not servers:
        return
    status.retiringServers = [RetiringServer(c) for c in servers if c != ""]
    status.retiringServers.sort()


def processEvacuatingNodes(status, nodes):
    servers = nodes.split("\t")
    if not servers:
        return
    status.evacuatingServers = [
        EvacuatingServer(c) for c in servers if c != ""
    ]
    status.evacuatingServers.sort()


def bytesToReadable(b):
    v = int(b)
    if v > (int(1) << 50):
        return "%.2f&nbsp;PB" % (float(v) / (int(1) << 50))
    if v > (int(1) << 40):
        return "%.2f&nbsp;TB" % (float(v) / (int(1) << 40))
    if v > (int(1) << 30):
        return "%.2f&nbsp;GB" % (float(v) / (int(1) << 30))
    if v > (int(1) << 20):
        return "%.2f&nbsp;MB" % (float(v) / (int(1) << 20))
    return "%.2f&nbsp;bytes" % (v)


def processSystemInfo(systemInfo, sysInfo):
    info = sysInfo.split("\t")
    if len(info) < 3:
        return
    systemInfo.startedAt = info[0].split("=")[1]
    systemInfo.totalSpace = int(info[1].split("=")[1])
    systemInfo.usedSpace = int(info[2].split("=")[1])
    if len(info) < 4:
        return
    systemInfo.replications = int(info[3].split("=")[1])
    if len(info) < 5:
        return
    systemInfo.replicationsCheck = int(info[4].split("=")[1])
    if len(info) < 6:
        return
    systemInfo.pendingRecovery = int(info[5].split("=")[1])
    if len(info) < 8:
        return
    systemInfo.openFilesCount = int(info[7].split("=")[1])
    if len(info) < 10:
        return
    systemInfo.uptime = int(info[9].split("=")[1])
    if len(info) < 11:
        return
    systemInfo.usedBuffers = int(info[10].split("=")[1])
    if len(info) < 12:
        return
    systemInfo.clients = int(info[11].split("=")[1])
    if len(info) < 13:
        return
    systemInfo.chunkServers = int(info[12].split("=")[1])
    if len(info) < 14:
        return
    systemInfo.allocatedRequests = int(info[13].split("=")[1])
    if len(info) < 15:
        return
    systemInfo.sockets = int(info[14].split("=")[1])
    if len(info) < 16:
        return
    systemInfo.chunks = int(info[15].split("=")[1])
    if len(info) < 17:
        return
    systemInfo.pendingReplication = int(info[16].split("=")[1])
    if len(info) < 18:
        return
    systemInfo.internalNodes = int(info[17].split("=")[1])
    if len(info) < 19:
        return
    systemInfo.internalNodeSize = int(info[18].split("=")[1])
    if len(info) < 20:
        return
    systemInfo.internalNodeAllocSize = int(info[19].split("=")[1])
    if len(info) < 21:
        return
    systemInfo.dentries = int(info[20].split("=")[1])
    if len(info) < 22:
        return
    systemInfo.dentrySize = int(info[21].split("=")[1])
    if len(info) < 23:
        return
    systemInfo.dentryAllocSize = int(info[22].split("=")[1])
    if len(info) < 24:
        return
    systemInfo.fattrs = int(info[23].split("=")[1])
    if len(info) < 25:
        return
    systemInfo.fattrSize = int(info[24].split("=")[1])
    if len(info) < 26:
        return
    systemInfo.fattrAllocSize = int(info[25].split("=")[1])
    if len(info) < 27:
        return
    systemInfo.cinfos = int(info[26].split("=")[1])
    if len(info) < 28:
        return
    systemInfo.cinfoSize = int(info[27].split("=")[1])
    if len(info) < 29:
        return
    systemInfo.cinfoAllocSize = int(info[28].split("=")[1])
    if len(info) < 30:
        return
    systemInfo.csmapNodes = int(info[29].split("=")[1])
    if len(info) < 31:
        return
    systemInfo.csmapNodeSize = int(info[30].split("=")[1])
    if len(info) < 32:
        return
    systemInfo.csmapAllocSize = int(info[31].split("=")[1])
    if len(info) < 33:
        return
    systemInfo.csmapEntryAllocs = int(info[32].split("=")[1])
    if len(info) < 34:
        return
    systemInfo.csmapEntryBytes = int(info[33].split("=")[1])
    if len(info) < 35:
        return
    systemInfo.delayedRecovery = int(info[34].split("=")[1])
    if len(info) < 36:
        return
    systemInfo.replicationBacklog = int(info[35].split("=")[1])
    if len(info) < 37:
        return
    systemInfo.isInRecovery = int(info[36].split("=")[1]) != 0
    if len(info) < 38:
        return
    systemInfo.csToRestart = int(info[37].split("=")[1])
    if len(info) < 39:
        return
    systemInfo.csMastersToRestart = int(info[38].split("=")[1])
    if len(info) < 40:
        return
    systemInfo.csMaxGoodCandidateLoadAvg = int(info[39].split("=")[1])
    if len(info) < 41:
        return
    systemInfo.csMaxGoodMasterLoadAvg = int(info[40].split("=")[1])
    if len(info) < 42:
        return
    systemInfo.csMaxGoodSlaveLoadAvg = int(info[41].split("=")[1])
    if len(info) < 43:
        return
    systemInfo.hibernatedServerCount = int(info[42].split("=")[1])
    if len(info) < 44:
        return
    systemInfo.freeFsSpace = int(info[43].split("=")[1])
    if len(info) < 45:
        return
    systemInfo.goodMasters = int(info[44].split("=")[1])
    if len(info) < 46:
        return
    systemInfo.goodSlaves = int(info[45].split("=")[1])
    if len(info) < 47:
        return
    systemInfo.totalDrives = int(info[46].split("=")[1])
    if len(info) < 48:
        return
    systemInfo.writableDrives = int(info[47].split("=")[1])
    if len(info) < 49:
        return
    systemInfo.appendCacheSize = int(info[48].split("=")[1])
    if len(info) < 50:
        return
    systemInfo.maxClients = int(info[49].split("=")[1])
    if len(info) < 51:
        return
    systemInfo.maxChunkServers = int(info[50].split("=")[1])
    if len(info) < 52:
        return
    systemInfo.totalBuffers = int(info[51].split("=")[1])
    if len(info) < 53:
        return
    systemInfo.objStoreEnabled = int(info[52].split("=")[1])
    if len(info) < 54:
        return
    systemInfo.objStoreDeletes = int(info[53].split("=")[1])
    if len(info) < 55:
        return
    systemInfo.objStoreDeletesInFlight = int(info[54].split("=")[1])
    if len(info) < 56:
        return
    systemInfo.objStoreDeletesRetry = int(info[55].split("=")[1])
    if len(info) < 57:
        return
    systemInfo.objStoreDeletesStartedAgo = int(info[56].split("=")[1])
    if len(info) < 58:
        return
    systemInfo.fileCount = int(info[57].split("=")[1])
    if len(info) < 59:
        return
    systemInfo.dirCount = int(info[58].split("=")[1])
    if len(info) < 60:
        return
    systemInfo.sumOfLogicalFileSizes = int(info[59].split("=")[1])
    if len(info) < 61:
        return
    systemInfo.fileSystemId = int(info[60].split("=")[1])
    if len(info) < 62:
        return
    systemInfo.vrPrimaryFlag = int(info[61].split("=")[1])
    if len(info) < 63:
        return
    systemInfo.vrNodeId = int(info[62].split("=")[1])
    if len(info) < 64:
        return
    systemInfo.vrPrimaryNodeId = int(info[63].split("=")[1])
    if len(info) < 65:
        return
    systemInfo.vrActiveFlag = int(info[64].split("=")[1])
    if len(info) < 66:
        return
    systemInfo.logTimeUsec = int(info[65].split("=")[1])
    if len(info) < 67:
        return
    systemInfo.logTimeOpsCount = int(info[66].split("=")[1])
    if len(info) < 68:
        return
    systemInfo.logPendingOpsCount = int(info[67].split("=")[1])
    if len(info) < 69:
        return
    systemInfo.log5SecAvgUsec = int(info[68].split("=")[1])
    if len(info) < 70:
        return
    systemInfo.log10SecAvgUsec = int(info[69].split("=")[1])
    if len(info) < 71:
        return
    systemInfo.log15SecAvgUsec = int(info[70].split("=")[1])
    if len(info) < 72:
        return
    systemInfo.log5SecAvgReqRate = int(info[71].split("=")[1])
    if len(info) < 73:
        return
    systemInfo.log10SecAvgReqRate = int(info[72].split("=")[1])
    if len(info) < 74:
        return
    systemInfo.log15SecAvgReqRate = int(info[73].split("=")[1])
    if len(info) < 75:
        return
    systemInfo.logAvgReqRateDiv = int(info[74].split("=")[1])
    if 0 == systemInfo.logAvgReqRateDiv:
        systemInfo.logAvgReqRateDiv = 1
    if len(info) < 76:
        return
    systemInfo.bTreeHeight = int(info[75].split("=")[1])
    if len(info) < 77:
        return
    systemInfo.logDiskWriteUsec = int(info[76].split("=")[1])
    if len(info) < 78:
        return
    systemInfo.logDiskWriteByteCount = int(info[77].split("=")[1])
    if len(info) < 79:
        return
    systemInfo.logDiskWriteCount = int(info[78].split("=")[1])
    if len(info) < 80:
        return
    systemInfo.logOpWrite5SecAvgUsec = int(info[79].split("=")[1])
    if len(info) < 81:
        return
    systemInfo.logOpWrite10SecAvgUsec = int(info[80].split("=")[1])
    if len(info) < 82:
        return
    systemInfo.logOpWrite15SecAvgUsec = int(info[81].split("=")[1])
    if len(info) < 83:
        return
    systemInfo.logExceedQueueDepthFailedCount = int(info[82].split("=")[1])
    if len(info) < 84:
        return
    systemInfo.logPendingAckByteCount = int(info[83].split("=")[1])
    if len(info) < 85:
        return
    systemInfo.logTotalRequestCount = int(info[84].split("=")[1])
    if len(info) < 86:
        return
    systemInfo.logExceedLogQueueDepthFailureCount300SecAvg = int(
        info[85].split("=")[1]
    )
    if len(info) < 87:
        return
    systemInfo.watchDogPolls = int(info[86].split("=")[1])
    if len(info) < 88:
        return
    systemInfo.watchDogTimeouts = int(info[87].split("=")[1])
    if len(info) < 89:
        return
    systemInfo.watchDogTimerOverruns = int(info[88].split("=")[1])
    if len(info) < 90:
        return
    systemInfo.watchDogTimerOverrunUsecs = int(info[89].split("=")[1])
    if len(info) < 91:
        return
    systemInfo.watchDogTimeSinseLastTimerOverrunUsecs = int(
        info[90].split("=")[1]
    )
    if len(info) < 92:
        return
    systemInfo.checkpointTimeSinceLastRunStart = int(info[91].split("=")[1])
    if len(info) < 93:
        return
    systemInfo.checkpointTimeSinceLastRunEnd = int(info[92].split("=")[1])
    if len(info) < 94:
        return
    systemInfo.checkpointConsecutiveFailures = int(info[93].split("=")[1])
    if len(info) < 95:
        return
    systemInfo.checkpointInterval = int(info[94].split("=")[1])
    if len(info) < 96:
        return
    systemInfo.objectStoreDeleteNoTier = int(info[95].split("=")[1])


def updateServerState(status, rackId, host, server):
    if rackId in status.serversByRack:
        # we really need a find_if()
        for r in status.serversByRack[rackId]:
            if r.host == host:
                if isinstance(server, UpServer(status)):
                    r.overloaded = server.overloaded
                r.wasStarted = 1
                if hasattr(server, "stillDown"):
                    r.isDown = server.stillDown
                    if r.isDown:
                        r.overloaded = 0


def splitServersByRack(status):
    for u in status.upServers:
        s = socket.gethostbyname(u.host)
        rackId = int(s.split(".")[2])
        updateServerState(status, rackId, s, u)

    for u in status.downServers:
        s = socket.gethostbyname(u.host)
        rackId = int(s.split(".")[2])
        updateServerState(status, rackId, s, u)


def ping(status, metaserver):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((metaserver.node, metaserver.port))
    req = REQUEST_PING
    sock.send(req)
    sockIn = sock.makefile("r")
    status.tiersColumnNames = {}
    status.tiersInfo = {}
    for line in sockIn:
        line = line.lstrip()
        if line == "":
            break
        if line.startswith("Down Servers:"):
            processDownNodes(status, line[line.find(":") + 1 :].strip())
            continue

        if line.startswith("Retiring Servers:"):
            processRetiringNodes(status, line[line.find(":") + 1 :].strip())
            continue

        if line.startswith("Evacuating Servers:"):
            processEvacuatingNodes(status, line[line.find(":") + 1 :].strip())
            continue

        if line.startswith("WORM:"):
            try:
                wormMode = line[line.find(":") + 1 :].strip()
                if int(wormMode) == 1:
                    status.systemInfo.wormMode = "Enabled"
                else:
                    status.systemInfo.wormMode = "Disabled"
            except Exception:
                pass

        if line.startswith("Build-version:"):
            status.systemInfo.buildVersion = line[line.find(":") + 1 :].strip()
            continue

        if line.startswith("Source-version:"):
            status.systemInfo.sourceVersion = line[
                line.find(":") + 1 :
            ].strip()
            continue

        if line.startswith("System Info:"):
            processSystemInfo(
                status.systemInfo, line[line.find(":") + 1 :].strip()
            )
            continue

        if line.startswith("Servers:"):
            processUpNodes(status, line[line.find(":") + 1 :].strip())
            continue

        if line.startswith("Storage tiers info names:"):
            status.tiersColumnNames = (
                line[line.find(":") + 1 :].strip().split("\t")
            )
            continue

        if line.startswith("Storage tiers info:"):
            status.tiersInfo = line[line.find(":") + 1 :].strip().split("\t")
            continue

        if line.startswith("Rebalance status:"):
            status.rebalanceStatus = parse_fields(
                line, field_sep="\t", key_sep="="
            )
            continue

        if line.startswith("Config:"):
            status.config = parse_fields(line, field_sep=";", key_sep="=")
            continue

        if line.startswith("VR Status:"):
            status.vrStatus = parse_fields(line, field_sep=";", key_sep="=")
            continue

        if line.startswith("Watchdog:"):
            status.watchdog = parse_fields(line, field_sep=";", key_sep="=")
            continue

    mergeDownUpNodes(status)
    mergeRetiringUpNodes(status)
    status.upServers.sort()

    sock.close()


def parse_fields(line, field_sep="\t", key_sep="="):
    if gHasCollections:
        res = OrderedDict()
    else:
        res = {}
    for keyval in line[line.find(":") + 1 :].strip().split(field_sep):
        try:
            key, value = keyval.split(key_sep, 1)
            res[key] = value
        except ValueError:
            continue
    return res


def splitThousands(s, tSep=",", dSep="."):
    """Splits a general float on thousands. GIGO on general input"""
    if s is None:
        return 0
    if not isinstance(s, str):
        s = str(s)

    cnt = 0
    numChars = dSep + "0123456789"
    ls = len(s)
    while cnt < ls and s[cnt] not in numChars:
        cnt += 1

    lhs = s[0:cnt]
    s = s[cnt:]
    if dSep == "":
        cnt = -1
    else:
        cnt = s.rfind(dSep)
    if cnt > 0:
        rhs = dSep + s[cnt + 1 :]
        s = s[:cnt]
    else:
        rhs = ""

    splt = ""
    while s != "":
        splt = s[-3:] + tSep + splt
        s = s[:-3]

    return lhs + splt[:-1] + rhs


def printRackViewHTML(rack, servers, buffer):
    """Print out all the servers in the specified rack"""
    print(
        """
    <div class="floatleft">
     <table class="network-status-table" cellspacing="0" cellpadding="0.1em"
        summary="Status of nodes in the rack """,
        rack,
        """ ">
     <tbody><tr><td><b>Rack : """,
        rack,
        """</b></td></tr>""",
        file=buffer,
    )
    count = 0
    for s in servers:
        s.printHTML(buffer, count)
        count = count + 1
    print("""</tbody></table></div>""", file=buffer)


def rackView(buffer, status):
    splitServersByRack(status)
    numNodes = sum([len(v) for v in status.serversByRack.values()])
    print(
        """
<body class="oneColLiqCtr">
<div id="container">
  <div id="mainContent">
    <table width=100%>
      <tr>
      <td>
      <p> Number of nodes: """,
        numNodes,
        """ </p>
      </td>
      <td align="right">
      <table class="network-status-table" font-size=14>
      <tbody>
        <tr class=notstarted><td></td><td>Not Started</td></tr>
        <tr class=dead><td></td><td>Dead Node</td></tr>
        <tr class=retiring><td></td><td>Retiring Node</td></tr>
        <tr class=><td ></td><td>Healthy</td></tr>
        <tr class=overloaded><td></td><td>
            Healthy, but not enough space for writes</td></tr>
      </tbody>
      </table>
      </td>
      </tr>
    </table>
    <hr>""",
        file=buffer,
    )

    for rack, servers in status.serversByRack.items():
        printRackViewHTML(rack, servers, buffer)

    print(
        """
    </div>
    </div>
    </body>
    </html>""",
        file=buffer,
    )


class ChunkHandler:
    def __init__(self):
        self.chunkDataManager = None
        self.countersDataManager = None
        self.chunkDirDataManager = None
        self.thread = None

        self.interval = 5

    def processInput(self, inputBody):
        if inputBody.find("GETCOUNTERS") != -1:
            theType = kMeta
        elif inputBody.find("GETCHART") != -1:
            theType = kChart
        elif inputBody.find("GETDIRCOUNTERS") != -1:
            theType = kChunkDirs
        else:
            theType = kChunks

        # refresh=60&delta=60&&dividedelta=dividedelta
        self.setDeltaValues(inputBody, theType)

        index = inputBody.find("MUMU")
        if index < 0:
            return -1
        newInputBody = inputBody[index:]
        headers = newInputBody.split("&")
        newHeaders = [
            header.strip()[5:].replace("%25", "%") for header in headers
        ]
        # newHeaders = [header.strip()[5:] for header in headers]
        # data MUMU=header1&MUMU=header22&MUMU=header3

        if theType != kMeta:
            if theType == kChunkDirs:
                self.setChunkDirsSelectedHeaders(newHeaders)
            else:
                self.setChunkSelectedHeaders(newHeaders)
        else:
            self.setCountersSelectedHeaders(newHeaders)
        return theType

    def setIntervalData(
        self,
        refreshInterval,
        predefinedHeaders,
        predefinedChunkDirHeaders,
        monthly,
        dayly,
        hourly,
        current,
    ):
        self.interval = refreshInterval
        headers = []
        if predefinedHeaders != "":
            headers = predefinedHeaders.split("&")
        dirHeaders = []
        if predefinedChunkDirHeaders != "":
            dirHeaders = predefinedChunkDirHeaders.split("&")
        self.chunkDataManager = ChunkDataManager(
            kServerName, headers, monthly, dayly, hourly, current
        )
        self.countersDataManager = ChunkDataManager(
            None, None, monthly, dayly, hourly, current
        )
        self.chunkDirDataManager = ChunkDataManager(
            kChunkDirName, dirHeaders, monthly, dayly, hourly, current
        )

    def startThread(self, serverHost, serverPort):
        if (
            self.chunkDataManager is None
            or self.countersDataManager is None
            or self.chunkDirDataManager is None
        ):
            print("ERROR - need to set the chunk intervals data first")
            return
        if self.thread is not None:
            return

        self.thread = ChunkThread(
            serverHost,
            serverPort,
            self.interval,
            self.chunkDataManager,
            self.countersDataManager,
            self.chunkDirDataManager,
        )
        self.thread.start()

    def chunksToHTML(self, buffer):
        if self.chunkDataManager is None:
            return 0
        self.chunkDataManager.lock.acquire()
        # print "deltaInterval", self.deltaInterval
        deltaList = self.chunkDataManager.getDelta()

        iRet = 0
        if deltaList is not None:
            HtmlPrintData(
                kServerName,
                deltaList,
                self.chunkDataManager,
                "no",
                "Chunk Servers Status",
                "servers",
            ).printToHTML(buffer)
            iRet = 1
        self.chunkDataManager.lock.release()
        return iRet

    def chunkDirsToHTML(self, buffer):
        if self.chunkDirDataManager is None:
            return 0
        self.chunkDirDataManager.lock.acquire()
        # print "deltaInterval", self.deltaInterval
        deltaList = self.chunkDirDataManager.getDelta()

        iRet = 0
        if deltaList is not None:
            HtmlPrintData(
                kChunkDirName,
                deltaList,
                self.chunkDirDataManager,
                "GETDIRCOUNTERS",
                "Chunk Directories Status",
                "directories",
            ).printToHTML(buffer)
            iRet = 1
        self.chunkDirDataManager.lock.release()
        return iRet

    def countersToHTML(self, buffer):
        if self.countersDataManager is None:
            return 0
        self.countersDataManager.lock.acquire()
        # print "deltaInterval", self.deltaInterval
        deltaList = self.countersDataManager.getDelta()

        iRet = 0
        if deltaList is not None:
            HtmlPrintMetaData(deltaList, self.countersDataManager).printToHTML(
                buffer
            )
            iRet = 1
        self.countersDataManager.lock.release()
        return iRet

    def chartsToHTML(self, buffer):
        if self.chunkDataManager is None:
            return 0
        chartData = ChartData()
        self.chunkDataManager.lock.acquire()
        self.chunkDataManager.getChartData(chartData)
        self.chunkDataManager.lock.release()
        ChartHTML(chartData).printToHTML(buffer)
        return 1

    def parseMinusTime(self, str1):
        if str1 == "":
            return 0
        theTime = 0
        index = str1.find("d")
        if index > 0:
            theTime = int(str1[:index]) * 86400
            str2 = str1[index + 1 :].strip()
        else:
            str2 = str1
        if str2 == "":
            return theTime

        index = str2.find("h")
        if index > 0:
            theTime = theTime + int(str2[:index]) * 3600
            str3 = str2[index + 1 :].strip()
        else:
            str3 = str2
        if str3 == "":
            return theTime

        index = str3.find("m")
        if index > 0:
            theTime = theTime + int(str3[:index]) * 60
            str4 = str3[index + 1 :].strip()
        else:
            str4 = str3
        if str4 == "":
            return theTime

        index = str4.find("s")
        if index > 0:
            theTime = theTime + int(str4[:index])
        else:
            theTime = theTime + int(str4.strip())
        return theTime

    def setDeltaValues(self, inputStr, theType):
        if theType == kMeta:
            dataManager = self.countersDataManager
        else:
            if theType == kChunkDirs:
                dataManager = self.chunkDirDataManager
            else:
                dataManager = self.chunkDataManager
        if dataManager is None:
            return

        value = self.getIntValue(inputStr, "refresh")
        if value > 0:
            dataManager.refreshInterval = value

        str1 = self.getValue(inputStr, "startTime")
        if str1 is not None:
            dataManager.minusLatestTime = self.parseMinusTime(str1)
        else:
            dataManager.minusLatestTime = 0

        value = self.getIntValue(inputStr, "delta")
        if value > 0:
            dataManager.deltaInterval = value
        doDivide = self.getIntValue(inputStr, "dividedelta")
        if doDivide > 0:
            dataManager.doDivide = 1
        else:
            dataManager.doDivide = 0

    def getIntValue(self, inputStr, keyword):
        str1 = self.getValue(inputStr, keyword)
        if str1 is None:
            return -1
        else:
            return int(str1)

    def getValue(self, inputStr, keyword):
        keyLength = len(keyword)
        index = inputStr.find(keyword)
        if index < 0:
            return None
        indexEnd = inputStr.find("&", index)
        if indexEnd < 0:
            newStr = inputStr[index + keyLength + 1 :]
        else:
            newStr = inputStr[index + keyLength + 1 : indexEnd]
        return newStr.strip()

    def setChunkSelectedHeaders(self, headers):
        self.chunkDataManager.lock.acquire()
        self.chunkDataManager.setSelectedHeaders(headers)
        self.chunkDataManager.lock.release()

    def setCountersSelectedHeaders(self, headers):
        self.countersDataManager.lock.acquire()
        self.countersDataManager.setSelectedHeaders(headers)
        self.countersDataManager.lock.release()

    def setChunkDirsSelectedHeaders(self, headers):
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
        chunkserverIndex = QueryCache.DIR_COUNTERS.chunkHeaders.index(
            "Chunk-server"
        )
        chunkDirIndex = QueryCache.DIR_COUNTERS.chunkHeaders.index("Chunk-dir")
        for entry in QueryCache.DIR_COUNTERS.chunkServers:
            dirResult = {}
            aResult = {}
            chunkserver = entry.nodes[chunkserverIndex].split(":")[0]
            chunkdir = entry.nodes[chunkDirIndex]
            if chunkserver in chunkserverHosts:
                for i in range(len(QueryCache.DIR_COUNTERS.chunkHeaders)):
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
                # print("Using cached numbers:",
                #   QueryCache.DIR_COUNTERS.printDebug())
                return QueryCache.GetMatchingCounters(chunkserverHosts)
        dir_counters = ChunkServerData()
        req = REQUEST_GET_DIRS_COUNTERS
        isConnected = False
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((metaserverHost, metaserverPort))
            isConnected = True
            sock.send(req)
            sockIn = sock.makefile("r")

            contentLength = -1
            gotHeader = 0
            sizeRead = 0

            for line in sockIn:
                if contentLength == -1:
                    infoData = line.strip().split(":")
                    if len(infoData) > 1:
                        if infoData[0].lower() == "content-length":
                            contentLength = int(infoData[1].strip())
                    continue
                sizeRead += len(line)
                if len(line.strip()) == 0:
                    if sizeRead >= contentLength:
                        break
                    else:
                        continue
                nodes = line.strip().split(",")
                if gotHeader == 0:
                    dir_counters.initHeader(nodes)
                    gotHeader = 1
                else:
                    dir_counters.addChunkServer(nodes)
                if contentLength >= 0 and sizeRead >= contentLength:
                    break
            sock.close()
        except socket.error as msg:
            print(msg, datetime.now().ctime())
            if isConnected:
                sock.close()
            return 0
        QueryCache.TIME = time.time()
        QueryCache.DIR_COUNTERS = dir_counters
        # print "Using fresh numbers:", QueryCache.DIR_COUNTERS.printDebug()
        if len(QueryCache.DIR_COUNTERS.chunkServers) > 0:
            return QueryCache.GetMatchingCounters(chunkserverHosts)


class QFSQueryHandler:
    @staticmethod
    def HandleQuery(queryPath, metaserver, buffer):
        if not gJsonSupported:
            return (501, "Server does not support query")

        if queryPath.startswith("/query/chunkservers"):
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
                output["up_servers"] = upServers
                output["down_servers"] = downServers
                print(json.dumps(output, cls=SetEncoder), file=buffer)
                return (200, "")
            except IOError:
                return (504, "Unable to ping metaserver")
        elif queryPath.startswith("/query/meta"):
            status = Status()
            try:
                ping(status, metaserver)
                print(
                    json.dumps(status.__dict__, cls=ObjectEncoder), file=buffer
                )
                return (200, "")
            except IOError:
                return (504, "Unable to ping metaserver")
        elif queryPath.startswith("/query/chunkserverdirs/"):
            try:
                hostsToMatch = queryPath[
                    len("/query/chunkserverdirs/") :
                ].split("&")
                print(
                    json.dumps(
                        QueryCache.GetChunkServerCounters(set(hostsToMatch))
                    ),
                    file=buffer,
                )
                return (200, "")
            except IOError:
                return (504, "Unable to ping metaserver")
        return (404, "Not Found")


class Pinger(SimpleHTTPRequestHandler):
    def __init__(self, request, client_address, server):
        SimpleHTTPRequestHandler.__init__(
            self, request, client_address, server
        )

    def setMeta(self, meta):
        self.metaserver = meta

    def sendErrorResponse(self, code, msg):
        self.send_response(code)

        body = f"error {msg}".encode("utf-8")
        # Send standard HTP headers
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.send_header("Connection", "close")
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-length", len(body))
        self.end_headers()
        self.wfile.write(body)
        return

    def do_POST(self):
        global gChunkHandler
        try:
            # interval = 60  # todo

            clen = int(self.headers.get("Content-Length").strip())
            if clen <= 0:
                self.send_response(400)
                return

            inputBody = self.rfile.read(clen).decode("utf-8")

            theType = gChunkHandler.processInput(inputBody)

            txtStream = StringIO()

            if theType == kMeta:
                if gChunkHandler.countersToHTML(txtStream) == 0:
                    print("POST NOT working! counters=0")
                    self.send_error(404, "No data, counters=0")
                    return
            elif theType == kChart:
                if gChunkHandler.chartsToHTML(txtStream) == 0:
                    print("NOT working! charts = 0'")
                    self.send_error(404, "Not data, charts = 0")
                    return
            elif theType == kChunks:
                if gChunkHandler.chunksToHTML(txtStream) == 0:
                    print("NOT working! chunks = 0")
                    self.send_error(404, "Not data. chunks = 0")
                    return
            elif theType == kChunkDirs:
                if gChunkHandler.chunkDirsToHTML(txtStream) == 0:
                    print("NOT working! chunk sirs = 0")
                    self.send_error(404, "Not data, chunk dirs = 0")
                    return
            else:
                self.send_response(400)
                return

            # reqHost = self.headers.get("Host")
            # refresh = "%d ; URL=http://%s%s" % (interval, reqHost, self.path)

            self.send_response(200)
            self.send_header("Content-type", "text/html")
            bytes_array = txtStream.getvalue().encode("utf-8")
            self.send_header("Content-length", len(bytes_array))
            self.end_headers()
            self.wfile.write(bytes_array)

        except IOError:
            print("Unable to post to metaserver, IO error")
            self.send_error(504, "Unable to post to metaserver, IO error")
        except Exception as e:
            print(f"Unable to post to metaserver, {e}")
            self.send_error(504, f"Unable to post to metaserver, {e}")

    def do_GET(self):
        global metaserverPort, metaserverHost, docRoot
        global gChunkHandler
        try:
            if self.path.startswith("/favicon.ico"):
                self.send_response(200)
                return
            if self.path.startswith("/files"):
                # skip over '/files/
                fpath = os.path.abspath(os.path.join(docRoot, self.path[7:]))
                try:
                    self.send_response(200)
                    self.send_header(
                        "Content-length", str(os.path.getsize(fpath))
                    )
                    self.end_headers()
                    self.copyfile(urlopen("file://" + fpath), self.wfile)
                except IOError:
                    print(f" failed to find file: {'file://' + fpath} ")
                    self.send_error(404, "Not found")
                return

            if self.path.startswith("/charts"):
                fpath = os.path.abspath(self.path[1:])
                try:
                    self.send_response(200)
                    self.send_header(
                        "Content-length", str(os.path.getsize(fpath))
                    )
                    self.end_headers()
                    self.copyfile(urlopen("file://" + fpath), self.wfile)
                except IOError:
                    print(f" failed to find chart file: {'file://' + fpath} ")
                    self.send_error(404, "Not found")
                return

            metaserver = ServerLocation(
                node=metaserverHost, port=metaserverPort
            )
            txtStream = StringIO()

            if self.path.startswith("/query/"):
                (ret, msg) = QFSQueryHandler.HandleQuery(
                    self.path, metaserver, txtStream
                )
                if ret != 200:
                    self.send_error(ret, msg)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                bytes_array = txtStream.getvalue().encode("utf-8")
                self.send_header("Content-length", len(bytes_array))
                self.end_headers()
                self.wfile.write(bytes_array)
                return

            if gChunkHandler.thread is None:
                gChunkHandler.startThread(metaserverHost, metaserverPort)

            status = None
            reqType = None
            getVrStatusHtml = self.path.startswith("/meta-vr-status-html")
            if getVrStatusHtml or self.path.startswith("/meta-conf-html"):
                status = Status()
                ping(status, metaserver)
                if getVrStatusHtml:
                    title = "Meta Server Viewstamped Replication Status"
                    keyColumnName = "Name"
                    keyvals = status.vrStatus
                    reqType = kVrStatus
                else:
                    title = "Meta Server Configuration"
                    keyColumnName = "Parameter Name"
                    keyvals = status.config
                    reqType = kConfig
                printStyle(txtStream, title)
                print(
                    """
                    <body class="oneColLiqCtr">
                    <div id="container">
                    <div id="mainContent">
                        <h1>""",
                    title,
                    displayName,
                    """</h1>
                        <P> <A href="/">Back</A>
                        </P>
                        <div class="floatleft">
                        <table class="sortable network-status-table"
                            id="configtable">
                        <caption> """,
                    title,
                    """ </caption>
                        <thead>
                        <tr>
                        <th>""",
                    keyColumnName,
                    """</th>
                        <th>Value</th>
                        </tr>
                        </thead>
                        <tbody>
                        """,
                    file=txtStream,
                )
                for k in keyvals:
                    print(
                        "<tr><td>",
                        htmlEscape(k),
                        "</td><td>",
                        htmlEscape(keyvals[k]),
                        "</td></tr>",
                        file=txtStream,
                    )
                print(
                    """
                        </tbody>
                        </table>
                        </div>
                    </div>
                    </div>
                    </body>
                    </html>
                """,
                    file=txtStream,
                )
                self.path = "/"
            elif self.path.startswith("/meta-conf"):
                status = Status()
                ping(status, metaserver)
                if gJsonSupported:
                    print(
                        json.dumps(status.config, sort_keys=True, indent=0),
                        file=txtStream,
                    )
                else:
                    print(status.config, file=txtStream)
                self.path = "/"
                reqType = cMeta
            elif self.path.startswith("/meta-vr-status"):
                status = Status()
                ping(status, metaserver)
                if gJsonSupported:
                    print(
                        json.dumps(status.vrStatus, sort_keys=True, indent=0),
                        file=txtStream,
                    )
                else:
                    print(status.vrStatus, file=txtStream)
                self.path = "/"
                reqType = kVrStatus
            elif self.path.startswith("/chunk-it"):
                self.path = "/"
                reqType = kChunks
            elif self.path.startswith("/meta-it"):
                self.path = "/"
                reqType = kMeta
            elif self.path.startswith("/browse-it"):
                self.path = self.path[len("/browse-it") :]
                if self.path == "":
                    self.path = "/"
                reqType = kBrowse
            elif self.path.startswith("/chunkdir-it"):
                self.path = "/"
                reqType = kChunkDirs

            if reqType == kChunks:
                if gChunkHandler.chunksToHTML(txtStream) == 0:
                    self.send_error(404, "Not found")
                    return
            elif reqType == kMeta:
                if gChunkHandler.countersToHTML(txtStream) == 0:
                    self.send_error(404, "Not found")
                    return
            elif reqType == kChunkDirs:
                if gChunkHandler.chunkDirsToHTML(txtStream) == 0:
                    self.send_error(404, "Not found")
                    return
            elif reqType == kBrowse and gQfsBrowser.browsable:
                if (
                    gQfsBrowser.printToHTML(
                        self.path, metaserverHost, metaserverPort, txtStream
                    )
                    == 0
                ):
                    self.send_error(404, "Not found")
                    return
            elif (
                reqType != cMeta
                and reqType != kConfig
                and reqType != kVrStatus
            ):
                status = Status()
                ping(status, metaserver)
                printStyle(txtStream, "QFS Status")

            refresh = None

            if self.path.startswith("/cluster-view"):
                rackView(txtStream, status)
            else:
                if reqType is None:
                    status.systemStatus(txtStream)
                reqHost = self.headers.get("Host")
                if reqHost is not None and autoRefresh > 0:
                    if reqType is not None:
                        refresh = None
                    else:
                        refresh = (
                            str(autoRefresh)
                            + " ; URL=http://"
                            + reqHost
                            + self.path
                        )

            self.send_response(200)
            if reqType != cMeta:
                self.send_header("Content-type", "text/html")
            else:
                self.send_header("Content-type", "application/json")
            bytes_array = txtStream.getvalue().encode("utf-8")
            self.send_header("Content-length", len(bytes_array))
            if refresh is not None:
                self.send_header("Refresh", refresh)
            self.end_headers()
            self.wfile.write(bytes_array)

        except IOError:
            print("Unable to ping metaserver, IO error")
            self.send_error(504, "Unable to ping metaserver, IO error")
        except Exception as e:
            print(f"Unable to ping metaserver, {e}")
            self.send_error(504, f"Unable to ping metaserver, {e}")


def parseChunkConfig(config):
    refreshInterval = 10
    predefinedHeaders = ""
    predefinedChunkDirHeaders = ""
    try:
        refreshInterval = config.getint("chunk", "refreshInterval")
    except Exception:
        pass
    try:
        predefinedHeaders = config.get("chunk", "predefinedHeaders")
    except Exception:
        pass
    try:
        predefinedChunkDirHeaders = config.get(
            "chunk", "predefinedChunkDirHeaders"
        )
    except Exception:
        pass

    theSize = 10
    timespan = 10
    try:
        theSize = config.getint("chunk", "currentSize")
    except Exception:
        pass
    try:
        timespan = config.getint("chunk", "currentSpan")
    except Exception:
        pass
    current = ChunkArrayData(timespan, theSize)

    theSize = 10
    timespan = 120
    try:
        theSize = config.getint("chunk", "hourlySize")
    except Exception:
        pass
    try:
        timespan = config.getint("chunk", "hourlySpan")
    except Exception:
        pass
    hourly = ChunkArrayData(timespan, theSize)

    theSize = 10
    timespan = 120
    try:
        theSize = config.getint("chunk", "daylySize")
    except Exception:
        pass
    try:
        timespan = config.getint("chunk", "daylySpan")
    except Exception:
        pass
    dayly = ChunkArrayData(timespan, theSize)

    theSize = 10
    timespan = 120
    try:
        theSize = config.getint("chunk", "monthlySize")
    except Exception:
        pass
    try:
        timespan = config.getint("chunk", "monthlySpan")
    except Exception:
        pass
    monthly = ChunkArrayData(timespan, theSize)

    gChunkHandler.setIntervalData(
        int(refreshInterval),
        predefinedHeaders,
        predefinedChunkDirHeaders,
        monthly,
        dayly,
        hourly,
        current,
    )


if __name__ == "__main__":
    global gChunkHandler
    global gQfsBrowser
    if len(sys.argv) != 2:
        print("Usage : ./qfsstatus.py <server.conf>")
        sys.exit()

    if not os.path.exists(sys.argv[1]):
        print("Unable to open ", sys.argv[1])
        sys.exit()

    gChunkHandler = ChunkHandler()
    gQfsBrowser = QFSBrowser()

    config = configparser.ConfigParser()
    config.read_file(open(sys.argv[1]))
    metaserverPort = config.getint("webserver", "webServer.metaserverPort")
    try:
        metaserverHost = config.get("webserver", "webServer.metaserverHost")
    except Exception:
        pass
    try:
        autoRefresh = config.getint("webserver", "webServer.autoRefresh")
    except Exception:
        pass
    try:
        displayPorts = config.getboolean("webserver", "webServer.displayPorts")
    except Exception:
        pass
    try:
        socketTimeout = config.getint("webserver", "webServer.socketTimeout")
    except Exception:
        socketTimeout = 90
        pass
    try:
        displayChunkServerStorageTiers = config.getboolean(
            "webserver", "webServer.displayChunkServerStorageTiers"
        )
    except Exception:
        displayChunkServerStorageTiers = True
        pass
    docRoot = config.get("webserver", "webServer.docRoot")
    try:
        HOST = config.get("webserver", "webServer.host")
    except Exception:
        HOST = "0.0.0.0"
        pass
    myWebserverPort = config.getint("webserver", "webServer.port")
    try:
        objectStoreMode = config.getboolean(
            "webserver", "webServer.objectStoreMode"
        )
    except Exception:
        objectStoreMode = False
        pass
    if metaserverHost != "127.0.0.1" and metaserverHost != "localhost":
        displayName = metaserverHost
    else:
        displayName = platform.node()
    displayName += ":" + str(metaserverPort)

    parseChunkConfig(config)

    socket.setdefaulttimeout(socketTimeout)
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    httpd = socketserver.ThreadingTCPServer((HOST, myWebserverPort), Pinger)
    pidf = ""
    try:
        pidf = config.get("webserver", "webServer.pidFile")
    except Exception:
        pass
    if 0 < len(pidf):
        f = open(pidf, "w")
        f.write("%d\n" % os.getpid())
        f.close()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("^C received, exiting")
        os._exit(1)
