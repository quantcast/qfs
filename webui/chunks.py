#
# $Id$
#
# Copyright 2010-2012,2016 Quantcast Corporation. All rights reserved.
#
# Author: Kate Labeeva
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
# 1. change time interval display
# 2 add sorted table to hidden html
# 3 sorting script currently works only for 1 sort table per page
# 7 delta divide to JS?

# ---------------
# Postponed:
# 1. how to stop thread if app has died
# 2  cookies
# ----------------
from __future__ import print_function

import socket
import threading
import time
import sys
from datetime import datetime

from chart import ChartServerData

if sys.version_info < (3, 0):
    from StringIO import StringIO
else:
    from io import StringIO

kDeltaPrefix = "D-"
REQUEST_GET_SERVER_COUNTERS = (
    "GET_CHUNK_SERVERS_COUNTERS\r\n"
    "Version: KFS/1.0\r\n"
    "Cseq: 1\r\n"
    "Client-Protocol-Version: 116\r\n"
    "\r\n"
).encode("utf-8")
REQUEST_GET_COUNTERS = (
    "GET_REQUEST_COUNTERS\r\n"
    "Version: KFS/1.0\r\n"
    "Cseq: 1\r\n"
    "Client-Protocol-Version: 116\r\n"
    "\r\n"
).encode("utf-8")
REQUEST_GET_CHUNK_COUNTERS = (
    "GET_CHUNK_SERVER_DIRS_COUNTERS\r\n"
    "Version: KFS/1.0\r\n"
    "Cseq: 1\r\n"
    "Client-Protocol-Version: 116\r\n"
    "\r\n"
).encode("utf-8")


class ChunkArrayData:
    def __init__(self, timespan, theSize):
        self.timespan = float(timespan)
        self.theSize = int(theSize)


# -------------------------------------------


class ChunkServer:
    def __init__(self, nodes):
        #        print("ChunkServer init")
        self.nodes = [elem.strip() for elem in nodes]


# ________________________________________________________
class ChunkServerData:
    def __init__(self):
        # print "ChunkServerData init !!!!"
        self.chunkServers = []
        self.chunkHeaders = []

    def addChunkServer(self, nodes):
        self.chunkServers.append(ChunkServer(nodes))

    def initHeader(self, nodes):
        self.chunkHeaders = [elem.strip() for elem in nodes]

    def printDebug(self):
        print(("Headers:", self.chunkHeaders))
        if len(self.chunkServers) > 0:
            print(("First row:", self.chunkServers[0].nodes))


# ________________________________________________________


class SavedChunkData:
    def __init__(self, timestamp, chunkServerData):
        self.timestamp = timestamp
        self.chunkServerData = chunkServerData

    def printDebug(self):
        print(("timestamp of the list= %d" % (self.timestamp)))
        self.chunkServerData.printDebug()


class SavedChunkDataList:
    def __init__(self, timespan, maxLength, nextList=None):
        self.nextList = nextList
        self.maxLength = maxLength
        self.timespan = timespan

        self.theList = []  # SavedChunkData
        self.firstTime = 0
        self.lastTime = 0
        self.name = "data_list: maxlength %d, timespan = %d" % (
            self.maxLength,
            self.timespan,
        )

    def free_space_for_new_data(self):
        # move to next list if posssible
        oldData = self.extract(-1)
        if self.nextList is not None and oldData is not None:
            self.nextList.addData(oldData, 0)

    def addData(self, data, always=1):
        if always == 0:
            if self.canAddData(data) == 0:
                return

        if len(self.theList) >= self.maxLength:
            self.free_space_for_new_data()
        self.justAdd(data)

    def justAdd(self, data):
        if self.firstTime == 0:
            self.firstTime = data.timestamp
        self.lastTime = data.timestamp
        self.theList.insert(0, data)

    def extract(self, index):
        # if index is -1 extract the last
        theLen = len(self.theList)
        if theLen <= 0 or index >= theLen:
            return None
        if index < 0:
            index = theLen - 1
        data = self.theList.pop(index)
        if theLen > 1:
            self.firstTime = self.theList[theLen - 2].timestamp
        elif theLen == 1:
            self.lastTime = self.firstTime = 0
        return data

    def canAddData(self, data):
        # todo
        if len(self.theList) == 0:
            return 1

        timediff = data.timestamp - self.theList[0].timestamp
        if timediff >= self.timespan:
            return 1
        else:
            return 0

    def printDebug(self):
        if self.nextList is not None:
            s = self.nextList.name
        else:
            s = "none"
        print("the list", self.name)
        print("next List", s)
        print("maxLength", self.maxLength)
        print("timespan", self.timespan)

        print("firstTime", self.firstTime)
        print("lastTime", self.lastTime)

        for elem in self.theList:
            elem.printDebug()


class ChunkDataManager:
    def __init__(
        self,
        mainColumnName,
        predefinedHeaders,
        monthly,
        dayly,
        hourly,
        current,
    ):
        # 1 entry per day
        # self.monthlyData = SavedChunkDataList(60*60*24, 30)
        # 1 entry per hour
        # self.daylyData = SavedChunkDataList(60*60, 24, self.monthlyData)
        # 1 entry per 2 minutes
        # self.hourlyData = SavedChunkDataList(60*2, 30, self.daylyData)
        # 1 entry per 10 sec
        # self.latestData = SavedChunkDataList(10, 30, self.hourlyData, 0)

        self.lock = threading.Lock()

        self.deltaInterval = 10 * 60  # todo Cookie?
        self.refreshInterval = 120  # todo Cookie?
        self.minusLatestTime = 0  # todo Cookie?
        self.doDivide = 1

        self.selectedHeaders = predefinedHeaders
        self.mainColumnName = mainColumnName

        # SavedChunkDataList.__init__(self, timespan, maxLength,
        # nextList=None):
        self.dataArray = []
        theData = SavedChunkDataList(
            monthly.timespan, monthly.theSize
        )  # monthly
        # theData = SavedChunkDataList(8.0, 6)    #monthly
        self.dataArray.append(theData)

        theData = SavedChunkDataList(
            dayly.timespan, dayly.theSize, self.dataArray[0]
        )  # dayly
        self.dataArray.insert(0, theData)

        theData = SavedChunkDataList(
            hourly.timespan, hourly.theSize, self.dataArray[0]
        )  # hourly
        self.dataArray.insert(0, theData)

        theData = SavedChunkDataList(
            current.timespan, current.theSize, self.dataArray[0]
        )  # current
        self.dataArray.insert(0, theData)

    def getLastTime(self):
        return self.dataArray[0].lastTime

    def getFirstTime(self):
        for array in reversed(self.dataArray):
            if array.firstTime > 0:
                return array.firstTime

    def add(self, data):
        self.dataArray[0].addData(data)

    def printDebug(self):
        theLen = len(self.dataArray)
        print("ChunkDataManager debug print")
        arrayNames = ["latestData", "hourlyData", "daylyData", "monthlyData"]
        for i in range(theLen):
            print(
                arrayNames[i] + "---------------------------------------------"
            )
            self.dataArray[i].printDebug()

    def setSelectedHeaders(self, headers):
        self.selectedHeaders = headers

    def getDelta(self):
        #        print "-------- get Delta"
        if len(self.dataArray) <= 0:
            return None

        if len(self.dataArray[0].theList) == 0:
            return None

        data1 = None
        data2 = None

        dataIter = DataIter(self.dataArray)

        # data in arrays is sorted by descending time, so fromTime is bigger
        # than toTime
        if self.minusLatestTime > 0:
            fromTime = self.dataArray[0].lastTime - self.minusLatestTime
            if self.findArrayIndexByTime(dataIter, fromTime):
                if self.findTimedData(dataIter, fromTime):
                    data1 = dataIter.getCur()

        if data1 is None:
            dataIter.setFirst()
            data1 = dataIter.getCur()

        fromTime = data1.timestamp
        toTime = fromTime - self.deltaInterval
        # print "---, fromTime, toTime, self.minusLatestTime, dataIter.iArray,
        # dataIter.iElem

        if dataIter.getNext() is not None:
            if self.findArrayIndexByTime(dataIter, toTime):
                if self.findTimedData(dataIter, toTime):
                    data2 = dataIter.getCur()
            if data2 is None:
                dataIter.setLast()
                data2 = dataIter.getCur()
                if data2 is not None and data2.timestamp == data1.timestamp:
                    data2 = None
        else:
            dataPrev = dataIter.getPrev()
            if dataPrev is not None:
                data2 = data1
                data1 = dataPrev

        # print data1.timestamp

        if data2 is not None:
            # print data2.timestamp
            return [data1, data2]
        else:
            return [data1]

    def findArrayIndexByTime(self, dataIter, theTime):
        if theTime < 0:
            return 0

        array = dataIter.getArray()
        while array is not None:
            if theTime >= array.firstTime:
                return 1
            array = dataIter.getNextArray()
        return 0

    def findTimedData(self, dataIter, timestamp):
        elem = dataIter.getCur()
        while elem is not None:
            if elem.timestamp - timestamp <= 0:
                return 1
            elem = dataIter.getNext()
        return 0

    # 4 "SavedChunksDatList" arrays
    #        SavedChunksDatList: theList  #SavedChunkData
    #        firstTime
    #        lastTime
    # SavedChunkData:
    #        timestamp
    #        chunkServerData #chunkServerData
    def getChartData(self, chartData):
        chartData.headers = self.selectedHeaders
        elem = self.dataArray[0].theList[0]
        if self.mainColumnName is not None:
            if self.mainColumnName in elem.chunkServerData.chunkHeaders:
                serverNameIndex = elem.chunkServerData.chunkHeaders.index(
                    self.mainColumnName
                )
            else:
                return
        else:
            serverNameIndex = 0

        for chunk in elem.chunkServerData.chunkServers:
            if len(chunk.nodes) > serverNameIndex:
                serverName = chunk.nodes[serverNameIndex]
                self.getServerChartData(serverName, chartData.serverArray)

    def getServerChartData(self, serverName, chartArray):
        serverArray = []
        for array in self.dataArray:
            theList = array.theList
            for elem in theList:
                timeDataArray = []
                if self.mainColumnName is None:
                    serverNameIndex = 0
                else:
                    serverNameIndex = -1
                    if (
                        self.mainColumnName
                        in elem.chunkServerData.chunkHeaders
                    ):
                        serverNameIndex = (
                            elem.chunkServerData.chunkHeaders.index(
                                self.mainColumnName
                            )
                        )
                    if serverNameIndex == -1:
                        return
                for i in range(len(elem.chunkServerData.chunkServers)):
                    chunk = elem.chunkServerData.chunkServers[i]
                    if len(chunk.nodes) <= serverNameIndex:
                        return
                    serverIndex = -1
                    if chunk.nodes[serverNameIndex] == serverName:
                        serverIndex = i
                        break

                if serverIndex == -1:
                    return

                chunk = elem.chunkServerData.chunkServers[serverIndex]
                timeDataArray.append(elem.timestamp)
                for header in self.selectedHeaders:
                    if header.startswith(kDeltaPrefix):
                        theHeader = header[len(kDeltaPrefix) :]
                    else:
                        theHeader = header
                    if theHeader in elem.chunkServerData.chunkHeaders:
                        index = elem.chunkServerData.chunkHeaders.index(
                            theHeader
                        )
                    else:
                        index = -1
                    value = None
                    if index >= 0:
                        if chunk.nodes[index].isdigit():
                            value = float(chunk.nodes[index])
                    timeDataArray.append(value)

                serverArray.insert(0, timeDataArray)

        # estimating delta
        for i in range(len(self.selectedHeaders)):
            if self.selectedHeaders[i].startswith(kDeltaPrefix):
                valPrev = None
                for j in range(1, len(serverArray)):
                    array0 = serverArray[j - 1]
                    array1 = serverArray[j]
                    timediff = (
                        array1[0] - array0[0]
                    )  # first element in array is timestamp
                    val1 = None
                    if len(array0) > i + 1 and len(array1) > i + 1:
                        val1 = array1[i + 1]
                        if j == 1:
                            valPrev = array0[i + 1]
                    if valPrev is None or val1 is None or timediff == 0:
                        value = None
                    else:
                        value = (val1 - valPrev) / timediff
                    valPrev = val1
                    if len(serverArray[j]) > i + 1:
                        serverArray[j][i + 1] = value
                if len(serverArray[0]) > i + 1:
                    serverArray[0][i + 1] = 0

        chartData = ChartServerData(serverName, serverArray)
        chartArray.append(chartData)


#        print self.selectedHeaders
#        print "Data for server:" + serverName
#        print serverArray


class DataIter:
    def __init__(self, dataArray):
        self.dataArray = dataArray
        if dataArray is None or len(self.dataArray) == 0:
            self.iArray = self.iElem = -1
        elif (
            self.dataArray[0].theList is None
            or len(self.dataArray[0].theList) == 0
        ):
            self.iArray = self.iElem = -1
        else:
            self.iArray = self.iElem = 0

    def getArray(self):
        if self.iElem == -1:
            return None
        return self.dataArray[self.iArray]

    def getNextArray(self):
        if self.iElem == -1:
            return None
        if self.iArray + 1 >= len(self.dataArray):
            return None
        elif len(self.dataArray[self.iArray + 1].theList) > 0:
            self.iArray = self.iArray + 1
            self.iElem = 0
            return self.dataArray[self.iArray]
        else:
            return None

    def getNext(self):
        if self.iElem == -1:
            return None
        if self.iElem + 1 < len(self.dataArray[self.iArray].theList):
            self.iElem = self.iElem + 1
            # print "getNext", self.iArray, self.iElem
            return self.dataArray[self.iArray].theList[self.iElem]
        elif (
            self.iArray + 1 < len(self.dataArray)
            and len(self.dataArray[self.iArray + 1].theList) > 0
        ):
            self.iArray = self.iArray + 1
            self.iElem = 0
            # print "getNext", self.iArray, self.iElem
            return self.dataArray[self.iArray].theList[self.iElem]
        else:
            return None

    def getCur(self):
        if self.iElem == -1:
            return None
        # print "getCur", self.iArray, self.iElem
        return self.dataArray[self.iArray].theList[self.iElem]

    def getPrev(self):
        if self.iElem == -1:
            return None
        if self.iElem > 0:
            self.iElem = self.iElem - 1
            # print "getPrev", self.iArray, self.iElem
            return self.dataArray[self.iArray].theList[self.iElem]
        elif self.iArray > 0:
            self.iArray = self.iArray - 1
            self.iElem = len(self.dataArray[self.iArray].theList) - 1
            # print "getPrev", self.iArray, self.iElem
            return self.dataArray[self.iArray].theList[self.iElem]
        else:
            return None

    def setFirst(self):
        if self.iElem == -1:
            return 0
        self.iArray = self.iElem = 0
        return 1

    def setLast(self):
        if self.iElem == -1:
            return 0
        for ind in range(len(self.dataArray)):
            if len(self.dataArray[ind].theList) == 0:
                break
            self.iArray = ind
            self.iElem = len(self.dataArray[self.iArray].theList) - 1
        return 1


class ChunkThread(threading.Thread):
    def __init__(
        self,
        serverName,
        port,
        interval,
        chunkDataManager,
        countersDataManager,
        chunkDirDataManager,
    ):
        threading.Thread.__init__(self)
        self.interval = float(interval)
        self.chunkDataManager = chunkDataManager
        self.countersDataManager = countersDataManager
        self.chunkDirDataManager = chunkDirDataManager
        self.doStop = 0
        self.serverName = serverName
        self.port = port

    def run(self):
        print("\nThread started\n")
        while self.doStop == 0:
            req = REQUEST_GET_SERVER_COUNTERS
            chunkServerData = ChunkServerData()
            if self.getChunkServerData(chunkServerData, req) == 0:
                del chunkServerData
                time.sleep(300)
                continue
            theTime = time.time()
            chunkData = SavedChunkData(theTime, chunkServerData)
            self.chunkDataManager.lock.acquire()
            self.chunkDataManager.add(chunkData)
            self.chunkDataManager.lock.release()

            req = REQUEST_GET_COUNTERS
            countersServerData = ChunkServerData()
            if self.getChunkServerData(countersServerData, req) == 0:
                del countersServerData
                time.sleep(300)
                continue
            theTime = time.time()
            countersData = SavedChunkData(theTime, countersServerData)

            self.countersDataManager.lock.acquire()
            self.countersDataManager.add(countersData)
            self.countersDataManager.lock.release()

            req = REQUEST_GET_CHUNK_COUNTERS
            chunkServerDirData = ChunkServerData()
            if self.getChunkServerData(chunkServerDirData, req) == 0:
                del chunkServerDirData
                time.sleep(300)
                continue
            theTime = time.time()
            chunkDirData = SavedChunkData(theTime, chunkServerDirData)
            self.chunkDirDataManager.lock.acquire()
            self.chunkDirDataManager.add(chunkDirData)
            self.chunkDirDataManager.lock.release()

            time.sleep(self.interval)

    # chunk request to metaserver
    def getChunkServerData(self, chunkServerData, req):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as msg:
            print(msg, datetime.now().ctime())
            return 0

        try:
            sock.connect((self.serverName, self.port))
            sock.send(req)
            sockIn = sock.makefile("r")
        except socket.error as msg:
            print(msg, datetime.now().ctime())
            sock.close()
            return 0

        curLength = 0
        contentLength = -1
        count = 0

        try:
            for line in sockIn:
                nodes = line.split(",")
                ilen = len(nodes)
                if ilen <= 1:
                    infoData = line.strip().split(":")
                    if len(infoData) > 1:
                        if infoData[0].find("Content-length") != -1:
                            contentLength = int(infoData[1].strip())
                else:
                    curLength += len(line)
                    if count == 0:
                        chunkServerData.initHeader(nodes)
                        count = 1
                    else:
                        chunkServerData.addChunkServer(nodes)

                if contentLength >= 0 and curLength >= contentLength:
                    break
        except socket.error as msg:
            print(msg, datetime.now().ctime())
            sock.close()
            return 0

        sock.close()
        return 1


class HtmlPrintData:
    def __init__(
        self,
        mainColumnName,
        deltaList,
        dataManager,
        formId,
        pageTitle,
        itemName,
    ):
        # list of 2 SavedChunkData: timestamp + ChunkServerData
        self.data1 = deltaList[0]
        self.data2 = None
        if len(deltaList) > 1:
            self.data2 = deltaList[1]
        self.deltaInterval = dataManager.deltaInterval
        self.refreshInterval = dataManager.refreshInterval
        self.divideByTime = dataManager.doDivide
        self.firstTime = dataManager.getFirstTime()
        self.lastTime = dataManager.getLastTime()
        self.minusTime = dataManager.minusLatestTime
        self.serverNameIndex = -1

        self.selectedHeaders = dataManager.selectedHeaders
        self.indexList = (
            []
        )  # index of corresponding headers in allHeaders for each
        # selectedHeader in selectedHeaders
        self.mainColumnName = mainColumnName
        self.formId = formId
        self.pageTitle = pageTitle
        self.itemName = itemName

    # beginning of html
    def startHTML(self, buffer):
        # beginning of html
        if self.data2 is not None:
            timediffStr = str(int(self.data1.timestamp - self.data2.timestamp))
            timestr = (
                time.strftime(
                    "%m-%d-%Y %H:%M:%S", time.localtime(self.data2.timestamp)
                )
                + " - "
                + time.strftime(
                    "%m-%d-%Y %H:%M:%S", time.localtime(self.data1.timestamp)
                )
                + " ("
                + timediffStr
                + ")"
            )
        else:
            timestr = time.strftime(
                "%m-%d-%Y %H:%M:%S", time.localtime(self.data1.timestamp)
            )
        numServers = len(self.data1.chunkServerData.chunkServers)

        print(
            """
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"\
"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
    <html xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <link rel="stylesheet" type="text/css" href="files/qfsstyle.css">
        <script type="text/javascript" src="files/sorttable/sorttable.js">
        </script>
        <title>""",
            self.pageTitle,
            """</title>
    </head>
<body class="oneColLiqCtr">
<div id="container">
  <div id="mainContent">
    <h1> """,
            self.pageTitle,
            """ </h1>
    <P>  <A href="/"> Back....</A></P>
    <div class="info-table1">
    <table cellspacing="0" cellpadding="0.1em"> <tbody>
    <tr> <td> Current Time </td><td>:</td><td>""",
            datetime.now().ctime(),
            """</td></tr>
    <tr> <td> Number of """,
            self.itemName,
            """ </td><td>:</td><td>%d</td></tr>
    </tbody></table></div><br />

    <div class="floatleft">
     <table class="sortable status-table" id="table1" cellspacing="0"
        cellpadding="0.1em" summary="Status of chunk servers">
     <caption> %s %s </caption>"""
            % (numServers, self.pageTitle, timestr),
            file=buffer,
        )

    def endHTML(self, buffer):
        #       end of HTML
        print(
            """
  </div> </div>
 </body>
</html>""",
            file=buffer,
        )

    def printToHTML(self, buffer):
        self.initHeaders()

        self.startHTML(buffer)

        self.printHeaderToHTML(buffer)
        self.printServerListToHTML(buffer)
        self.printIntervalsToHTML(buffer)
        self.printSelectionToHTML(buffer)

        self.endHTML(buffer)

    def printServerListToHTML(self, buffer):
        txtStream = StringIO()
        theLen = len(self.selectedHeaders)
        totalValue = [[0, 0] for i in range(theLen)]
        # beginning of table body
        print(
            """
     <tbody>""",
            file=buffer,
        )
        # table body
        for i in range(len(self.data1.chunkServerData.chunkServers)):
            if i % 2 == 1:
                trclass = "class=odd"
            else:
                trclass = ""
            self.printServerToHTML(txtStream, i, trclass, totalValue)

        # total value
        print(
            """
        <tr class="totalCls">""",
            end=" ",
            file=buffer,
        )
        for val in totalValue:
            if val[1] == 0:
                s = "n/a"
            else:
                s = "%.2e" % (val[0])
            print(
                """
            <td align="center">%s</td>"""
                % (s),
                end=" ",
                file=buffer,
            )
        print("""</tr>""", file=buffer)

        # all other values
        print(txtStream.getvalue(), file=buffer)
        txtStream.close()

        # end of table body & table
        print(
            """
    </tbody>
   </table></div>""",
            file=buffer,
        )

    def printHeaderToHTML(self, buffer):
        # table headers
        print("""<thead> <tr>""", end=" ", file=buffer)
        for i in range(len(self.selectedHeaders)):
            print(
                """<th>""",
                self.selectedHeaders[i],
                """</th>""",
                end=" ",
                file=buffer,
            )
        print("""</tr></thead>""", file=buffer)

    def initHeaders(self):
        allHeaders = self.data1.chunkServerData.chunkHeaders
        newHeaders = []
        if len(self.indexList) != 0:
            del self.indexList
        if self.mainColumnName is None:
            self.serverNameIndex = 0
        for i in range(len(allHeaders)):
            header = allHeaders[i]
            d_header = kDeltaPrefix + header

            if (
                self.mainColumnName is not None
                and header == self.mainColumnName
            ):
                self.serverNameIndex = i

            if header in self.selectedHeaders:
                newHeaders.append(header)
                self.indexList.append(i)
            if d_header in self.selectedHeaders:
                newHeaders.append(d_header)
                self.indexList.append(i)
        self.selectedHeaders = newHeaders

        # print(
        #     "serverNameIndex:%d, %s"
        #     % (self.serverNameIndex, allHeaders[self.serverNameIndex])
        # )
        # print(self.indexList)

    def findServerForData2(self, serverIndex):
        serverName = self.data1.chunkServerData.chunkServers[
            serverIndex
        ].nodes[self.serverNameIndex]
        #       print "ServerName:" + serverName
        if serverIndex < len(self.data2.chunkServerData.chunkServers):
            chunkData2 = self.data2.chunkServerData.chunkServers[serverIndex]
            if serverName == chunkData2.nodes[self.serverNameIndex]:
                #                print "Same index for server name"
                return chunkData2
        for chunkData2 in self.data2.chunkServerData.chunkServers:
            if serverName == chunkData2.nodes[self.serverNameIndex]:
                return chunkData2
        #        print "Not found"
        return None

    def printServerToHTML(self, buffer, serverIndex, trclass, totalValue):
        #       print "server index %d" % (serverIndex)
        chunkData1 = self.data1.chunkServerData.chunkServers[serverIndex]
        chunkData2 = None
        if self.data2 is not None:
            chunkData2 = self.findServerForData2(serverIndex)

        if chunkData2 is not None:
            timediff = self.data1.timestamp - self.data2.timestamp
        else:
            #            print "No Chunk data2!!!"
            timediff = 0
            chunkData2 = None

        print(
            """
        <tr %s>"""
            % (trclass),
            end=" ",
            file=buffer,
        )

        for i in range(len(self.selectedHeaders)):
            index = self.indexList[i]
            value = None
            if index >= 0:
                if self.selectedHeaders[i].startswith(kDeltaPrefix):
                    if timediff == 0:
                        s = "undefined"
                    else:
                        if (
                            chunkData1.nodes[index] is None
                            or chunkData1.nodes[index].isalpha() == 1
                            or chunkData2.nodes[index] is None
                            or chunkData2.nodes[index].isalpha() == 1
                        ):
                            # print(
                            #     "not digit !%s! !%s!"
                            #     % (
                            #         chunkData1.nodes[index],
                            #         chunkData2.nodes[index],
                            #     )
                            # )
                            s = "n/a"
                        else:
                            try:
                                if self.divideByTime:
                                    value = (
                                        float(chunkData1.nodes[index])
                                        - float(chunkData2.nodes[index])
                                    ) / timediff
                                else:
                                    value = float(
                                        chunkData1.nodes[index]
                                    ) - float(chunkData2.nodes[index])
                                s = "%.2e" % (value)
                            except Exception:
                                s = "n/a"
                elif (
                    chunkData1.nodes[index] is not None
                    or chunkData1.nodes[index].isalpha() != 1
                ):
                    try:
                        value = float(chunkData1.nodes[index])
                        s = "%.2e" % (value)
                    except Exception:
                        s = chunkData1.nodes[index]
                else:
                    s = chunkData1.nodes[index]
            else:
                s = "undefined"

            print(
                """
        <td>%s</td>"""
                % (s),
                end=" ",
                file=buffer,
            )
            #        <td align="center">%s</td>''' % (s),

            if totalValue is not None and value is not None:
                totalValue[i][0] = totalValue[i][0] + value
                totalValue[i][1] = 1

        print("""</tr>""", file=buffer)

    def printIntervalsToHTML(self, buffer):
        if self.divideByTime:
            checked = "checked"
        else:
            checked = ""
        timediff = self.lastTime - self.firstTime
        timestr_latest = time.strftime(
            "%m-%d-%Y %H:%M:%S", time.localtime(self.lastTime)
        )
        timestr_first = time.strftime(
            "%m-%d-%Y %H:%M:%S", time.localtime(self.firstTime)
        )
        minusTimeStr = ""
        minusTime = self.minusTime
        if minusTime > 0:
            days = int(minusTime / 86400)
            if days > 0:
                minusTimeStr = "%dd" % (days)
                minusTime = minusTime - days * 86400
            hours = int(minusTime / 3600)
            if hours > 0:
                minusTimeStr = "%s%dh" % (minusTimeStr, hours)
                minusTime = minusTime - hours * 3600
            minutes = int(minusTime / 60)
            if minutes > 0:
                minusTimeStr = "%s%dm" % (minusTimeStr, minutes)
                minusTime = minusTime - minutes * 60
            if minusTime > 0:
                minusTimeStr = "%s%ds" % (minusTimeStr, minusTime)

        print(
            """
    <div class="floatleft">
    <FORM action="/" method="post" name="selectHeaderForm"
        id="selectHeaderForm">
        <br>
        refresh in
        <INPUT type="text" id="refresh" name="refresh"
            class="theInputClr" value="%d" size=4 maxlength=5>
        sec
        <br>
        available interval: %s - %s ( %d sec )
        <br>
        latest time minus
        <INPUT type="text" id="startTime" name="startTime"
            class="theInputClr" value="%s" size=20 maxlength=20>
        (d-days, h-hours,m-minutes, s-seconds; default - seconds)
        <br>
        show delta for
        <INPUT type="text" class="theInputClr" id="delta"
            name="delta" value="%d" size=6 maxlength=10>
        sec&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;divide by time
        <INPUT type=checkbox  class="theInputClr"
            name="dividedelta" value="1" %s>"""
            % (
                self.refreshInterval,
                timestr_first,
                timestr_latest,
                timediff,
                minusTimeStr,
                self.deltaInterval,
                checked,
            ),
            file=buffer,
        )

    def printFormTypeToHTML(self, buffer):
        # &nbsp;&nbsp; &nbsp; &nbsp;<INPUT type="button"
        # value="Charts" id = "chartButton">
        print(
            '''
        <INPUT type=hidden name="chartInput" id="chartInput" value="''',
            self.formId,
            """">
        <br><br>&nbsp;&nbsp; &nbsp; &nbsp;
        <INPUT type="submit" value="Selection done">
        <br><br>
        <table class="status-table" cellspacing="6" cellpadding="0.1em"
            summary="Show/hide columns">
        <tbody>""",
            end=" ",
            file=buffer,
        )

    def printSelectionToHTML(self, buffer):
        self.printFormTypeToHTML(buffer)

        thedHeaders = self.data1.chunkServerData.chunkHeaders

        n_col = 5
        lenn = len(thedHeaders)
        n_height = (lenn + n_col - 1) // n_col
        if n_height % 2:
            n_height = n_height + 1
        for i in range(n_height):
            print(
                """
            <tr>""",
                end=" ",
                file=buffer,
            )
            for j in range(i, lenn, n_height):
                header = thedHeaders[j]
                header2 = kDeltaPrefix + header
                if header in self.selectedHeaders:
                    checked1 = "checked"
                else:
                    checked1 = ""
                if header2 in self.selectedHeaders:
                    checked2 = "checked"
                else:
                    checked2 = ""
                print(
                    """
              <td> <INPUT TYPE=checkbox NAME=MUMU VALUE=%s %s>&nbsp&nbsp   #
              <INPUT TYPE=checkbox NAME=MUMU VALUE=%s %s> %s</td>"""
                    % (header, checked1, header2, checked2, header),
                    end=" ",
                    file=buffer,
                )
            print("</tr>", file=buffer)
        print(
            """
      </tbody></table>
      </FORM></div>""",
            file=buffer,
        )


class HtmlPrintMetaData(HtmlPrintData):
    def __init__(self, deltaList, dataManager):
        HtmlPrintData.__init__(
            self, None, deltaList, dataManager, "no", "none", "rows"
        )

    def startHTML(self, buffer):
        # beginning of html
        if self.data2 is not None:
            timestr = (
                time.strftime(
                    "%m-%d-%Y %H:%M:%S", time.localtime(self.data2.timestamp)
                )
                + " - "
                + time.strftime(
                    "%m-%d-%Y %H:%M:%S", time.localtime(self.data1.timestamp)
                )
            )
        else:
            timestr = time.strftime(
                "%m-%d-%Y %H:%M:%S", time.localtime(self.data1.timestamp)
            )

        print(
            """
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"\
 "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
    <html xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <link rel="stylesheet" type="text/css" href="files/qfsstyle.css">
        <script type="text/javascript" src="files/sorttable/sorttable.js">
        </script>
        <title>Meta Server Status</title>
    </head>
<body class="oneColLiqCtr">
<div id="container">
  <div id="mainContent">
    <h1> Meta Server Status </h1>
    <P>  <A href="/"> Back....</A></P>
    <div class="info-table1">
    <table cellspacing="0" cellpadding="0.1em"> <tbody>
    <tr> <td> Current Time </td><td>:</td><td>%s</td></tr>
    </tbody></table></div><br />

    <div class="floatleft">
     <table class="sortable status-table" id="table1" cellspacing="0"
        cellpadding="0.1em" summary="Status of chunk servers">
     <caption> Meta Server Status &nbsp;&nbsp;&nbsp;&nbsp; %s </caption>"""
            % (datetime.now().ctime(), timestr),
            file=buffer,
        )

    def initHeaders(self):
        if self.selectedHeaders is None:
            self.selectedHeaders = []
            for header in self.data1.chunkServerData.chunkHeaders:
                d_header = kDeltaPrefix + header
                self.selectedHeaders.append(header)
                self.selectedHeaders.append(d_header)
        HtmlPrintData.initHeaders(self)

    def printFormTypeToHTML(self, buffer):
        print(
            """
        <INPUT type=hidden name="countersInput" id="countersInput"
            value="GETCOUNTERS">
        <br><br>&nbsp;&nbsp; &nbsp; &nbsp;<INPUT type="submit"
            value="Selection done">
        <br><br>
        <table class="status-table" cellspacing="6" cellpadding="0.1em"
            summary="Show/hide columns">
        <tbody>""",
            end=" ",
            file=buffer,
        )

    def printServerListToHTML(self, buffer):
        # theLen = len(self.selectedHeaders)
        # beginning of table body
        print(
            """
     <tbody>""",
            file=buffer,
        )
        # table body
        for i in range(len(self.data1.chunkServerData.chunkServers)):
            if i == 0:
                trclass = "class=totalCls"
            elif i % 2 == 1:
                trclass = "class=odd"
            else:
                trclass = ""
            self.printServerToHTML(buffer, i, trclass, None)
        # end of table body & table
        print(
            """
    </tbody>
   </table></div>""",
            file=buffer,
        )
