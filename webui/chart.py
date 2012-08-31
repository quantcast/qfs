#
# $Id$
#
# Copyright 2011 Quantcast Corp.
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

import platform

kDeltaPrefix="D-"



class  ChartServerData:

    def __init__(self,serverName,serverArray):
        self.serverName = serverName
        self.serverArray = serverArray
    

class  ChartData:

    def __init__(self):
        self.headers = None
        self.serverArray = []
    

class ChartHTML:
    def __init__(self, chartData):
        self.chartData = chartData
    

    def printToHTML(self,buffer):
        print "TBD"
