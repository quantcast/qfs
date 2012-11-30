#
# $Id$
#
# Copyright 2012 Quantcast Corp.
#
# Author: Thilee Subramaniam (Quantcast Corp.)
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
# File system lister / browser.
#

import os
import sys
import stat
import platform

gBrowsable = True
try:
    import qfs
except ImportError:
    sys.stderr.write("Warning: %s. Proceeding without file browser.\n" % str(sys.exc_info()[1]))
    gBrowsable = False

class QFSBrowser:

    PERMISSIONS = {
        0 : '---',
        1 : '--x',
        2 : '-w-',
        3 : '-wx',
        4 : 'r--',
        5 : 'r-x',
        6 : 'rw-',
        7 : 'rwx'
    }
    browsable = True

    def __init__(self):
        self.cwd = "/"
        self.browsable = gBrowsable

    def getPermissionStr(self, mode, isDir):
        oPerm = self.PERMISSIONS[mode & 7]
        gPerm = self.PERMISSIONS[(mode >> 3) & 7]
        uPerm = self.PERMISSIONS[(mode >> 6) & 7]
        if isDir:
            dirChar = 'd'
        else:
            dirChar = '-'
        permission = dirChar + uPerm + gPerm + oPerm
        if mode & stat.S_ISUID:
            if (mode & stat.S_IXUSR):
                p3 = 's'
            else:
                p3 = 'S'
            permission = permission[:3] + p3 + permission[4:]
        if mode & stat.S_ISGID:
            if (mode & stat.S_IXGRP):
                p6 = 's'
            else:
                p6 = 'l'
            permission = permission[:6] + p6 + permission[7:]
        if mode & stat.S_ISVTX:
            if (mode & stat.S_IXOTH):
                p9 = 't'
            else:
                p9 = 'T'
            permission = permission[:9] + p9 + permission[10:]
        return permission


    def startHTML(self, directory, buffer):
        if directory == '/':
            parent = '/'
        else:
            if directory.endswith('/'):
                directory = os.path.split(directory)[0]
            parent = os.path.split(directory)[0]

        print >> buffer, '''
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>QFS:%s</title>
    </head>
    <body class="oneColLiqCtr">
        <div id="container">
        <div id="mainContent">
        <h3> Contents of directory %s </h3>
        <P><A href="/"><U>Go back to QFS home</U></A></P>
        <table border="1" cellpadding="2" cellspacing="2">
          <tbody>
            <tr><td style="vertical-align: top;background-color:LightGrey;"><B>Name</B><br></td>
                <td style="vertical-align: top;background-color:LightGrey;"><B>Type</B><br></td>
                <td style="vertical-align: top;background-color:LightGrey;"><B>Size</B><br></td>
                <td style="vertical-align: top;background-color:LightGrey;"><B>Replication</B><br></td>
                <td style="vertical-align: top;background-color:LightGrey;"><B>Data Stripes</B><br></td>
                <td style="vertical-align: top;background-color:LightGrey;"><B>Recovery Stripes</B><br></td>
                <td style="vertical-align: top;background-color:LightGrey;"><B>Modification Time</B><br></td>
                <td style="vertical-align: top;background-color:LightGrey;"><B>Permission</B><br></td>
                <td style="vertical-align: top;background-color:LightGrey;"><B>Owner</B><br></td>
                <td style="vertical-align: top;background-color:LightGrey;"><B>Group</B><br></td>
            </tr>
            <tr><td style="vertical-align: top;background-color:LightBlue;"><B><a href="/browse-it%s">..</a></B><br></td>
                <td style="background-color:LightBlue;"><br></td>
                <td style="background-color:LightBlue;"><br></td>
                <td style="background-color:LightBlue;"><br></td>
                <td style="background-color:LightBlue;"><br></td>
                <td style="background-color:LightBlue;"><br></td>
                <td style="background-color:LightBlue;"><br></td>
                <td style="background-color:LightBlue;"><br></td>
                <td style="background-color:LightBlue;"><br></td>
                <td style="background-color:LightBlue;"><br></td>
            </tr>''' % (directory, directory, parent)

    def endHTML(self, buffer):
        print >> buffer, '''
        </div>
        </div>
    </body>
</html>'''

    def contentHTML(self, path, info, fullStat, buffer):
        if fullStat[9] == 1:
            dataStripes = fullStat[11]
            recoveryStripes = fullStat[12]
        else:
            dataStripes = '-'
            recoveryStripes = '-'
        permissions = self.getPermissionStr(fullStat[8], fullStat[0] == 'dir')

        if fullStat[0] == 'dir':
            target = '/browse-it%s' % path
            print >> buffer, '''
                <tr><td style="vertical-align: top;background-color:LightBlue;"><B><a href="%s">%s</a></B><br></td>
                ''' % (target, info[0])
        else:
            print >> buffer, '''
                <tr><td style="vertical-align: top;background-color:LightBlue;"><B>%s</B><br></td>
                ''' % info[0]

        print >> buffer, '''
                <td style="vertical-align: top;background-color:LightBlue;"><B>%s</B><br></td>
                <td style="vertical-align: top;background-color:LightBlue;"><B>%s</B><br></td>
                <td style="vertical-align: top;background-color:LightBlue;"><B>%s</B><br></td>
                <td style="vertical-align: top;background-color:LightBlue;"><B>%s</B><br></td>
                <td style="vertical-align: top;background-color:LightBlue;"><B>%s</B><br></td>
                <td style="vertical-align: top;background-color:LightBlue;"><B>%s</B><br></td>
                <td style="vertical-align: top;background-color:LightBlue;"><B>%s</B><br></td>
                <td style="vertical-align: top;background-color:LightBlue;"><B>%s</B><br></td>
                <td style="vertical-align: top;background-color:LightBlue;"><B>%s</B><br></td>
            </tr>''' % (info[5],
                        fullStat[3],
                        fullStat[5],
                        dataStripes,
                        recoveryStripes,
                        fullStat[2],
                        permissions,
                        fullStat[6],
                        fullStat[7])


    def printToHTML(self, directory, host, port, buffer):
        client = qfs.client((host, port))
        self.startHTML(directory, buffer)
        dirPath = os.path.join("/", directory)
        for info in client.readdirplus(dirPath):
            if info[0] in ('.', '..'):
                continue
            pathEntry = os.path.join(dirPath, info[0])
            fullStat = client.fullstat(pathEntry)
            self.contentHTML(pathEntry, info, fullStat, buffer)
        self.endHTML(buffer)
