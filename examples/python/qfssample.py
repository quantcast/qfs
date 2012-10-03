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
# NOTE: The python support for QFS is EXPERIMENTAL at this stage. The
#       python extension module has not been tested on large scale
#       deploymentsi yet. Please excercise caution while using the
#       python module.

"""
This simple test tries to create some files and directories, and write some
data at specific offsets in the created files. Then it tries to ensure that
the created paths are valid, and that the file contents are as expected.

To run this script,
  - Prepare qfs.so as described in the file 'doc/ClientDeveloperDoc'
  - Ensure that the QFS metaserver and chunkserver are running.
  - Ensure that the metaserver host/port matches the contents of argv[1].
  - Ensure that the PYTHONPATH and LD_LIBRARY_PATH are set accordingly.
  eg: PYTHONPATH=${PYTHONPATH}:~/code/qfs/build/lib/lib64/python \
      LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:~/code/qfs/build/lib    \
      python ./qfssample.py qfssample.cfg
"""

import os
import sys
import time
import errno

import qfs

def ParseConfig(config):
    host = ''
    port = -1
    for line in open("qfssample.cfg"):
        if line.startswith("#") or len(line.strip()) == 0:
            continue
        s = line.strip()
        if s.split('=')[0].strip() == 'metaServer.name':
            host = s.split('=')[1].strip()
        elif s.split('=')[0].strip() == 'metaServer.port':
            port = int(s.split('=')[1].strip())
    if (host,port) == ('', -1):
        sys.exit('Failed to parse config file')
    return (host,port)


def main():
    if len(sys.argv) < 2:
        sys.exit('Usage: %s config_file' % sys.argv[0])

    client = None
    server = ParseConfig(sys.argv[1])

    try:
        client = qfs.client(server)
    except:
        print "Unable to start the QFS client."
        print "Make sure that the meta- and chunkservers are running."
        sys.exit(1)

    testBaseDir = "qfssample_base"
    testDirs  = ("dir1", "dir2")
    testFile1 = "dir1/file1"
    testFile2 = "file2"
    file1Content = "Cu populo nusquam alienum vim, graece latine prodesset ex qui, quo ea lucilius intellegat."
    file2ContentA = { 0       : "are ",    # at offset 0
                      40      : "you ",    # at offset 40
                      1030    : "always ",
                      1048580 : "wrong?" }
    file2ContentB = { 500     : "really " }

    client.cd("/")

    try: # just in case we didn't cleanup last time
        client.rmdirs(testBaseDir)
    except IOError, err:
        pass

    client.mkdir(testBaseDir)
    client.cd(testBaseDir)
    for td in testDirs:
        client.mkdir(td)
    time.sleep(1)
    print "Created directories."

    client.cd("/" + testBaseDir)
    f1 = client.create(testFile1, 2)
    f2 = client.create(testFile2, 3)

    f1.write(file1Content)
    for offset, content in file2ContentA.items():
        f2.seek(offset)
        f2.write(content)
    print "Created files."

    f1.sync()
    f1.close()
    f2.sync()
    f2.close()
    time.sleep(1)
    print "Closed files (first time)."

    f1 = client.open(testFile1, 'r')
    f2 = client.open(testFile2, 'w')
    print "Opened files."

    for offset, content in file2ContentB.items():
        f2.seek(offset)
        f2.write(content)

    f1.sync()
    f1.close()
    f2.sync()
    f2.close()
    time.sleep(1)
    print "Closed files (second time)."

    # Verify if everything is fine.
    client.cd("/")
    expected = ("dir1", "dir2", "file2")
    for node in client.readdir(testBaseDir):
        print node
        if node in (".", ".."):
            continue
        if node not in expected:
            sys.exit("%s is not in expected list %r" % (node, expected))

    expected = ("file1")
    for node in client.readdir(testBaseDir + "/dir1"):
        print node
        if node in (".", ".."):
            continue
        if node not in expected:
            sys.exit("%s is not in expected list %r" % (node, expected))
        print "Created paths are in order."

    filePath1 = testBaseDir + "/" + testFile1
    filePath2 = testBaseDir + "/" + testFile2

    print "Stat for %s is %r" % (filePath1, client.stat(filePath1))
    print "Stat for %s is %r" % (filePath2, client.stat(filePath2))

    f1 = client.open(filePath1, 'r')
    out = f1.read(2)
    if (out != "Cu"):
        sys.exit("Error: Expected 'Cu', got '%s'.", out)
    f1.seek(31)
    out = f1.read(6)
    if (out != "graece"):
        sys.exit("Error: Expected 'graece', got '%s'.", out)
    pos = f1.tell()
    if pos != 37:
        sys.exit("Error: Expected 'pos = 37', got 'pos = %d'.", pos)
    f1.close()
    print "File1 contents are in order"

    f2 = client.open(filePath2, 'r')
    f2.seek(1032)
    out = f2.read(3)
    if (out != "way"):
        sys.exit("Error: Expected 'way', got '%s'.", out)
    f2.seek(1048578)
    out = f2.read(7)
    if out[2:] != "wrong":
        sys.exit("Error: Expected '..wrong', got '%r'.", out)
    f2.close()
    print "File2 contents are in order"

    client.rmdirs(testBaseDir)

if __name__ == '__main__':
    main()

