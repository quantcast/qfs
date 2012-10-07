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
#  Note: The Python Extension Module is in experimental stage. Please use it
#        with caution.
#
#  This script uses python distutils to build and install the python KFS
#  access module. Execute this script from KFS build directory after building
#  libclient.
#
#  Python apps can access KFS after installation of the KFS access module.
#
#  Instructions for using this script is in top level 'doc' directory in the
#  file doc/DeveloperDoc.
#

from distutils.core import setup, Extension
import sys
import os
import os.path

kfs_access_dir=os.path.dirname(sys.argv[0])

kfsext = Extension(
    'qfs',
    include_dirs = [
        os.path.abspath(os.path.join(kfs_access_dir, ".."))
    ],
    libraries = [
        'qfs_client',
        'qfs_common',
        'qfs_io',
        'qfs_qcdio',
        'qfs_qcrs'
    ],
    library_dirs = [
        'src/cc/libclient',
        'src/cc/common',
        'src/cc/kfsio',
        'src/cc/qcdio',
        'src/cc/qcrs',
    ],
    runtime_library_dirs = [],
    sources = [
         os.path.abspath(os.path.join(kfs_access_dir, "kfs_module_py.cc"))
    ],
)

# OSX boost ports typically end up at /opt/local/lib
if sys.platform in ('darwin', 'Darwin'):
    kfsext.library_dirs.append('/opt/local/lib')
    kfsext.libraries.append('boost_regex-mt')
else:
    kfsext.libraries.append('boost_regex')

setup(
    name = "qfs", version = "1.0",
    description="QFS client module",
    author="Blake Lewis and Sriram Rao",
    ext_modules = [kfsext]
)
