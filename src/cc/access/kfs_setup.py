#
# $Id$
#
# Created 2006
# Author: Sriram Rao (Kosmix Corp)
#
# Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
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

import os
import sys
from distutils.core import Extension, setup

kfs_access_dir = os.path.dirname(os.path.realpath(sys.argv[0]))

# The following assumes that QFS was build by running cmake and then
# make install
libs_dir = "lib"
if os.path.exists("qfs"):
    # kfs_setup.py symlinked to setup.py and build directory symlinked to qfs.
    # QFS shared libraries are packaged into qfs/lib and QFS C extension's run
    # time path is set accordingly.
    libs_dir = os.path.join("qfs", libs_dir)
    setup_extra_args = {
        "packages": ["qfs"],
        "package_data": {
            "qfs": [
                os.path.join("lib", f)
                for f in os.listdir(libs_dir)
                if os.path.isfile(os.path.join(libs_dir, f))
                and not os.path.islink(os.path.join(libs_dir, f))
                and "qfsc." not in f
                and "qfss3io." not in f
                and "qfs_access." not in f
            ]
        },
        # "ext_package": "qfs",
    }
    extension_extra_args = {
        "runtime_library_dirs": {
            "darwin": ["@loader_path/qfs/lib"],
            "win32": [],
        }.get(sys.platform, ["$ORIGIN/qfs/lib"])
    }
else:
    # Old way of invoking kfs_setup.py from build directory.
    setup_extra_args = {}
    extension_extra_args = {}

qfs_ext = Extension(
    "qfs",
    include_dirs=[os.path.abspath(os.path.join(kfs_access_dir, ".."))],
    libraries=["qfs_client"],
    library_dirs=[libs_dir],
    sources=[
        os.path.abspath(os.path.join(kfs_access_dir, "kfs_module_py.cc"))
    ],
    **extension_extra_args,
)

setup(
    name="qfs",
    version="2.5",
    description="QFS client module",
    author="Blake Lewis and Sriram Rao",
    ext_modules=[qfs_ext],
    **setup_extra_args,
)
