#
# $Id$
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

%define debug_package %{nil}
%define debug_packages %{nil}

Summary: QFS Meta Server Package
Name: qfs-metaserver
Version: 1.0.0
Release: 0
License: Apache
Group: Applications/Distributed
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

BuildRequires: boost

%define _install_prefix /opt/qc/qfs/metaserver

%description
This package contains the Quantcast Distributed Filesystem meta server.

%prep
cd %{_sourcedir}
if [ -d qfs ]; then
    rm -rf qfs
fi
git clone https://github.com/quantcast/qfs.git

%build
cd %{_sourcedir}/qfs
make release

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}%{_install_prefix}/bin
mkdir -p %{buildroot}%{_install_prefix}/sbin
mkdir -p %{buildroot}%{_install_prefix}/conf
install -m 755 %{_sourcedir}/qfs/build/release/bin/metaserver %{buildroot}%{_install_prefix}/bin
install -m 755 %{_sourcedir}/qfs/build/release/bin/filelister %{buildroot}%{_install_prefix}/bin
install -m 755 %{_sourcedir}/qfs/build/release/bin/qfsfsck %{buildroot}%{_install_prefix}/bin
install -m 755 %{_sourcedir}/qfs/build/release/bin/logcompactor %{buildroot}%{_install_prefix}/bin
cp -a %{_sourcedir}/qfs/webui %{buildroot}%{_install_prefix}/
install -m 644 %{_sourcedir}/qfs/conf/MetaServer.prp %{buildroot}%{_install_prefix}/conf

%clean
rm -rf %{buildroot}
rm -rf %{_sourcedir}/qfs

%pre

%post

%preun

%files
%defattr(-,root,root,-)
%{_install_prefix}/*

%postun

%changelog
