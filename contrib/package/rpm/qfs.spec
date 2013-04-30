%if %{?!QFS_VERSION:1}0
%global QFS_VERSION 1.0.2
%endif
%if %{?!QFS_RELEASE:1}0
%global QFS_RELEASE 0
%endif
%if %{?!HADOOP_VERSION:1}0
%global HADOOP_VERSION 1.0.4
%endif
%if %{?!HADOOP_HOME:1}0
%global HADOOP_HOME /usr/lib/hadoop
%endif
%if %{?!JDK_LOCATION:1}0
%global JDK_LOCATION /usr/lib/jvm/java
%endif

%global QFS_SOURCE_DIR %{_builddir}/qfs-%{QFS_VERSION}

# Sets the maven package name based upon the distribution we're building for.
%if 0%{?fedora} >= 16
%global MAVEN_PACKAGE maven
%else
%global MAVEN_PACKAGE apache-maven
%endif

Name:           qfs
Version:        %{QFS_VERSION}
Release:        %{QFS_RELEASE}%{?dist}
Summary:        Libraries required for accessing Quantcast File System

Group:          System Environment/Libraries
License:        ASL 2.0
URL:            https://github.com/quantcast/qfs
Source0:        qfs-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

# Patches:
Patch0:         qfs-set-default-directories.patch

BuildRequires:  boost-devel >= 1.3.4
BuildRequires:  cmake >= 2.4.7
BuildRequires:  fuse-devel
BuildRequires:  gcc-c++
BuildRequires:  libuuid-devel
BuildRequires:  make
BuildRequires:  net-tools
BuildRequires:  openssl-devel
BuildRequires:  python-devel
BuildRequires:  xfsprogs-devel

Requires:       boost >= 1.3.4
Requires:       openssl

Obsoletes:      qfs-client

%description
Quantcast File System (QFS) is a high-performance, fault-tolerant, distributed
file system developed to support MapReduce processing or other applications
reading and writing large files sequentially.


%package chunkserver
Group:          System Environment/Daemons
Requires:       qfs%{?_isa} = %{version}-%{release}
Requires:       daemonize
Summary:        Executables required to run the Quantcast File System chunkserver

%description chunkserver
The QFS chunkserver service hosts the binary contents of the QFS distributed
filesystem.  A metaserver coordinates many data nodes running this service,
replicating data amongst chunkservers in a redundant fashion.


%package devel
Group:          Development/Libraries
Requires:       qfs%{?_isa} = %{version}-%{release}
Summary:        Files needed for building Quantcast File System-based applications

%description devel
The QFS devel package contains the headers, static libraries, and developer
tool binaries required to develop applications which build against QFS.


%package fuse
Group:          Applications/System
Requires:       qfs%{?_isa} = %{version}-%{release}
Summary:        Support for mounting the Quantcast File System under FUSE

%description fuse
This package contains the qfs_fuse executable which is required when mounting
QFS distributed filesystems under FUSE.


%package hadoop
Group:          Development/Libraries
BuildRequires:  java-devel
BuildRequires:  jpackage-utils
BuildRequires:  %{MAVEN_PACKAGE} >= 3.0.3
Requires:       qfs%{?_isa} = %{version}-%{release}
Requires:       java >= 1.6.0
Requires:       hadoop = %{HADOOP_VERSION}
Summary:        Quantcast File System plugin JAR for Hadoop

%description hadoop
This package contains a plugin JAR to enable QFS to serve as a drop-in
replacement for HDFS under Hadoop.


%package java
Group:          Development/Libraries
BuildRequires:  java-devel
BuildRequires:  jpackage-utils
BuildRequires:  %{MAVEN_PACKAGE} >= 3.0.3
Requires:       qfs%{?_isa} = %{version}-%{release}
Requires:       java >= 1.6.0
Summary:        Java libraries for accessing the Quantcast File System

%description java
This package contains a JAR which enables Java applications to access QFS via
its JNI interface.


%package metaserver
Group:          System Environment/Daemons
Requires:       qfs%{?_isa} = %{version}-%{release}
Requires:       daemonize
Summary:        Executables required to run the Quantcast File System metaserver

%description metaserver
This package contains the executables required to run the Quantcast File System
metaserver service, which tracks the location of data chunks distributed across
QFS chunkservers.


%package python
Group:          Development/Libraries
BuildRequires:  python-devel
Requires:       qfs%{?_isa} = %{version}-%{release}
Requires:       python
Summary:        Python libraries for accessing the Quantcast File System

%description python
This package contains the libraries required to access the Quantcast File
System libraries via Python.


%package webui
Group:          System Environment/Daemons
Requires:       qfs%{?_isa} = %{version}-%{release}
Requires:       daemonize
Requires:       python
Summary:        Quantcast File System metaserver/chunkserver web frontend

%description webui
This package contains several Python scripts which provide a simple Hadoop-like
Web UI for viewing Quantcast File Server chunkserver and metaserver status.


# Build scripts:
%prep
%setup -q
%patch0 -p1
# Sets up the build directories as per the QFS documentation.
mkdir -p %{QFS_SOURCE_DIR}/build/release


%build
cd %{QFS_SOURCE_DIR}/build/release
# Builds QFS core and client/server executables.
cmake -D JAVA_INCLUDE_PATH=%{JDK_LOCATION}/include -D JAVA_INCLUDE_PATH2=%{JDK_LOCATION}/include/linux %{QFS_SOURCE_DIR}
make %{?_smp_mflags}

# Populates QFS build directory with built executables and libraries.
make install

# Builds QFS Java library JAR and Hadoop plugin JAR.
export HADOOP_TO_BUILD="%{HADOOP_VERSION}"
%{QFS_SOURCE_DIR}/src/java/javabuild.sh
%{QFS_SOURCE_DIR}/src/java/javabuild.sh $HADOOP_TO_BUILD

# Builds QFS Python libraries.
%if 0%{?with_python3}
%{__python3} %{QFS_SOURCE_DIR}/src/cc/access/kfs_setup.py build
%endif # with_python3

%{__python} %{QFS_SOURCE_DIR}/src/cc/access/kfs_setup.py build


%install
rm -rf $RPM_BUILD_ROOT

# Builds installation directory structure.
mkdir -p %{buildroot}%{_bindir}
mkdir -p %{buildroot}%{_datadir}/java
mkdir -p %{buildroot}%{_datadir}/qfs-webui-%{QFS_VERSION}
mkdir -p %{buildroot}%{_docdir}/qfs-%{QFS_VERSION}
mkdir -p %{buildroot}%{_sbindir}
mkdir -p %{buildroot}%{_sysconfdir}/init.d
mkdir -p %{buildroot}%{_sysconfdir}/logrotate.d
mkdir -p %{buildroot}%{_sysconfdir}/qfs
mkdir -p %{buildroot}%{_libdir}
mkdir -p %{buildroot}%{_localstatedir}/qfs
mkdir -p %{buildroot}%{_localstatedir}/qfs/metaserver/checkpoint
mkdir -p %{buildroot}%{_includedir}
mkdir -p %{buildroot}%{HADOOP_HOME}/lib

# Installs QFS binaries.
install -m 755 %{QFS_SOURCE_DIR}/build/release/bin/chunk* %{buildroot}%{_bindir}
install -m 755 %{QFS_SOURCE_DIR}/build/release/bin/file* %{buildroot}%{_bindir}
install -m 755 %{QFS_SOURCE_DIR}/build/release/bin/log* %{buildroot}%{_bindir}
install -m 755 %{QFS_SOURCE_DIR}/build/release/bin/meta* %{buildroot}%{_bindir}
install -m 755 %{QFS_SOURCE_DIR}/build/release/bin/qfs* %{buildroot}%{_bindir}
install -m 755 %{QFS_SOURCE_DIR}/build/release/bin/devtools/* %{buildroot}%{_bindir}
install -m 755 %{QFS_SOURCE_DIR}/build/release/bin/emulator/* %{buildroot}%{_bindir}
install -m 755 %{QFS_SOURCE_DIR}/build/release/bin/tools/* %{buildroot}%{_bindir}

# Installs QFS dynamic and static libraries.
install -m 644 %{QFS_SOURCE_DIR}/build/release/lib/lib* %{buildroot}%{_libdir}
install -m 644 %{QFS_SOURCE_DIR}/build/release/lib/static/* %{buildroot}%{_libdir}

# Installs QFS base configurations.
install -m 644 %{QFS_SOURCE_DIR}/conf/*.prp %{buildroot}%{_sysconfdir}/qfs

# Installs QFS helper scripts.
install -m 755 %{QFS_SOURCE_DIR}/scripts/* %{buildroot}%{_bindir}

# Installs QFS headers.
cp -a %{QFS_SOURCE_DIR}/build/release/include %{buildroot}%{_includedir}

# Installs QFS web UI and creates a symlink to its folder.
cp -rpPR %{QFS_SOURCE_DIR}/webui/* %{buildroot}%{_datadir}/qfs-webui-%{QFS_VERSION}
ln -s %{_datadir}/qfs-webui-%{QFS_VERSION} %{buildroot}%{_datadir}/qfs-webui
mv %{buildroot}%{_datadir}/qfs-webui-%{QFS_VERSION}/server.conf %{buildroot}%{_sysconfdir}/qfs/qfs-webui.conf

# Installs QFS Java libraries.
install -m 755 %{QFS_SOURCE_DIR}/build/java/qfs-access/qfs-access-%{QFS_VERSION}.jar %{buildroot}%{_datadir}/java/qfs-access-%{QFS_VERSION}.jar

# Installs QFS Hadoop libraries.
install -m 755 %{QFS_SOURCE_DIR}/build/java/hadoop-qfs/hadoop-%{HADOOP_VERSION}-qfs-%{QFS_VERSION}.jar %{buildroot}%{HADOOP_HOME}/lib/hadoop-%{HADOOP_VERSION}-qfs-%{QFS_VERSION}.jar

# Installs QFS Python libraries.
cd %{QFS_SOURCE_DIR}/build/release
%if 0%{?with_python3}
%{__python3} %{QFS_SOURCE_DIR}/src/cc/access/kfs_setup.py install --skip-build --root $RPM_BUILD_ROOT
%endif # with_python3
%{__python} %{QFS_SOURCE_DIR}/src/cc/access/kfs_setup.py install --skip-build --root $RPM_BUILD_ROOT

# Installs init scripts.
install -m 755 %{QFS_SOURCE_DIR}/contrib/initscripts/qfs* %{buildroot}%{_sysconfdir}/init.d

# Installs logrotate scripts.
install -m 644 %{QFS_SOURCE_DIR}/contrib/logrotate/qfs* %{buildroot}%{_sysconfdir}/logrotate.d


%clean
rm -rf $RPM_BUILD_ROOT


# Install/uninstall scripts:
%post chunkserver
/sbin/chkconfig --add qfs-chunkserver

%preun chunkserver
if [ $1 -eq 0 ]; then
    /sbin/service qfs-chunkserver stop &>/dev/null || :
    /sbin/chkconfig --del qfs-chunkserver
fi


%post metaserver
/sbin/chkconfig --add qfs-metaserver

%preun metaserver
if [ $1 -eq 0 ]; then
    /sbin/service qfs-metaserver stop &>/dev/null || :
    /sbin/chkconfig --del qfs-metaserver
fi


%post webui
/sbin/chkconfig --add qfs-webui

%preun webui
if [ $1 -eq 0 ]; then
    /sbin/service qfs-webui stop &>/dev/null || :
    /sbin/chkconfig --del qfs-webui
fi


%files
%defattr(-,root,root,-)
%doc README.md LICENSE.txt
%exclude %{_bindir}/qfs_fuse
%{_bindir}/qfs*
%{_bindir}/cp*
%{_libdir}/*.so
%{_localstatedir}/qfs
%config(noreplace) %{_sysconfdir}/qfs/QfsClient.prp


%files chunkserver
%defattr(-,root,root,-)
%{_bindir}/chunk*
%{_sysconfdir}/init.d/qfs-chunkserver
%config(noreplace) %{_sysconfdir}/qfs/ChunkServer.prp
%config(noreplace) %{_sysconfdir}/logrotate.d/qfs-chunkserver


%files devel
%defattr(-,root,root,-)
%{_includedir}/**
%{_libdir}/*.a
%{_bindir}/checksum
%{_bindir}/dirtree_creator
%{_bindir}/logger
%{_bindir}/rand-sfmt
%{_bindir}/rebalance*
%{_bindir}/replica*
%{_bindir}/requestparser
%{_bindir}/sortedhash
%{_bindir}/stlset


%files fuse
%defattr(-,root,root,-)
%{_bindir}/qfs_fuse


%files hadoop
%defattr(-,root,root,-)
%{HADOOP_HOME}/lib/hadoop-%{HADOOP_VERSION}-qfs-%{QFS_VERSION}.jar


%files java
%defattr(-,root,root,-)
%{_datadir}/java/qfs-access-%{QFS_VERSION}.jar


%files metaserver
%defattr(-,root,root,-)
%{_bindir}/metaserver
%{_bindir}/filelister
%{_bindir}/qfsfsck
%{_bindir}/logcompactor
%{_localstatedir}/qfs/metaserver/checkpoint
%{_sysconfdir}/init.d/qfs-metaserver
%config(noreplace) %{_sysconfdir}/qfs/MetaServer.prp
%config(noreplace) %{_sysconfdir}/logrotate.d/qfs-metaserver


%files python
%defattr(-,root,root,-)
%{python_sitearch}/**


%files webui
%defattr(-,root,root,-)
%{_datadir}/qfs-webui-%{QFS_VERSION}
%{_datadir}/qfs-webui
%{_sysconfdir}/init.d/qfs-webui
%config(noreplace) %{_sysconfdir}/qfs/qfs-webui.conf
%config(noreplace) %{_sysconfdir}/logrotate.d/qfs-webui


%changelog
* Sun Oct 28 2012 Steve Salevan <steve@tumblr.com> 1.0.1-1
- Initial specfile writeup.

