#!/bin/sh
NAME=qfs-metaserver
VERSION=1.0
QFS_BUILD_ROOT=/tmp/
SOURCE_DIR=/tmp/
QFS_INSTALL_PREFIX=/opt/qc/qfs/metaserver

#PREP
cd $SOURCE_DIR
if [ -d qfs ]; then
    rm -rf qfs
fi
git clone https://github.com/quantcast/qfs.git

#BUILD
cd qfs
make release

#INSTALL
rm -rf $QFS_BUILD_ROOT$NAME_$VERSION
mkdir -p $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/bin
mkdir -p $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/sbin
mkdir -p $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/conf
install -m 755 $SOURCE_DIR/qfs/build/release/bin/metaserver $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/bin
install -m 755 $SOURCE_DIR/qfs/build/release/bin/filelister $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/bin
install -m 755 $SOURCE_DIR/qfs/build/release/bin/qfsfsck $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/bin
install -m 755 $SOURCE_DIR/qfs/build/release/bin/logcompactor $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/bin
cp -a $SOURCE_DIR/qfs/webui $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/
install -m 644 $SOURCE_DIR/qfs/conf/MetaServer.prp $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/conf

#ADD DEBIAN/control
mkdir -p $QFS_BUILD_ROOT$NAME"_"$VERSION/DEBIAN
cp $SOURCE_DIR/qfs/contrib/package/deb/qfs-metaserver.DEBIAN.control $QFS_BUILD_ROOT$NAME"_"$VERSION/DEBIAN/control

#MODIFY
sudo chown -R root:root $QFS_BUILD_ROOT$NAME"_"$VERSION

#BUILD DEB
dpkg-deb --build $QFS_BUILD_ROOT$NAME"_"$VERSION
mv $QFS_BUILD_ROOT$NAME"_"$VERSION.deb .
