#!/bin/sh
NAME=qfs-client
VERSION=1.0
QFS_BUILD_ROOT=/tmp/
SOURCE_DIR=/tmp/
QFS_INSTALL_PREFIX=/opt/qc/qfs/client

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
sudo rm -rf $QFS_BUILD_ROOT$NAME_$VERSION
mkdir -p $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/bin
mkdir -p $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/conf
mkdir -p $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/lib
mkdir -p $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/include

install -m 755 $SOURCE_DIR/qfs/build/release/bin/tools/* $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/bin
install -m 644 $SOURCE_DIR/qfs/conf/QfsClient.prp $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/conf
install -m 644 $SOURCE_DIR/qfs/build/release/lib/lib* $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX/lib
cp -a $SOURCE_DIR/qfs/build/release/include $QFS_BUILD_ROOT$NAME"_"$VERSION$QFS_INSTALL_PREFIX

#ADD DEBIAN/control
mkdir -p $QFS_BUILD_ROOT$NAME"_"$VERSION/DEBIAN
cp $SOURCE_DIR/qfs/contrib/package/deb/qfs-client.DEBIAN.control $QFS_BUILD_ROOT$NAME"_"$VERSION/DEBIAN/control

#MODIFY
sudo chown -R root:root $QFS_BUILD_ROOT$NAME"_"$VERSION

#BUILD DEB
dpkg-deb --build $QFS_BUILD_ROOT$NAME"_"$VERSION
mv $QFS_BUILD_ROOT$NAME"_"$VERSION.deb .
