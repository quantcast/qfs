// Copyright 2016-2017 Quantcast Corporation. All rights reserved.
//
// This file is part of Quantcast File System.
//
// Licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package qfs

// #cgo LDFLAGS: -lqfsc
//
//#include <stdlib.h>
//#include <kfs/c/qfs.h>
//
import "C"
import (
	"os"
	"path"
	"time"
)

// FileInfo provides information about a specific file in QFS.  This implements
// os.FileInfo (https://golang.org/pkg/os/#FileInfo) and therefore provides
// the following methods:
//
// Name() string         base name of the file
// Size() int64          length in bytes for regular files
// Mode() FileMode       file mode bits
// ModTime() time.Time   modification time
// IsDir() bool          abbreviation for Mode().IsDir()
// Sys() interface{}     a FileInfoAttr struct
type FileInfo struct {
	Attr *FileInfoAttr
}

var _ os.FileInfo = &FileInfo{}

func newFileInfo(attr C.struct_qfs_attr) *FileInfo {
	return &FileInfo{Attr: newFileInfoAttr(attr)}
}

// Name returns the base name of the file
func (fi *FileInfo) Name() string {
	return path.Base(fi.Attr.Path)
}

// Size returns the size in bytes of the file.
func (fi *FileInfo) Size() int64 {
	return int64(fi.Attr.Size)
}

// Mode returns an os.FileMode for the mode of the file.
func (fi *FileInfo) Mode() os.FileMode {
	return fi.Attr.Mode
}

// ModTime returns the time last modified.
func (fi *FileInfo) ModTime() time.Time {
	return fi.Attr.Mtime
}

// IsDir returns true if this FileInfo is on a directory
func (fi *FileInfo) IsDir() bool {
	return fi.Attr.IsDirectory
}

// Sys returns a qfs_attr struct which contains additional details about the
// file.  The fields exposed by the qfs_attr struct are detailed at
// https://github.com/quantcast/qfs/blob/9f6b4887b6fe60ee06e39bacf7fa2e14436bf5b0/src/cc/qfsc/qfs.h#L48
func (fi *FileInfo) Sys() interface{} {
	return fi.Attr
}

func timevalToTime(tval C.struct_timeval) time.Time {
	return time.Unix(
		int64(tval.tv_sec),
		int64(tval.tv_usec)*int64(time.Microsecond))
}

// FileInfoAttr exposes the underlying qfs_attr struct for more information if needed.
// You can see https://github.com/quantcast/qfs/blob/9f6b4887b6fe60ee06e39bacf7fa2e14436bf5b0/src/cc/qfsc/qfs.h#L48
// for more information on the meaning of the fields.
type FileInfoAttr struct {
	Path            string
	UID             uint64
	GID             uint64
	Mode            os.FileMode
	Inode           int64
	Mtime           time.Time
	Ctime           time.Time // attribute change time
	CRtime          time.Time // creation time
	IsDirectory     bool
	Size            uint64
	Chunks          uint64
	Directories     uint64
	Replicas        int16
	Stripes         int16
	RecoveryStripes int16
	StripeSize      int32
}

func newFileInfoAttr(attr C.struct_qfs_attr) *FileInfoAttr {
	return &FileInfoAttr{
		Path:            C.GoString(&attr.filename[0]),
		UID:             uint64(attr.uid),
		GID:             uint64(attr.gid),
		Mode:            os.FileMode(attr.mode),
		Inode:           int64(attr.id),
		Mtime:           timevalToTime(attr.mtime),
		Ctime:           timevalToTime(attr.ctime),
		CRtime:          timevalToTime(attr.crtime),
		IsDirectory:     bool(attr.directory),
		Size:            uint64(attr.size),
		Chunks:          uint64(attr.chunks),
		Directories:     uint64(attr.directories),
		Replicas:        int16(attr.replicas),
		Stripes:         int16(attr.stripes),
		RecoveryStripes: int16(attr.recovery_stripes),
		StripeSize:      int32(attr.stripe_size),
	}
}
