// Package qfs interfaces with a instance of the Quantcast File System

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
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

// A Client represents the connection to a QFS instance.
type Client struct {
	qfs *C.struct_QFS
	m   sync.Mutex

	// Set when the connection was closed and the QFS handle has been released.
	closed bool

	// A pool of buffers to use for reading from files.  This pool beings empty,
	// but is added to when the Client wants to create a new file but has no
	// existing buffers in the pool.  This helps minimize initialization of new
	// buffers and garbage collection.
	buffers sync.Pool
}

// Dial conects to QFS at the given address, where address is of the form
// "host:port".  This returns a QFS Client.
func Dial(addr string) (*Client, error) {
	host, portS, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(portS)
	if err != nil {
		return nil, err
	}

	chost := C.CString(host)
	defer freeCString(chost)

	qfs := C.qfs_connect(chost, C.int(port))
	if qfs == nil {
		return nil, fmt.Errorf("unable to connect to QFS at %s", addr)
	}

	C.qfs_set_default_sparsefilesupport(qfs, true)

	client := &Client{qfs: qfs}
	runtime.SetFinalizer(client, freeQFSClient)

	return client, nil
}

// Open a specific file at the specified path.
func (qfs *Client) Open(name string) (*File, error) {
	return qfs.OpenFile(name, os.O_RDONLY, 0666)
}

// OpenFile opens a specific file at the specified path, with the appropriate flags
// and mode string.  The flags are typical unix `open` flags (see
// https://golang.org/pkg/io/#pkg-constants).
func (qfs *Client) OpenFile(name string, flag int, perm os.FileMode) (*File, error) {
	cname := C.CString(name)
	defer freeCString(cname)

	fd := C.qfs_open_file(qfs.qfs, cname, C.int(flag),
		C.uint16_t(perm), nil)
	if fd < 0 {
		return nil, errFromQfs(fd)
	}

	return newFile(qfs, fd, name), nil
}

// Close the Client and release the QFS handle.
func (qfs *Client) Close() {
	qfs.m.Lock()
	defer qfs.m.Unlock()

	if qfs.closed {
		return
	}
	qfs.closed = true

	C.qfs_release(qfs.qfs)
}

// Stat gets the FileInfo type for a given file.
func (qfs *Client) Stat(path string) (*FileInfo, error) {
	var attr C.struct_qfs_attr

	cpath := C.CString(path)
	defer freeCString(cpath)

	res := C.qfs_stat(qfs.qfs, cpath, &attr)

	if res < 0 {
		return nil, errFromQfs(res)
	}

	return newFileInfo(attr), nil
}

// This is a helper method that calls stat, and derives information from the
// call to facilitate other public methods.  It returns a FileInfo struct,
// a bool indicated if or if not the file exists, and optionally an error.
func (qfs *Client) existsWithInfo(path string) (*FileInfo, bool, error) {
	val, err := qfs.Stat(path)

	// Return true if the error is nil, or false if file not found
	if err == nil {
		return val, true, nil
	} else if IsQfsErrorCode(err, unix.ENOENT) {
		return nil, false, nil
	}

	return nil, false, err // There must have been an error
}

// Exists tests if a file or directory at the specified path exists.
func (qfs *Client) Exists(path string) (bool, error) {
	_, exists, err := qfs.existsWithInfo(path)
	return exists, err
}

// IsFile tests if the file at the specified path exists and is a file.
func (qfs *Client) IsFile(path string) (bool, error) {
	val, exists, err := qfs.existsWithInfo(path)
	if err != nil {
		return false, err
	} else if !exists {
		return false, nil
	}
	return exists && !val.IsDir(), nil
}

// IsDirectory tests if the file at the specified path exists and is a directory.
func (qfs *Client) IsDirectory(path string) (bool, error) {
	val, exists, err := qfs.existsWithInfo(path)
	if err != nil {
		return false, err
	} else if !exists {
		return false, nil
	}
	return exists && val.IsDir(), nil
}

// Mkdir creates a new directory with the specified name and permission bits.
func (qfs *Client) Mkdir(path string, perm os.FileMode) error {
	cpath := C.CString(path)
	defer freeCString(cpath)

	res := C.qfs_mkdir(qfs.qfs, cpath, C.mode_t(perm))
	if res < 0 {
		return errFromQfs(res)
	}
	return nil
}

// MkdirAll creates a directory with the specified name, and any parents
// necessary.  All of the directories have the specified permissions.
func (qfs *Client) MkdirAll(path string, perm os.FileMode) error {
	cpath := C.CString(path)
	defer freeCString(cpath)

	res := C.qfs_mkdirs(qfs.qfs, cpath, C.mode_t(perm))
	if res < 0 {
		return errFromQfs(res)
	}
	return nil
}

// Remove removes the specified file or directory.
func (qfs *Client) Remove(path string) error {
	isDir, err := qfs.IsDirectory(path)
	if err != nil {
		return err
	}

	cpath := C.CString(path)
	defer freeCString(cpath)

	var res C.int
	if isDir {
		res = C.qfs_rmdir(qfs.qfs, cpath)
	} else {
		res = C.qfs_remove(qfs.qfs, cpath)
	}

	if res < 0 {
		return errFromQfs(res)
	}
	return nil
}

// RemoveAll removes the specified path, and if it is a directory, removes
// all the children it contains.
func (qfs *Client) RemoveAll(path string) error {
	isDir, err := qfs.IsDirectory(path)
	if err != nil {
		return err
	}

	cpath := C.CString(path)
	defer freeCString(cpath)

	var res C.int
	if isDir {
		res = C.qfs_rmdirs(qfs.qfs, cpath)
	} else {
		res = C.qfs_remove(qfs.qfs, cpath)
	}

	if res < 0 {
		return errFromQfs(res)
	}
	return nil
}

// UmaskSet sets the umask for this Client.
func (qfs *Client) UmaskSet(perm os.FileMode) {
	C.qfs_set_umask(qfs.qfs, C.mode_t(perm))
}

// UmaskGet gets the current umask for this Client.
func (qfs *Client) UmaskGet() (os.FileMode, error) {
	res := C.qfs_get_umask(qfs.qfs)
	if int(res) < 0 {
		return 0, errFromQfs(C.int(res))
	}
	return os.FileMode(res), nil
}

// free frees the memory allocated by a C.CString.  Its probably best to just
// call this immediately after allocation with a "defer" statement
func freeCString(cs *C.char) {
	C.free(unsafe.Pointer(cs))
}

func freeQFSClient(qfs *Client) {
	qfs.Close()
}
