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
	"syscall"
)

// Error encapsulates the error return values form QFS operations.  A call
// to the QFS API is used to generate a human readable string from the error
// code.  The error codes are generally the same as standard unix codes, and
// this type can be compared to those values to determine the cause of an error
// when necessary.
type Error struct {
	Err syscall.Errno
}

func (e Error) Error() string {
	var buf [256]C.char
	return fmt.Sprintf("qfs error: %s", C.GoString(C.qfs_strerror(
		C.int(e.Err)*-1,
		(*C.char)(&buf[0]),
		C.size_t(len(buf)),
	)))
}

func errFromQfs(code C.int) error {
	if code < 0 {
		code = -code
	}
	return &Error{syscall.Errno(code)}
}

// IsQfsErrorCode returns if the error is for the specified error code
// (syscall.Errno)
func IsQfsErrorCode(err error, code syscall.Errno) bool {
	e, ok := err.(*Error)
	return ok && e.Err == code
}

// ErrPartialWrite is thrown when not all of the bytes given to a File.Write
// call are written to QFS.
type ErrPartialWrite struct {
	written int
	total   int
	path    string
}

func (e ErrPartialWrite) Error() string {
	return fmt.Sprintf("only wrote %d out of %d bytes to %s", e.written,
		e.total, e.path)
}
