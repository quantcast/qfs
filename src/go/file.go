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
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"unsafe"
)

// File represents a file in QFS that can be operated on.
type File struct {
	qfs  *Client
	fd   C.int
	path string // Hold onto the opening path. Shouldn't be an issue with qfs.
	m    sync.Mutex

	rfr   rawFileReader
	buf   *bufio.Reader
	dirty bool // Set this after a read to signal that seek should reset the buffer.

	// iter manages re-entrant directory traversal
	iter *C.struct_qfs_iter

	closed bool
}

// Initializes a new file from the specified file descriptor and path.  The
// reading is wrapped by a bufio.Reader, which is allocated from a pool
// maintained in the Client.
func newFile(qfs *Client, fd C.int, path string) *File {
	// This makes reads a little more robust. Maybe this isn't correct but it
	// doesn't seem to hurt.
	C.qfs_set_skipholes(qfs.qfs, fd)

	f := &File{
		qfs:  qfs,
		fd:   fd,
		path: path,
	}

	f.rfr.f = f

	// Setup the buffered reader.
	if buf := qfs.buffers.Get(); buf != nil {
		f.buf = buf.(*bufio.Reader)
		f.buf.Reset(f.rfr) // Set the buffer to read from the QFS file.
	} else {
		f.buf = bufio.NewReaderSize(f.rfr,
			// Just set the Go buffered reader size to match the internal
			// buffer size of the qfs client. This ensures that we copy all
			// the client-side data over to the go side, minimizing the number
			// of cgo calls made to qfs.
			int(C.qfs_get_iobuffersize(f.qfs.qfs, f.fd)))
	}

	return f
}

// TODO(sday): Right now, locking is pretty course grained to prevent race
// conditions. Once we have time to optimize, many of these locks should be
// released while issuing calls out to cgo that might block while waiting for
// qfs io. Essentially, we need to arrange it such that we defer concurrency
// management to the qfs library, where possible. This may not be an issue for
// Files, as they should generally not be shared among goroutines.

// Close the file. Calling this method after the file is already closed will
// not throw any errors.
func (f *File) Close() error {
	f.m.Lock()
	defer f.m.Unlock()

	return f.close()
}

// Stat returns an FileInfo struct representing details about the underlying
// file.
func (f *File) Stat() (os.FileInfo, error) {
	f.m.Lock()
	defer f.m.Unlock()

	return f.stat()
}

// Readdir reads the contents of the directory associated with file and returns
// a slice of up to n FileInfo values, as would be returned by Lstat, in
// directory order. Subsequent calls on the same file will yield further FileInfos.
func (f *File) Readdir(count int) ([]os.FileInfo, error) {
	f.m.Lock()
	defer f.m.Unlock()

	if f.closed {
		// Return EOF when directory is closed.
		return nil, io.EOF
	}

	var entries []os.FileInfo
	var attr C.struct_qfs_attr

	// We use count == 0 for the breaking value in the loop below. To meet the
	// Readdir interface, we must return all entries if count <= 0 is passed
	// in, so we fix up count to zero.
	if count <= 0 {
		count = -1
	}

	for count != 0 {
		cpath := C.CString(f.path)
		defer freeCString(cpath)

		left := C.qfs_readdir(f.qfs.qfs, cpath, &f.iter, &attr)
		if left < 0 {
			return nil, errFromQfs(left)
		}

		if left == 0 {
			// Since we have a directory, we need to mimic readdir being
			// single flight. File will be closed after read is completed.
			if err := f.close(); err != nil {
				return nil, err
			}

			break
		}

		fi := newFileInfo(attr)
		fname := fi.Name()

		if fname != "." && fname != ".." { // These should not be in directory listings
			entries = append(entries, fi)
			count--
		}
	}

	return entries, nil
}

// Read all the bytes from p and write them to the file.
func (f *File) Read(p []byte) (int, error) {
	f.m.Lock()
	defer f.m.Unlock()
	f.dirty = true

	return f.buf.Read(p)
}

// Seek changes the current position in the file to the specified offset.
// See https://golang.org/pkg/io/#pkg-constants for the various options for
// `whence`
func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.m.Lock()
	defer f.m.Unlock()

	// off_t qfs_seek(struct QFS* qfs, int fd, off_t offset, int whence);
	noffset := C.qfs_seek(f.qfs.qfs, f.fd, C.off_t(offset), C.int(whence))

	if noffset < 0 {
		return 0, errFromQfs(C.int(noffset))
	}

	if f.dirty {
		// Reset the buffer state after a seek.
		f.buf.Reset(f.rfr)
		f.dirty = false

		// BUG(sday): Clearing out this buffer is *not* the correct action if
		// the seek doesn't break the bounds of the buffer. This issue was
		// detected because the http.FileSystem implementation does a Read on
		// the start of the file to sniff the content type, then seeks back to
		// 0 to enable the main read. Because the buffer is reset, the round
		// trip (either cgo -> KfsClient or on the network) is issued a second
		// time. This negatively affects fetch performance for small files.
	}

	return int64(noffset), nil
}

// Write len(p) bytes from p to the file.  Implements io.Writer.
func (f *File) Write(p []byte) (int, error) {
	// Check that the data to write isn't empty
	if len(p) == 0 {
		return 0, nil
	}

	f.m.Lock()
	defer f.m.Unlock()

	n := int(C.qfs_write(f.qfs.qfs, f.fd, unsafe.Pointer(&p[0]), C.size_t(len(p))))

	// Return a QFS error if n < 0, or a ErrPartialWrite if n < len(p)
	if n < 0 {
		return n, errFromQfs(C.int(n))
	} else if n < len(p) {
		return n, &ErrPartialWrite{written: n, total: len(p), path: f.path}
	}

	return n, nil
}

// WriteAt writes len(p) bytes from p to the file at the absolute address off.
// The current position of the file does not affect this method.  Implements
// io.WriterAt.
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	// Check that the data to write isn't empty
	if len(p) == 0 {
		return 0, nil
	}

	f.m.Lock()
	defer f.m.Unlock()

	n := int(C.qfs_pwrite(f.qfs.qfs, f.fd, unsafe.Pointer(&p[0]),
		C.size_t(len(p)), C.off_t(off)))

	// Return a QFS error if n < 0, or a ErrPartialWrite if n < len(p)
	if n < 0 {
		return n, errFromQfs(C.int(n))
	} else if n < len(p) {
		return n, &ErrPartialWrite{written: n, total: len(p), path: f.path}
	}

	return n, nil
}

// Sync forces the written data to be synced out to the chunk servers.
func (f *File) Sync() error {
	res := C.qfs_sync(f.qfs.qfs, f.fd)
	if res < 0 {
		return errFromQfs(res)
	}
	return nil
}

// Name returns the name (or path) to the file as given in the Client.Open call.
func (f *File) Name() string {
	return f.path
}

func (f *File) close() error {
	if f.closed {
		return nil
	}
	f.closed = true

	if ec := C.qfs_close(f.qfs.qfs, f.fd); ec < 0 {
		return errFromQfs(ec)
	}

	C.qfs_iter_free(&f.iter)

	// Return the buffer to the pool
	f.qfs.buffers.Put(f.buf)
	f.buf = nil

	return nil
}

func (f *File) stat() (*FileInfo, error) {
	var attr C.struct_qfs_attr

	ec := C.qfs_stat_fd(f.qfs.qfs, f.fd, &attr)

	if ec < 0 {
		return nil, errFromQfs(ec)
	}

	return newFileInfo(attr), nil
}

// This is an internal type necessary for being able to define a Read
// method on the QFS file that actually uses the API.  The Read method on File
// should instead use the buffered reader that wraps this type.
type rawFileReader struct {
	f *File
}

func (rfr rawFileReader) Read(p []byte) (int, error) {
	n := int(C.qfs_read(rfr.f.qfs.qfs, rfr.f.fd, unsafe.Pointer(&p[0]), C.size_t(len(p))))

	if n < 0 {
		return 0, errFromQfs(C.int(n))
	}

	if n == 0 {
		// HACK(sday): If the reads start returning 0, return EOF when the
		// seek position equals the file size. This may not be right under all
		// conditions, but should prevent issues with sparse files not
		// returning EOF on the last read.

		fi, err := rfr.f.stat()
		if err != nil {
			return 0, fmt.Errorf("error checking file size while reading: %s", err)
		}

		if int64(C.qfs_tell(rfr.f.qfs.qfs, rfr.f.fd)) == fi.Size() {
			return 0, io.EOF
		}
	}

	return n, nil
}
