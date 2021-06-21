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

import (
	"flag"
	"fmt"
	"log"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

const (
	badPath = "not/a/real/path"
	testDir = "/go-qfs-test"
)

var addr string

func init() {
	flag.StringVar(&addr, "qfs.addr", "localhost:10000",
		"Host and Port of metaserver to be used for testing")
}

func TestMain(m *testing.M) {
	flag.Parse()

	qfs, err := Dial(addr)
	if err != nil {
		log.Fatalf("Failed to connect to QFS")
	}

	if exist, err := qfs.Exists(testDir); exist {
		if err != nil {
			log.Fatalf("Failed to check if the test directory %s exists: %v",
				testDir, err)
		}
		err = qfs.RemoveAll(testDir)
		if err != nil {
			log.Fatalf("Failed to delete %s: %v", testDir, err)
		}
	}

	err = qfs.Mkdir(testDir, 0766)
	if err != nil {
		log.Fatalf("Couldn't make directory %s: %v", testDir, err)
	}

	os.Exit(m.Run())
}

func initQfs(t *testing.T) *Client {
	qfs, err := Dial(addr)
	if err != nil {
		t.Fatalf("Failed opening a connection to QFS: %v", err)
	}
	return qfs
}

func TestIntegration(t *testing.T) {
	qfs, err := Dial(addr)

	if err != nil {
		t.Fatalf("could not connect to qfs: %v", err)
	}
	defer qfs.Close()

	root, err := qfs.Open("/")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()

	fi, err := root.Stat()

	if err != nil {
		t.Fatal(err)
	}

	isdir, err := qfs.IsDirectory("/")
	if err != nil {
		t.Fatal(err)
	}
	exists, err := qfs.Exists("/")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() || !isdir || !exists {
		t.Fatalf("expected root to be a directory")
	}

	// Test a directory that shouldn't exist
	funcs := [](func(string) (bool, error)){qfs.IsDirectory, qfs.IsFile, qfs.Exists}
	fakeDir := "not a directory"
	for _, existFunc := range funcs {
		res, err := existFunc(fakeDir)
		if err != nil {
			t.Fatal(err)
		}
		if res {
			t.Errorf("The file '%s' should not exist", fakeDir)
		}
	}

	fis, err := root.Readdir(-1)

	if err != nil {
		t.Fatal(err)
	}

	for _, fi := range fis {
		t.Log("fi", fi.Name())
	}

}

func TestFileNotFound(t *testing.T) {
	qfs, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}

	_, err = qfs.Stat(badPath)
	if !IsQfsErrorCode(err, unix.ENOENT) {
		t.Errorf("The error should have been ENOENT, not: %v", err)
	}

	_, err = qfs.Open(badPath)
	if !IsQfsErrorCode(err, unix.ENOENT) {
		t.Errorf("The error should have been ENOENT, not: %v", err)
	}
}

func TestDial(t *testing.T) {
	_, err := Dial("not_an_address")
	if err == nil {
		t.Errorf("Dialing an address without a port should throw an error")
	}

	_, err = Dial("address:not_port")
	if err == nil {
		t.Errorf("Entering a non-numeric port should throw an error")
	}

	_, err = Dial(addr)
	if err != nil {
		t.Errorf("QFS didn't connect: %v", err)
	}
}

func TestOpen(t *testing.T) {
	qfs := initQfs(t)

	_, err := qfs.Open(badPath)
	if !IsQfsErrorCode(err, unix.ENOENT) {
		t.Errorf("Opening a nonexistant file should throw ENOENT, not: %v", err)
	}

	_, err = qfs.Open("/")
	if err != nil {
		t.Errorf("Failed to open the file: %v", err)
	}
}

func ExampleClient_Open() {
	qfs, err := Dial(addr)
	if err != nil {
		fmt.Println(err)
	}

	_, err = qfs.Open("/")
	if err != nil {
		fmt.Println(err)
	}
	// Output:
}

func TestClose(t *testing.T) {
	qfs := initQfs(t)

	qfs.Close()
	if !qfs.closed {
		t.Error("The QFS handle didn't say its closed")
	}

	qfs.Close()
	if !qfs.closed {
		t.Error("The QFS handle didn't say its closed")
	}
}

func ExampleClient_Close() {
	qfs, err := Dial(addr)
	if err != nil {
		fmt.Println(err)
	}
	qfs.Close()
	// Output:
}

func TestStat(t *testing.T) {
	qfs := initQfs(t)

	_, err := qfs.Stat(badPath)
	if err == nil {
		t.Error("Stat didn't return an error on a nonexistant file")
	}

	_, err = qfs.Stat("/")
	if err != nil {
		t.Errorf("Stat should have succeeded: %v", err)
	}
}

func ExampleClient_Stat() {
	qfs, err := Dial(addr)
	if err != nil {
		fmt.Println(err)
	}

	fileInfo, err := qfs.Stat("/")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Print(fileInfo.Name())
	// Output: /
}

func TestExists(t *testing.T) {
	qfs := initQfs(t)

	exists, err := qfs.Exists(badPath)
	if err != nil {
		t.Errorf("Exists should not have returned an error: %v", err)
	}
	if exists {
		t.Error("Calling Exists on a bad path should return false")
	}

	exists, err = qfs.Exists("/")
	if err != nil {
		t.Errorf("Exists should not have returned an error: %v", err)
	}
	if !exists {
		t.Error("Calling Exists on the root should return true")
	}
}

func TestIsFile(t *testing.T) {
	qfs := initQfs(t)

	isFile, err := qfs.IsFile(badPath)
	if err != nil {
		t.Errorf("IsFile should not have returned an error: %v", err)
	}
	if isFile {
		t.Error("A bad path isn't a file")
	}

	isFile, err = qfs.IsFile("/")
	if err != nil {
		t.Errorf("IsFile should not have returned an error: %v", err)
	}
	if isFile {
		t.Error("The root is not a file")
	}
}

func TestIsDirectory(t *testing.T) {
	qfs := initQfs(t)

	isDir, err := qfs.IsDirectory(badPath)
	if err != nil {
		t.Errorf("IsDirectory should not have returned an error: %v", err)
	}
	if isDir {
		t.Error("A bad path isn't a directory")
	}

	isDir, err = qfs.IsDirectory("/")
	if err != nil {
		t.Errorf("IsDirectory should not have returned an error: %v", err)
	}
	if !isDir {
		t.Error("The root should be a directory")
	}
}

func TestMkdirRmdir(t *testing.T) {
	qfs := initQfs(t)

	path := "/test_dir"
	err := qfs.Mkdir(path, 0666)
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	if res, err := qfs.IsDirectory(path); !res || err != nil {
		t.Errorf("Failed to create directory %s, exists: %v, err: %v",
			path, res, err)
	}

	err = qfs.Remove(path)
	if err != nil {
		t.Fatalf("Failed to remove directory: %v", err)
	}

	if res, err := qfs.IsDirectory(path); res || err != nil {
		t.Errorf("Failed to remove directory %s, exists: %v, err: %v",
			path, res, err)
	}
}

func TestMkdirAllRemoveAll(t *testing.T) {
	qfs := initQfs(t)

	path := "/very/deep/test/directory"
	err := qfs.MkdirAll(path, 0766)
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	if res, err := qfs.IsDirectory(path); !res || err != nil {
		t.Errorf("Failed to create directory %s, exists: %v, err: %v",
			path, res, err)
	}

	err = qfs.RemoveAll(path)
	if err != nil {
		t.Fatalf("Failed to remove directory: %v", err)
	}

	if res, err := qfs.IsDirectory(path); res || err != nil {
		t.Errorf("Failed to remove directory %s, exists: %v, err: %v",
			path, res, err)
	}
}

func TestUmask(t *testing.T) {
	qfs := initQfs(t)

	umasks := []os.FileMode{0666, 0777, 0743, 0611}
	for _, umask := range umasks {
		qfs.UmaskSet(umask)

		actualUmask, err := qfs.UmaskGet()
		if err != nil {
			t.Fatalf("Got an error getting a umask: %v", err)
		}
		if umask != actualUmask {
			t.Errorf("Umasks did not match\nExpected: %v\nActual:%v", umask,
				actualUmask)
		}
	}
}
