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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestFile_ReadWrite(t *testing.T) {
	qfs := initQfs(t)

	file, err := qfs.OpenFile("/test", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	testData := "test string"
	_, err = io.WriteString(file, testData)
	if err != nil {
		t.Fatalf("Failed to write to %v: %v", file, err)
	}
	file.Close()

	file, err = qfs.OpenFile("/test", os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatalf("Failed to read from %s: %v", file.path, err)
	}

	if string(data) != testData {
		t.Errorf("Got the wrong data\nExpected: %s\tActual: %s", testData,
			string(data))
	}
}

func TestFile_WriteAt(t *testing.T) {
	qfs := initQfs(t)

	file, err := qfs.OpenFile("/test", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	testData := "test  string"
	_, err = io.WriteString(file, testData)
	if err != nil {
		t.Fatalf("Failed to write to %v: %v", file, err)
	}
	file.Close()

	file, err = qfs.OpenFile("/test", os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	offset := int64(4)
	_, err = file.WriteAt([]byte("ed"), offset)
	if err != nil {
		t.Fatalf("Failed to write to %v at %d: %v", file, offset, err)
	}
	file.Close()

	file, err = qfs.OpenFile("/test", os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatalf("Failed to read from %s: %v", file.path, err)
	}

	expected := "testedstring"
	if string(data) != expected {
		t.Errorf("Got the wrong data\nExpected: %s\tActual: %s", expected,
			string(data))
	}
}

// Test that writing an array of empty bytes doesn't panic
func TestFile_EmptyWrite(t *testing.T) {
	qfs := initQfs(t)
	fpath := path.Join(testDir, "empty_write_test")

	file, err := qfs.OpenFile(fpath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	_, err = file.Write([]byte{})
	if err != nil {
		t.Fatalf("Failed to write to %v: %v", file, err)
	}
}

func TestFile_Seek(t *testing.T) {
	qfs := initQfs(t)
	fpath := path.Join(testDir, "empty_write_test")

	file, err := qfs.OpenFile(fpath, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	testData := "test string"
	_, err = io.WriteString(file, testData)
	if err != nil {
		t.Fatalf("Failed to write to %v: %v", file, err)
	}
	file.Close()

	file, err = qfs.Open(fpath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	offset := int64(5)
	newOffset, err := file.Seek(offset, os.SEEK_SET)
	if err != nil {
		t.Fatalf("Failed to seek to %d in %v: %v", offset, file, err)
	}

	if offset != newOffset {
		t.Errorf("The new offset %d should've been %d", newOffset, offset)
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatalf("Failed to read from %s: %v", file.path, err)
	}

	expected := "string"
	if string(data) != expected {
		t.Errorf("Got the wrong data\nExpected: %s\tActual: %s", expected,
			string(data))
	}
}

func ExampleFile_Readdir() {
	qfs, _ := Dial(addr)

	file, err := qfs.Open("/")
	if err != nil {
		return
	}

	// Read all fo the fileinfos in the directory
	infos, err := file.Readdir(-1)

	for _, info := range infos {
		// Operate on the fileinfo...
		fmt.Printf("Found file: %s", info.Name())
	}
}

func TestFile_Readdir(t *testing.T) {
	qfs := initQfs(t)

	files := []string{"test_file1", "t2", "test3"}

	// construct the files
	for i := range files {
		file, err := qfs.OpenFile(path.Join(testDir, files[i]),
			os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
		if err != nil {
			t.Fatalf("Failed to open file %s: %v", files[i], err)
		}

		_, err = io.WriteString(file, "test")
		if err != nil {
			t.Fatalf("Failed to write to %s: %v", files[i], err)
		}

		file.Close()
	}

	// Verify that all files are shown in Readdir
	dir, err := qfs.Open(testDir)
	if err != nil {
		t.Fatalf("Failed to open %s: %v", testDir, err)
	}
	defer dir.Close()

	infos, err := dir.Readdir(-1)
	if err != nil {
		t.Fatalf("Readdir failed: %v", err)
	}

	checkFunc := func(file string, infos []os.FileInfo) bool {
		for _, info := range infos {
			if file == info.Name() {
				return true
			}
		}
		return false
	}

	for _, file := range files {
		if !checkFunc(file, infos) {
			t.Errorf("Didn't find %s in the output of Readdir: %v", file, infos)
		}
	}

	// Delete the files
	for i := range files {
		err := qfs.Remove(path.Join(testDir, files[i]))
		if err != nil {
			t.Fatalf("Failed to delete file %s: %v", files[i], err)
		}
	}
}

func TestFile_DirtySeek(t *testing.T) {
	qfs := initQfs(t)
	fpath := path.Join(testDir, "dirty_seek")

	file, err := qfs.OpenFile(fpath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Failed to open file %s: %v", fpath, err)
	}

	testData := "test data"
	_, err = io.WriteString(file, testData)
	if err != nil {
		t.Fatalf("Failed to write to %s: %v", fpath, err)
	}
	file.Close()

	// Read the file with a dirty seek
	file, err = qfs.Open(fpath)
	if err != nil {
		t.Fatalf("Failed to open file %s: %v", fpath, err)
	}

	buf := make([]byte, 3)
	if _, err := file.Read(buf); err != nil {
		t.Fatalf("Failed to read from %s: %v", fpath, err)
	}

	index, err := file.Seek(2, os.SEEK_SET)
	if err != nil {
		t.Fatalf("Failed to seek: %v", err)
	} else if index != 2 {
		t.Errorf("The index should be %d but is %d", 2, index)
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatalf("Failed to read from %s: %v", fpath, err)
	}

	expected := testData[2:]
	if expected != string(data) {
		t.Errorf("Got the wrong result from read\nExpected: %s\nActual: %s",
			expected, data)
	}
}
