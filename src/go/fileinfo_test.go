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
	"os"
	"path"
	"testing"
	"time"
)

func statTestFile(t *testing.T, name string, data []byte, mode os.FileMode) *FileInfo {
	qfs := initQfs(t)
	qfs.UmaskSet(0)
	fpath := path.Join(testDir, name)

	file, err := qfs.OpenFile(fpath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, mode)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	_, err = file.Write(data)
	if err != nil {
		t.Fatalf("Failed to write to %v: %v", file, err)
	}
	file.Close()

	fi, err := qfs.Stat(fpath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	err = qfs.Remove(fpath)
	if err != nil {
		t.Fatalf("Failed to remove file: %v", err)
	}

	return fi
}

func TestFileInfo_Name(t *testing.T) {
	name := "test_file_name"
	fi := statTestFile(t, name, []byte("test data"), 0666)

	if fi.Name() != name {
		t.Errorf("The name should've been '%s', not '%s'", name, fi.Name())
	}
}

func TestFileInfo_Size(t *testing.T) {
	data := []byte("some general test data")
	fi := statTestFile(t, "test_name", data, 0666)

	if fi.Size() != int64(len(data)) {
		t.Errorf("Expected size to be %d, not %d", len(data), fi.Size())
	}
}

func TestFileInfo_Mode(t *testing.T) {
	modes := []os.FileMode{0666, 0777, 0706, 0744}

	for _, mode := range modes {
		fi := statTestFile(t, "test_name", []byte("test data"), mode)
		if fi.Mode() != mode {
			t.Errorf("Expected mode to be %b, not %b", mode, fi.Mode())
		}
	}
}

func TestFileInfo_ModTime(t *testing.T) {
	fi := statTestFile(t, "test_name", []byte("data"), 0666)
	actualDuration := time.Since(fi.ModTime())

	// The time difference should be less than 30s
	expectedDuration, _ := time.ParseDuration("30s")
	if expectedDuration.Nanoseconds() < actualDuration.Nanoseconds() {
		t.Errorf("Time difference is %v, greater than %v", actualDuration,
			expectedDuration.Nanoseconds())
	}
}
