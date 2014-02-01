//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package btree

import (
	"bytes"
	"math/rand"
	"testing"
)

var datafile = "./data/appendkv_datafile.dat"

func Test_KV(t *testing.T) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	fpos := store.appendKey([]byte("Hello world"))
	if bytes.Equal(store.fetchKey(fpos), []byte("Hello world")) == false {
		t.Fail()
	}
}

var fposs = make([]int64, 0)
var maxEntries = 100000

func Benchmark_appendKV(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	data := "abcdefghijklmnopqrstuvwxyz " + "abcdefghijklmnopqrstuvwxyz "
	data += data
	data += data
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.appendValue([]byte(data))
	}
}

func Benchmark_appendFetchSetup(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Close()
	}()

	data := "abcdefghijklmnopqrstuvwxyz " + "abcdefghijklmnopqrstuvwxyz "
	data += data
	data += data
	for i := 0; i < maxEntries; i++ {
		fposs = append(fposs, store.appendKey([]byte(data)))
	}
	b.SkipNow()
}

func Benchmark_appendFetchKV(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Close()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.fetchKey(fposs[rand.Intn(maxEntries)])
	}
}

func Benchmark_appendFetchFinish(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()
	b.SkipNow()
}
