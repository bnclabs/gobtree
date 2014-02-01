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
	"testing"
)

func Benchmark_gobenc(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	max := store.maxKeys()
	kn := (&knode{}).newNode(store)
	kn.ks = kn.ks[:0]
	kn.vs = kn.vs[:0]
	for i := 0; i < max; i++ {
		kn.ks = append(kn.ks, int64(i))
		kn.ds = append(kn.ds, int64(i))
		kn.vs = append(kn.vs, int64(i))
	}
	kn.vs = append(kn.vs, 0)
	kn.size = max
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kn.gobEncode()
	}
}

func Benchmark_gobdec(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	max := store.maxKeys()
	kn := (&knode{}).newNode(store)
	kn.ks = kn.ks[:0]
	kn.vs = kn.vs[:0]
	for i := 0; i < max; i++ {
		kn.ks = append(kn.ks, int64(i))
		kn.ds = append(kn.ds, int64(i))
		kn.vs = append(kn.vs, int64(i))
	}
	kn.vs = append(kn.vs, 0)
	kn.size = max
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytebuf := kn.gobEncode()
		kn.gobDecode(bytebuf)
	}
}
