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

func Test_maxFreeBlock(t *testing.T) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	wstore := store.wstore
	if (wstore.maxFreeBlocks() * 8) != int(store.Flistsize) {
		t.Fail()
	}
}

func Test_fetchFreelist(t *testing.T) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	wstore := store.wstore
	freelist := wstore.freelist
	firstfpos := wstore.fpos_firstblock
	if (freelist.offsets[0] != firstfpos+int64(wstore.Blocksize)) ||
		(freelist.offsets[1] != firstfpos+(2*int64(wstore.Blocksize))) {
		t.Fail()
	}
}
