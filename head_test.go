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

func Test_Head(t *testing.T) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	head := store.wstore.head
	if head.wstore != store.wstore {
		t.Fail()
	}
	if head.root != store.wstore.fpos_firstblock {
		t.Fail()
	}
}
