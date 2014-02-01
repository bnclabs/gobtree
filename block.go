//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// translates btree blocks from persistant storage to in-memory data
// structure, called btree-node. A btree node can be a knode (also called leaf
// node) or it can be a inode. `block` structure is fundamental to both type
// of nodes.
package btree

import (
	"bytes"
	"encoding/gob"
)

const (
	// FIXME : Is there a better way to learn sizeof a struct.
	BLK_KEY_SIZE   = 20 // bytes
	BLK_VALUE_SIZE = 8  // bytes
	BLK_OVERHEAD   = 16 // bytes, leaf+size field
	TRUE           = 1
	FALSE          = 0
)

// block structure. every field in this structure has a corresponding field
// persisted as btree-block.
type block struct {
	leaf byte    // Is this leaf block ? 0-false, 1-true
	size int     // number of `keys` in this node. `values` will be `size+1`
	ks   []int64 // slice of key position in appendkv file.
	ds   []int64 // slice of docid position in appendkv file.
	vs   []int64 // slice of `size+1`.
}

// check whether `block` is a leaf block, which means `Node` is a `knode`
// structure.
func (b *block) isLeaf() bool {
	return b.leaf == TRUE
}

// create a new inmemory block with `max` capacity and `length` length.
func (b *block) newBlock(length int, max int) *block {
	b.size = 0
	b.ks = make([]int64, length, max+1)
	b.ds = make([]int64, length, max+1)
	b.vs = make([]int64, length+1, max+2)
	return b
}

// Encode in-memory node into binary representation to store them on disk.
func (b *block) gobEncode() []byte {
	buf := new(bytes.Buffer)
	genc := gob.NewEncoder(buf)
	genc.Encode(b.leaf)
	genc.Encode(b.size)
	genc.Encode(b.ks)
	genc.Encode(b.ds)
	genc.Encode(b.vs)
	return buf.Bytes()
}

// Decode in-memory node into binary representation to store them on disk.
func (b *block) gobDecode(bs []byte) {
	gdec := gob.NewDecoder(bytes.NewBuffer(bs))
	gdec.Decode(&b.leaf)
	gdec.Decode(&b.size)
	gdec.Decode(&b.ks)
	gdec.Decode(&b.ds)
	gdec.Decode(&b.vs)
}
