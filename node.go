//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// Handles all btree traversals except, insert() and remove()
package btree

import (
	"bytes"
	"fmt"
	"log"
)

type Emitter func([]byte) // Internal type

// in-memory structure for leaf-block.
type knode struct { // keynode
	block       // embedded structure
	fpos  int64 // file-offset where this block resides
	dirty bool  // Dirty or not
}

// in-memory structure for intermediate block.
type inode struct { // intermediate node
	knode
}

// Node interface that is implemented by both `knode` and `inode` structure.
type Node interface {
	// inserts the {key,docid,valud} typle into index tree, splitting the
	// nodes as necessary.
	//
	// returns,
	//  - node, newly spawned node, if the node was split into two.
	//  - kfpos, median key-position
	//  - dfpos, median docid-postion
	insert(*Store, Key, Value, *MV) (Node, int64, int64)

	// return number of entries on all the leaf nodes under this Node.
	count(*Store) int64

	// return {key,docid,value} tuple for the lowest key in the tree.
	front(*Store) ([]byte, []byte, []byte)

	// return true iff this tree contains the `key`.
	contains(*Store, Key) bool

	// return true iff this tree contains the `key` with specified `docid`
	equals(*Store, Key) bool

	// passes all of the data in this node and its children through the channel
	// in sort order.
	traverse(*Store, func(int64, int64, int64))

	// lookup index for key
	lookup(*Store, Key, Emitter) bool

	// removes the value from the tree, rebalancing as necessary. Returns true
	// iff an element was actually deleted. Return,
	//  - Node
	//  - whether to rebalance or not.
	//  - slice of stalenodes
	remove(*Store, Key, *MV) (Node, bool, int64, int64)

	//---- Support methods.
	isLeaf() bool     // Return whether node is a leaf node or not.
	getKnode() *knode // Return the underlying `knode` structure.
	getBlock() *block // Return the underlying `block` structure.
	copyOnWrite(*Store) Node

	// count to rotate
	balance(*Store, Node) int
	// merge receiver to Node
	mergeRight(*Store, Node, int64, int64) (Node, []int64)
	// merge Node to receiver
	mergeLeft(*Store, Node, int64, int64) (Node, []int64)
	// rotate entries from Node to receiver.
	rotateLeft(*Store, Node, int, int64, int64) (int64, int64)
	// rotate entries from receiver to Node.
	rotateRight(*Store, Node, int, int64, int64) (int64, int64)

	//---- Development methods.
	// Return list of offsets from sub-tree.
	listOffsets(*Store) []int64
	// Recursively render this block and its child block.
	show(*Store, int)
	// Check nodes for debugging
	check(*Store, *CheckContext)
	// Recursively check separator keys
	checkSeparator(*Store, []int64) []int64
	// Render keys at each level
	showKeys(*Store, int)
	// Count cummulative entries at each level
	levelCount(*Store, int, []int64, int64, int64) ([]int64, int64, int64)
}

// get `block` structure embedded in knode, TODO: This must go into Node
// interface !.
func (kn *knode) getBlock() *block {
	return &kn.block
}

// get `block` structure embedded in inode's knode.
func (in *inode) getBlock() *block {
	return &in.knode.block
}

// get `knode` structure, TODO: This must go into Node interface !
func (kn *knode) getKnode() *knode {
	return kn
}

// get `block` structure embedded in inode.
func (in *inode) getKnode() *knode {
	return &in.knode
}

// Return the list of key offsets from kv-file
func (kn *knode) listOffsets(store *Store) []int64 {
	return []int64{kn.fpos}
}

// Return the list of key offsets from kv-file
func (in *inode) listOffsets(store *Store) []int64 {
	ls := make([]int64, 0)
	for _, fpos := range in.vs {
		ls = append(ls, store.FetchNCache(fpos).listOffsets(store)...)
	}
	return append(ls, in.fpos)
}

// Returns,
//  - index of the smallest value that is not less than `key`
//  - whether or not it equals `key`
//  - whether or not it equals `docid`
// If there are no elements greater than or equal to `key` then it returns
// (len(node.key), false)
func (kn *knode) searchGE(store *Store, key Key, chkdocid bool) (int, int64, int64) {
	var kfpos, dfpos int64
	var cmp, pos int
	ks, ds := kn.ks, kn.ds
	if kn.size == 0 {
		return 0, -1, -1
	}

	low, high := 0, kn.size
	for (high - low) > 1 {
		mid := (high + low) / 2
		cmp, kfpos, dfpos = key.CompareLess(store, ks[mid], ds[mid], chkdocid)
		if cmp < 0 {
			high = mid
		} else {
			low = mid
		}
	}

	cmp, kfpos, dfpos = key.CompareLess(store, ks[low], ds[low], chkdocid)
	if cmp <= 0 {
		pos = low
	} else {
		pos = high
		// FIXME : Can the following CompareLess be optimized away ?
		if kfpos < 0 && high < kn.size {
			_, kfpos, dfpos = key.CompareLess(store, ks[high], ds[high], chkdocid)
		}
	}
	return pos, kfpos, dfpos
}

func (kn *knode) searchEqual(store *Store, key Key) (int, bool) {
	var cmp int
	ks, ds := kn.ks, kn.ds
	if kn.size == 0 {
		return 0, false
	}

	low, high := 0, kn.size
	for (high - low) > 1 {
		mid := (high + low) / 2
		cmp, _, _ = key.CompareLess(store, ks[mid], ds[mid], true)
		if cmp < 0 {
			high = mid
		} else {
			low = mid
		}
	}

	cmp, _, _ = key.CompareLess(store, ks[low], ds[low], true)
	if cmp == 0 {
		return low, true
	}
	return high, false
}

func (in *inode) searchEqual(store *Store, key Key) (int, bool) {
	var cmp int
	ks, ds := in.ks, in.ds
	if in.size == 0 {
		return 0, false
	}

	low, high := 0, in.size
	for (high - low) > 1 {
		mid := (high + low) / 2
		cmp, _, _ = key.CompareLess(store, ks[mid], ds[mid], true)
		if cmp < 0 {
			high = mid
		} else {
			low = mid
		}
	}

	cmp, _, _ = key.CompareLess(store, ks[low], ds[low], true)
	if cmp < 0 {
		return low, false
	} else if cmp == 0 {
		return high, true
	}
	return high, false
}

//---- count
func (kn *knode) count(store *Store) int64 {
	return int64(kn.size)
}

func (in *inode) count(store *Store) int64 {
	n := int64(0)
	for _, v := range in.vs {
		n += store.FetchNCache(v).count(store)
	}
	return n
}

//---- front
func (kn *knode) front(store *Store) ([]byte, []byte, []byte) {
	if kn.size == 0 {
		return nil, nil, nil
	} else {
		return store.fetchValue(kn.ks[0]),
			store.fetchValue(kn.ds[0]),
			store.fetchValue(kn.vs[0])
	}
}

func (in *inode) front(store *Store) ([]byte, []byte, []byte) {
	return store.FetchNCache(in.vs[0]).front(store)
}

//---- contains
func (kn *knode) contains(store *Store, key Key) bool {
	_, kfpos, _ := kn.searchGE(store, key, false)
	return kfpos >= 0
}

func (in *inode) contains(store *Store, key Key) bool {
	idx, kfpos, _ := in.searchGE(store, key, false)
	if kfpos >= 0 {
		return true
	}
	return store.FetchNCache(in.vs[idx]).contains(store, key)
}

//---- equals
func (kn *knode) equals(store *Store, key Key) bool {
	_, kfpos, dfpos := kn.searchGE(store, key, true)
	return (kfpos >= 0) && (dfpos >= 0)
}

func (in *inode) equals(store *Store, key Key) bool {
	idx, kfpos, dfpos := in.searchGE(store, key, true)
	if (kfpos >= 0) && (dfpos >= 0) {
		return true
	}
	return store.FetchNCache(in.vs[idx]).equals(store, key)
}

//-- traverse
func (kn *knode) traverse(store *Store, fun func(int64, int64, int64)) {
	for i := range kn.ks {
		fun(kn.ks[i], kn.ds[i], kn.vs[i])
	}
}

func (in *inode) traverse(store *Store, fun func(int64, int64, int64)) {
	for _, v := range in.vs {
		store.FetchNCache(v).traverse(store, fun)
	}
}

//---- lookup, we expect that key's docid should be set to proper value or
// minimum value if not material to lookup.
func (kn *knode) lookup(store *Store, key Key, emit Emitter) bool {
	index, _, _ := kn.searchGE(store, key, true)
	for i := index; i < kn.size; i++ {
		keyb := store.fetchKey(kn.ks[i])
		if keyeq, _ := key.Equal(keyb, nil); keyeq {
			emit(store.fetchValue(kn.vs[i]))
		} else {
			return false
		}
	}
	return true
}

func (in *inode) lookup(store *Store, key Key, emit Emitter) bool {
	index, kpos, dpos := in.searchGE(store, key, true)
	if kpos >= 0 && dpos >= 0 {
		index += 1
	}
	for i := index; i < in.size+1; i++ {
		if store.FetchNCache(in.vs[i]).lookup(store, key, emit) {
			if i < in.size {
				keyb := store.fetchKey(in.ks[i])
				if keyeq, _ := key.Equal(keyb, nil); keyeq == false {
					return false
				}
			}
		} else {
			return false
		}
	}
	return true
}

// Convinience method
func (kn *knode) show(store *Store, level int) {
	prefix := ""
	for i := 0; i < level; i++ {
		prefix += "  "
	}
	fmt.Printf(
		"%vleaf:%v size:%v fill: %v/%v, %v/%v, at fpos %v\n",
		prefix, kn.leaf, kn.size, len(kn.ks), cap(kn.ks), len(kn.vs),
		cap(kn.vs), kn.fpos,
	)
	for i := range kn.ks {
		fmt.Printf(
			"%v%v key:%v docid:%v\n",
			prefix+"  ", i,
			string(store.fetchKey(kn.ks[i])),
			string(store.fetchKey(kn.ds[i])),
		)
	}
	fmt.Printf("%vkeys: %v\n", prefix+"  ", kn.ks)
	fmt.Printf("%vvalues: %v\n", prefix+"  ", kn.vs)
}

func (in *inode) show(store *Store, level int) {
	prefix := ""
	for i := 0; i < level; i++ {
		prefix += "  "
	}
	(&in.knode).show(store, level)
	store.FetchNCache(in.vs[0]).show(store, level+1)
	for i := range in.ks {
		fmt.Printf("%v%vth key %v & %v\n", prefix, i, in.ks[i], in.ds[i])
		store.FetchNCache(in.vs[i+1]).show(store, level+1)
	}
}

func (kn *knode) showKeys(store *Store, level int) {
	prefix := ""
	for i := 0; i < level; i++ {
		prefix += "  "
	}
	for i := range kn.ks {
		keyb := store.fetchKey(kn.ks[i])
		docb := store.fetchKey(kn.ds[i])
		fmt.Println(prefix, string(keyb), " ; ", string(docb))
	}
}

func (in *inode) showKeys(store *Store, level int) {
	prefix := ""
	for i := 0; i < level; i++ {
		prefix += "  "
	}
	for i := range in.ks {
		store.FetchNCache(in.vs[i]).showKeys(store, level+1)
		keyb := store.fetchKey(in.ks[i])
		docb := store.fetchKey(in.ds[i])
		fmt.Println(prefix, "*", string(keyb), " ; ", string(docb))
	}
	store.FetchNCache(in.vs[in.size]).showKeys(store, level+1)
}

type CheckContext struct {
	nodepath []int64
}

func (kn *knode) check(store *Store, c *CheckContext) {
	c.nodepath = append(c.nodepath, kn.fpos)
	kn.checkKeys(store, c)
	if kn.vs[kn.size] != 0 {
		log.Panicln("Check: last entry is not zero")
	}
	c.nodepath = c.nodepath[:len(c.nodepath)-1]
}

func (in *inode) check(store *Store, c *CheckContext) {
	c.nodepath = append(c.nodepath, in.fpos)
	in.getKnode().checkKeys(store, c)
	for _, v := range in.vs {
		if v == 0 {
			log.Panicln("Check: value fpos in intermediate node cannot be zero")
		}
		for _, offset := range store.wstore.freelist.offsets {
			if v == offset {
				log.Panicln("Check: child node is also in freelist", offset)
			}
		}
		store.FetchNCache(v).check(store, c)
	}
	c.nodepath = c.nodepath[:len(c.nodepath)-1]
}

func (kn *knode) checkKeys(store *Store, c *CheckContext) {
	if len(kn.ks) != kn.size {
		log.Panicln("Check: number of keys does not match size")
	} else if len(kn.ds) != kn.size {
		log.Panicln("Check: number of docids does not match size")
	} else if len(kn.vs) != (kn.size + 1) {
		log.Panicln("Check: number of values does not match size")
	}
	// Detect circular loop, only for intermediate nodes. kn.vs for leaf nodes
	// with point into kvfile.
	if !kn.isLeaf() {
		for i := 0; i < len(kn.vs); i++ {
			for _, nfpos := range c.nodepath {
				if nfpos == kn.vs[i] {
					log.Panicln(
						"Circular loop", kn.fpos, c.nodepath, nfpos, kn.vs[i])
				}
			}
		}
	}

	for i := 0; i < kn.size-1; i++ {
		if kn.ks[i] < 0 || kn.ds[i] < 0 {
			log.Panicln("Check: File position less than zero")
		}
		x := store.fetchKey(kn.ks[i])
		y := store.fetchKey(kn.ks[i+1])
		cmp := bytes.Compare(x, y)
		if cmp > 0 {
			log.Panicln("Check: No sort order for key", string(x), string(y))
		}
		if cmp == 0 {
			x = store.fetchDocid(kn.ds[i])
			y = store.fetchDocid(kn.ds[i+1])
			if bytes.Compare(x, y) > 0 {
				log.Panicln("Check: No sort order for docid")
			}
		}
	}
}

func (kn *knode) checkSeparator(store *Store, keys []int64) []int64 {
	if kn.size > 0 {
		keys = append(keys, kn.ks[0])
	}
	return keys
}

func (in *inode) checkSeparator(store *Store, keys []int64) []int64 {
	inkeys := make([]int64, 0, store.maxKeys())
	for _, v := range in.vs {
		inkeys = store.FetchNCache(v).checkSeparator(store, inkeys)
	}
	for i := range in.ks {
		if in.ks[i] != inkeys[i+1] {
			log.Panicln("Mismatch in separator keys")
		}
	}
	keys = append(keys, inkeys[0])
	return keys
}

func (kn *knode) levelCount(store *Store, level int, acc []int64, ic, kc int64) ([]int64, int64, int64) {
	if len(acc) == level {
		acc = append(acc, int64(kn.size))
	} else {
		acc[level] += int64(kn.size)
	}
	return acc, ic, (kc + 1)
}

func (in *inode) levelCount(store *Store, level int, acc []int64, ic, kc int64) ([]int64, int64, int64) {
	if len(acc) == level {
		acc = append(acc, int64(in.size))
	} else {
		acc[level] += int64(in.size)
	}
	for _, v := range in.vs {
		acc, ic, kc =
			store.FetchNCache(v).levelCount(store, level+1, acc, ic, kc)
	}
	return acc, (ic + 1), kc
}
