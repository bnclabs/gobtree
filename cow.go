//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package btree

// Create a new copy of node by assigning a free file-position to it.
func (kn *knode) copyOnWrite(store *Store) Node {
	newkn := (&knode{}).newNode(store)
	newkn.ks = newkn.ks[:len(kn.ks)]
	copy(newkn.ks, kn.ks)
	newkn.ds = newkn.ds[:len(kn.ds)]
	copy(newkn.ds, kn.ds)
	newkn.vs = newkn.vs[:len(kn.vs)]
	copy(newkn.vs, kn.vs)
	newkn.size = len(kn.ks)
	return newkn
}

// Refer above.
func (in *inode) copyOnWrite(store *Store) Node {
	newin := (&inode{}).newNode(store)
	newin.ks = newin.ks[:len(in.ks)]
	copy(newin.ks, in.ks)
	newin.ds = newin.ds[:len(in.ds)]
	copy(newin.ds, in.ds)
	newin.vs = newin.vs[:len(in.vs)]
	copy(newin.vs, in.vs)
	newin.size = len(in.ks)
	return newin
}

// Create a new instance of `knode`, an in-memory representation of btree leaf
// block.
//   * `keys` slice must be half sized and zero valued, capacity of keys slice
//     must be 1 larger to accomodate overflow-detection.
//   * `values` slice must be half+1 sized and zero valued, capacity of value
//     slice must be 1 larger to accomodate overflow-detection.
func (kn *knode) newNode(store *Store) *knode {
	fpos := store.wstore.freelist.pop()

	max := store.maxKeys() // always even
	b := (&block{leaf: TRUE}).newBlock(max/2, max)
	newkn := &knode{block: *b, fpos: fpos, dirty: true}
	return newkn
}

// Refer to the notes above.
func (in *inode) newNode(store *Store) *inode {
	fpos := store.wstore.freelist.pop()

	max := store.maxKeys() // always even
	b := (&block{leaf: FALSE}).newBlock(max/2, max)
	kn := knode{block: *b, fpos: fpos, dirty: true}
	newin := &inode{knode: kn}
	return newin
}
