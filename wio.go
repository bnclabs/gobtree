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
	"log"
	//"sync/atomic"
)

type IO struct {
	mvQ     []*MV
	commitQ map[int64]Node
}

func mvRoot(store *Store) int64 {
	wstore := store.wstore
	if len(wstore.mvQ) > 0 {
		return wstore.mvQ[len(wstore.mvQ)-1].root
	}
	return 0
}

func (wstore *WStore) ccacheLookup(fpos int64) Node {
	node := wstore.commitQ[fpos]
	if node != nil {
		wstore.commitHits += 1
	}
	return node
}

func (wstore *WStore) commit(mv *MV, minAccess int64, force bool) {
	if mv != nil {
		for fpos, node := range mv.commits {
			wstore.commitQ[fpos] = node
		}
		wstore.postMV(mv)
		wstore.mvQ = append(wstore.mvQ, mv)
	}
	if force || len(wstore.mvQ) > wstore.DrainRate {
		wstore.syncSnapshot(minAccess, force)
	}
	if force == false && len(wstore.freelist.offsets) < (wstore.Maxlevel*2) {
		offsets := wstore.appendBlocks(0, wstore.appendCount())
		wstore.freelist.add(offsets)
	}
}

func (wstore *WStore) delCommits(mvQ []*MV, fpos int64) {
	var spotmv *MV = nil
	for _, mv := range mvQ {
		if mv.commits[fpos] != nil {
			spotmv = mv
			break
		}
	}
	if spotmv == nil {
		log.Panicln("stale node is expected in previous snapshot", fpos)
	} else {
		delete(spotmv.commits, fpos)
	}
}

func (wstore *WStore) flushSnapshot(
	commitQ []Node, offsets []int64, mvroot, mvts int64, force bool) {

	// Sync kv file
	wstore.kvWfd.Sync()
	for _, node := range commitQ { // flush nodes first
		//if force || node.isLeaf() {
		wstore.flushNode(node)
		//}
	}
	// if force also flush the intermediate ping cache and pong cache
	//nc := (*map[int64]Node)(atomic.LoadPointer(&wstore.ncping))
	//for _, node := range *nc { // flush nodes first
	//    wstore.flushNode(node)
	//}
	//nc = (*map[int64]Node)(atomic.LoadPointer(&wstore.ncpong))
	//for _, node := range *nc { // flush nodes first
	//    wstore.flushNode(node)
	//}

	// Cloned freelist
	freelist := wstore.freelist.clone()
	freelist.add(offsets)
	crc := freelist.flush() // then this
	// Cloned head
	head := wstore.head.clone()
	head.setRoot(mvroot, mvts)
	head.flush(crc) // finally this
	wstore.idxWfd.Sync()
	if wstore.Debug {
		log.Println("snapshot", mvroot, mvts, commitQ, offsets)
	}
}
