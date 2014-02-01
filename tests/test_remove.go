//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"github.com/prataprc/btree"
	"log"
	"os"
	"time"
)

var conf = btree.Config{
	Idxfile: "./data/test_rm_index.dat",
	Kvfile:  "./data/test_rm_kv.dat",
	IndexConfig: btree.IndexConfig{
		Sectorsize: 512,
		Flistsize:  1000 * btree.OFFSET_SIZE,
		Blocksize:  512,
	},
	Maxlevel:      6,
	RebalanceThrs: 3,
	AppendRatio:   0.7,
	DrainRate:     200,
	MaxLeafCache:  1000,
	Sync:          false,
	Nocache:       false,
}

func main() {
	//os.Remove(conf.idxfile)
	//os.Remove(conf.kvfile)
	if conf.Debug {
		fd, _ := os.Create("debug")
		log.SetOutput(fd)
	}

	bt := btree.NewBTree(btree.NewStore(conf))

	factor, count := 100, 10000
	rmcount := 0
	seed := time.Now().UnixNano()
	log.Println("Seed:", seed)
	for i := 0; i < 10; i++ {
		rmcount += doinsert(seed+int64(i), factor, count, bt)
		bt.Drain()
		if ((i+1)*factor*count)-rmcount != int(bt.Count()) {
			log.Panicln("mismatch in count",
				((i+1)*factor*count)-rmcount, bt.Count())
		}
		bt.Stats(true)
		log.Println()
	}
	log.Println("count", bt.Count())
	bt.Close()
}

func doinsert(seed int64, factor, count int, bt *btree.BTree) int {
	rmcount := 0
	for i := 0; i < factor; i++ {
		keys, values := btree.TestData(count, seed)
		for j := 0; j < count; j++ {
			k, v := keys[j], values[j]
			k.Id = int64((i * count) + j)
			bt.Insert(k, v)
		}
		log.Println("Done ", time.Now().UnixNano()/1000000, (i+1)*count)
		rmcount += doremove(keys, values, bt)
	}
	return rmcount
}

func doremove(keys []*btree.TestKey, values []*btree.TestValue, bt *btree.BTree) int {
	rmcount := 0
	count := len(keys)
	for j := 0; j < count; j += 3 {
		k := keys[j]
		bt.Remove(k)
		rmcount += 1
	}
	for j := 1; j < count; j += 3 {
		k := keys[j]
		bt.Remove(k)
		rmcount += 1
	}
	return rmcount
}
