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
	"flag"
	"fmt"
	"github.com/prataprc/gobtree"
	"os"
	"time"
)

var _ = fmt.Sprintln("keep 'fmt' import during debugging", time.Now(), os.O_WRONLY)

func main() {
	flag.Parse()
	args := flag.Args()
	idxfile, kvfile := args[0], args[1]
	os.Remove(idxfile)
	os.Remove(kvfile)

	var conf = btree.Config{
		Idxfile: idxfile,
		Kvfile:  kvfile,
		IndexConfig: btree.IndexConfig{
			Sectorsize: 512,
			Flistsize:  2000 * btree.OFFSET_SIZE,
			Blocksize:  512,
		},
		Maxlevel:      6,
		RebalanceThrs: 5,
		AppendRatio:   0.7,
		DrainRate:     600,
		MaxLeafCache:  1000,
		Sync:          false,
		Nocache:       false,
	}
	store := btree.NewStore(conf)
	bt := btree.NewBTree(store)
	factor := 1
	count := 10000
	seed := time.Now().UnixNano()
	fmt.Println("Seed:", seed)
	keys, values := btree.TestData(count, seed)
	fmt.Println(time.Now())
	for i := 0; i < factor; i++ {
		for j := 0; j < count; j++ {
			k, v := keys[j], values[j]
			k.Id = int64((i * count) + j)
			bt.Insert(k, v)
		}
		fmt.Println("Done ", time.Now().UnixNano()/1000000, (i+1)*count)
	}
	bt.Drain()
	fmt.Println(time.Now())
	// Sanity check
	if bt.Count() != int64(count*factor) {
		fmt.Println(bt.Count(), int64(count*factor))
		panic("Count mismatch")
	}
	// Remove
	checkcount := int64(count * factor)
	for i := 0; i < factor; i++ {
		for j := 0; j < count; j += 3 {
			k := keys[j]
			k.Id = int64((i * count) + j)
			bt.Remove(k)
			bt.Drain()
			bt.Check()
			checkcount -= 1
			if bt.Count() != checkcount {
				fmt.Println("remove mismatch count", bt.Count(), checkcount)
				panic("")
			}
		}
		for j := 1; j < count; j += 3 {
			k := keys[j]
			k.Id = int64((i * count) + j)
			bt.Remove(k)
			bt.Drain()
			bt.Check()
			checkcount -= 1
			if bt.Count() != checkcount {
				fmt.Println("remove mismatch count", bt.Count(), checkcount)
				panic("")
			}
		}
		for j := 2; j < count; j += 3 {
			k := keys[j]
			k.Id = int64((i * count) + j)
			bt.Remove(k)
			bt.Drain()
			bt.Check()
			checkcount -= 1
			if bt.Count() != checkcount {
				fmt.Println("remove mismatch count", bt.Count(), checkcount)
				panic("")
			}
		}
		fmt.Println("Done ", time.Now().UnixNano()/1000000, (i+1)*count)
	}
	bt.Drain()
	bt.Stats(false)
	fmt.Println("Count", bt.Count())
	bt.Close()
}
