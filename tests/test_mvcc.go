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
	Idxfile: "./data/indexfile.dat",
	Kvfile:  "./data/kvfile.dat",
	IndexConfig: btree.IndexConfig{
		Sectorsize: 512,
		Flistsize:  4000 * btree.OFFSET_SIZE,
		Blocksize:  4 * 1024,
	},
	Maxlevel:      6,
	RebalanceThrs: 25,
	AppendRatio:   0.7,
	DrainRate:     400,
	MaxLeafCache:  1000,
	Sync:          false,
	Nocache:       false,
	Debug:         false,
}

func main() {
	//os.Remove(conf.Idxfile)
	//os.Remove(conf.Kvfile)
	if conf.Debug {
		fd, _ := os.Create("debug")
		log.SetOutput(fd)
	}

	bt := btree.NewBTree(btree.NewStore(conf))

	seed := time.Now().UnixNano()
	log.Println("Seed:", seed)

	count, items := 100, 10000
	chans := []chan []interface{}{
		make(chan []interface{}, 100), make(chan []interface{}, 100),
		make(chan []interface{}, 100), make(chan []interface{}, 100),
	}
	endchan := make(chan []interface{}, count)
	check := false
	go doinsert(bt, 0, chans[0], chans[1], chans[2], check)
	go dolookup(bt, chans[1], endchan, check)
	go dolookup(bt, chans[1], endchan, check)
	go dolookup(bt, chans[1], endchan, check)
	go dolookup(bt, chans[1], endchan, check)
	go doremove(bt, chans[2], chans[3], check)
	go dolookupNeg(bt, chans[3], endchan, check)
	// Prepopulate
	//for i := 0; i < count; i++ {
	//    keys, values := btree.TestData(items, seed+int64(i))
	//    for i := range keys {
	//        k, v := keys[i], values[i]
	//        k.Id = i
	//        bt.Insert(k, v)
	//    }
	//}
	//bt.Check()
	//log.Println("Prepulated", precount)
	//bt.Stats(true)
	bt.Check()
	go func() {
		for i := 0; i < count; i++ {
			keys, values := btree.TestData(items, seed+int64(i))
			chans[0] <- []interface{}{keys, values}
		}
	}()
	for i := 0; i < count; i++ {
		<-endchan
		<-endchan
		<-endchan
		<-endchan
		<-endchan
		log.Println("Done ... ", i)
		bt.Check()
	}
	bt.Drain()
	bt.Stats(true)
	log.Println()
	bt.Close()
}

func doinsert(
	bt *btree.BTree, count int, in chan []interface{}, out,
	outr chan []interface{}, check bool) {

	for {
		cmd := <-in
		keys, values := cmd[0].([]*btree.TestKey), cmd[1].([]*btree.TestValue)
		for i := range keys {
			k, v := keys[i], values[i]
			k.Id = int64(count)
			count++
			bt.Insert(k, v)
		}
		log.Println("Insert ok ...", count)
		if check {
			bt.Check()
		}

		// 33.33% remove
		rmkeys := make([]*btree.TestKey, 0, len(keys))
		rmvals := make([]*btree.TestValue, 0, len(values))
		lkeys := make([]*btree.TestKey, 0, len(keys))
		lvals := make([]*btree.TestValue, 0, len(values))
		for i := 0; i < len(keys); i++ {
			if i%3 == 0 {
				rmkeys = append(rmkeys, keys[i])
				rmvals = append(rmvals, values[i])
			} else {
				lkeys = append(lkeys, keys[i])
				lvals = append(lvals, values[i])
			}
		}
		for i := 0; i < 4; i++ {
			out <- []interface{}{lkeys, lvals}
		}
		outr <- []interface{}{rmkeys, rmvals}
	}
}

func dolookup(
	bt *btree.BTree, in chan []interface{}, out chan []interface{}, check bool) {

	keys, values := make([]*btree.TestKey, 0), make([]*btree.TestValue, 0)
	count := 0
	for {
		cmd := <-in
		keys = append(keys[:len(keys)/4], cmd[0].([]*btree.TestKey)...)
		values = append(values[:len(values)/4], cmd[1].([]*btree.TestValue)...)
		for i := range keys {
			k, v := keys[i], values[i]
			ch := bt.Lookup(k)
			count += 1
			found := false
			vals := make([]string, 0, 100)
			val := <-ch
			for val != nil {
				vals = append(vals, string(val))
				if string(val) == v.V {
					found = true
				}
				val = <-ch
			}
			if found == false {
				log.Printf("could not find for %v, expected %v: %v", k, v.V, vals)
			}
		}
		if check {
			bt.Check()
		}
		out <- []interface{}{keys, values}
	}
}

func doremove(
	bt *btree.BTree, in chan []interface{}, out chan []interface{}, check bool) {

	for {
		cmd := <-in
		rmkeys, rmvals := cmd[0].([]*btree.TestKey), cmd[1].([]*btree.TestValue)
		for i := 0; i < len(rmkeys); i++ {
			k, _ := rmkeys[i], rmvals[i]
			bt.Remove(k)
		}
		if check {
			bt.Check()
		}
		out <- []interface{}{rmkeys, rmvals}
	}
}

func dolookupNeg(
	bt *btree.BTree, in chan []interface{}, out chan []interface{}, check bool) {

	count := 0
	for {
		cmd := <-in
		keys, values := cmd[0].([]*btree.TestKey), cmd[1].([]*btree.TestValue)
		for i := range keys {
			k := keys[i]
			ch := bt.Lookup(k)
			count += 1
			vals := make([][]byte, 0, 100)
			val := <-ch
			for val != nil {
				vals = append(vals, val)
				val = <-ch
			}
		}
		if check {
			bt.Check()
		}
		out <- []interface{}{keys, values}
	}
}
