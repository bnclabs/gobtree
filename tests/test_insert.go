package main

import (
	"github.com/couchbaselabs/indexing/btree"
	"log"
	"os"
	"time"
)

var conf = btree.Config{
	Idxfile: "./data/test_insert_index.dat",
	Kvfile:  "./data/test_insert_kv.dat",
	IndexConfig: btree.IndexConfig{
		Sectorsize: 512,
		Flistsize:  1000 * btree.OFFSET_SIZE,
		Blocksize:  4 * 1024,
	},
	Maxlevel:      6,
	RebalanceThrs: 25,
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

	seed := time.Now().UnixNano()
	log.Println("Seed:", seed)
	doinsert(seed, 2000, 10000, bt, false)
	bt.Drain()
	bt.Stats(true)
	log.Println()
	bt.Close()
}

func doinsert(seed int64, factor, count int, bt *btree.BTree, check bool) {
	keys, values := btree.TestData(count, seed)
	for i := 0; i < factor; i++ {
		for j := 0; j < count; j++ {
			k, v := keys[j], values[j]
			k.Id = int64((i * count) + j)
			bt.Insert(k, v)
			if check {
				bt.Drain()
				bt.Check()
			}
		}
		log.Println("Done ", time.Now().UnixNano()/1000000, (i+1)*count)
	}
}
