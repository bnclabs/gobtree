// Btree indexing algorithm for json {key,docid,value} triplets. `keys` and
// `values` are expected to be in json, while `docid` is the primary key of
// json document which contains the key fragment. `value` can optionally be
// used to store fragment of a document.

// since keys generated for seconday indexes may not be unique, indexing
// a.k.a sorting is done on {key,docid}.

package btree

import (
	"fmt"
	"log"
	"time"
)

// Sub-structure to `Config` structure.
type IndexConfig struct {
	Sectorsize int64 // head sector-size in bytes.
	Flistsize  int64 // free-list size in bytes.
	Blocksize  int64 // btree block size in bytes.
}

// BTree configuration parameters, these parameters cannot change once the
// index-file and kv-file are created, for intance, when indexing server
// restarts on existing index files.
type Config struct {
	//-- file store
	Idxfile string
	Kvfile  string
	IndexConfig

	// maximum number of levels btree can grow, this information is used as a
	// cue in calculating couple of limits within the algorithm.
	Maxlevel int

	// if number of entries within a node goes below this threshold, then a
	// rebalance will be triggered on its parent node.
	RebalanceThrs int

	// when free nodes are not available to record btree mutations, then a new
	// set of btree blocks will be appended to the index file.
	//  count of appended blocks = freelist-size * AppendRatio
	AppendRatio float32

	// MVCC snapshots are flushed in batches. DrainRate defines the maximum
	// number of snapshots to accumulate in-memory, after which they are
	// flushed to disk.
	DrainRate int

	// all intermediate nodes are cached in memory, there are no upper limit
	// to that. But number of leaf nodes can be really large and
	// `MaxLeafCache` limits the number of leaf nodes to be cached.
	MaxLeafCache int

	// MVCC throttle rate in milliseconds
	MVCCThrottleRate time.Duration

	// enables O_SYNC flag for indexfile and kvfile.
	Sync bool

	// enables O_DIRECT flag for indexfile and kvfile.
	Nocache bool

	// Debug
	Debug bool
}

// btree instance. Typical usage, where `conf` is Config structure.
//          bt = btree.NewBTree( btree.NewStore( conf ))
// any number of BTree instances can be created.
type BTree struct {
	Config
	store *Store
}

// interface made available to btree user.
type Indexer interface {
	// Insert {key,value} pairs into the index. key type is expected to
	// implement `Key` interface and value type is expected to implement
	// `Value` interface. If the key is successfuly inserted it returns true.
	Insert(Key, Value) bool

	// Count number of key,value pairs in this index.
	Count() int64

	// Return key-bytes, docid-bytes, and value bytes of the first
	// element in the list.
	Front() ([]byte, []byte, []byte)

	// Check whether `key` is present in the index.
	Contains(Key) bool

	// Check whether `key` and `docid` is present in the index.
	Equals(Key) bool

	// Return a channel on which the caller can receive key bytes, docid-
	// bytes and value-bytes for each entry in the index.
	//      ch := bt.FullSet()
	//      keybytes := <-ch
	//      valbytes := <-ch
	//      docidbytes := <-ch
	FullSet() <-chan []byte

	// Return a channel on which the caller can receive key-bytes.
	KeySet() <-chan []byte

	// Return a channel on which the caller can receive docid-bytes
	DocidSet() <-chan []byte

	// Return a channel on which the caller can receive value-bytes
	ValueSet() <-chan []byte

	// Return a channel that will transmit all values associated with `key`,
	// make sure the `docid` is set to minimum value to lookup all values
	// greater that `key` && `docid`
	Lookup(Key) (chan []byte, error)

	// FIXME: Define Range() API.
	//Range(Key, Key) (chan []byte, error)

	// Remove an entry identified by {key,docid}
	Remove(Key) bool

	//-- Meant for debugging.
	Drain()      // flush the MVCC snapshots into disk.
	Check()      // check the btree data structure for anamolies.
	Show()       // displays in-memory btree structure on stdout.
	ShowKeys()   // list keys and docids inside the tree.
	Stats(bool)  // display statistics so far.
	LevelCount() // count number of inodes, knodes and number of entries.
}

// interfaces to be supported by key,value types.
type Key interface {
	// transform actual key content into byte slice, that can be persisted in
	// file.
	Bytes() []byte

	// every key carries the document-id that emitted this {key,value} tupele,
	// transform the document-id into byte slice, that can be persisted in file.
	Docid() []byte

	// this is the call-back hook that `Key` types can use to sort themself.
	// kfpos : file-position inside kv-file that contains key-content.
	// dfpos : file-position inside kv-file that contains docid-content.
	// isD : boolean that says whether comparision needs to be done on
	//       document-id as well
	//
	// Example:
	//
	//      otherkey = s.fetchKey(kfpos)
	//      if cmp = bytes.Compare(thiskey, otherkey); cmp == 0 && isD {
	//          otherdocid = s.fetchKey(dfpos)
	//          cmp = bytes.Compare(thisdocid, otherdocid)
	//          if cmp == 0 {
	//              return cmp, kfpos, dfpos
	//          } else {
	//              return cmp, kfpos, -1
	//          }
	//      } else if cmp == 0 {
	//          return cmp, kfpos, -1
	//      } else {
	//          return cmp, -1, -1
	//      }
	//
	// Returns:
	//    - cmp, result of comparision, either -1, 0, 1.
	//    - kfpos, if > -1, it means the keys are equal and specifies the
	//      offset in kv-file that contains the key.
	//    - dfpos, if > -1, it means the docids are equal and specifies the
	//      offset in kv-file that contains the docid.
	CompareLess(s *Store, kfpos int64, dfpos int64, isD bool) (int, int64, int64)

	// check whether both key and document-id compares equal.
	Equal([]byte, []byte) (bool, bool)
}

type Value interface {
	// transform actual value content into byte slice, that can be persisted in
	// file.
	Bytes() []byte
}

// Create a new instance of btree. `store` will be used to persist btree
// blocks, key-value data and associated meta-information.
func NewBTree(store *Store) *BTree {
	btree := BTree{Config: store.Config, store: store}
	return &btree
}

// Opposite of NewBTree() API, make sure to call this on every instance of
// BTree before exiting.
func (bt *BTree) Close() {
	bt.store.Close()
}

func (bt *BTree) Insert(key Key, v Value) bool {
	root, mv, timestamp := bt.store.OpStart(true) // root with transaction
	spawn, mk, md := root.insert(bt.store, key, v, mv)
	if spawn != nil { // Root splits
		in := (&inode{}).newNode(bt.store)

		in.ks[0], in.ds[0] = mk, md
		in.ks, in.ds = in.ks[:1], in.ds[:1]
		in.size = len(in.ks)

		in.vs[0] = root.getKnode().fpos
		in.vs[1] = spawn.getKnode().fpos
		in.vs = in.vs[:2]

		mv.commits[in.fpos] = in
		root = in
	}
	mv.root = root.getKnode().fpos
	bt.store.OpEnd(true, mv, timestamp) // Then this
	return true
}

func (bt *BTree) Count() int64 {
	root, mv, timestamp := bt.store.OpStart(false)
	count := root.count(bt.store)
	bt.store.OpEnd(false, mv, timestamp)
	return count
}

func (bt *BTree) Front() ([]byte, []byte, []byte) {
	root, mv, timestamp := bt.store.OpStart(false)
	b, c, d := root.front(bt.store)
	bt.store.OpEnd(false, mv, timestamp)
	return b, c, d
}

func (bt *BTree) Contains(key Key) bool {
	root, mv, timestamp := bt.store.OpStart(false)
	st := root.contains(bt.store, key)
	bt.store.OpEnd(false, mv, timestamp)
	return st
}

func (bt *BTree) Equals(key Key) bool {
	root, mv, timestamp := bt.store.OpStart(false)
	st := root.equals(bt.store, key)
	bt.store.OpEnd(false, mv, timestamp)
	return st
}

func (bt *BTree) FullSet() <-chan []byte {
	c := make(chan []byte)
	go func() {
		root, mv, timestamp := bt.store.OpStart(false)
		root.traverse(bt.store, func(kpos, dpos int64, vpos int64) {
			c <- bt.store.fetchKey(kpos)
			c <- bt.store.fetchDocid(dpos)
			c <- bt.store.fetchValue(vpos)
		})
		bt.store.OpEnd(false, mv, timestamp)
		close(c)
	}()
	return c
}

func (bt *BTree) KeySet() <-chan []byte {
	c := make(chan []byte)
	go func() {
		root, mv, timestamp := bt.store.OpStart(false)
		root.traverse(bt.store, func(kpos, dpos int64, vpos int64) {
			c <- bt.store.fetchKey(kpos)
		})
		bt.store.OpEnd(false, mv, timestamp)
		close(c)
	}()
	return c
}

func (bt *BTree) DocidSet() <-chan []byte {
	c := make(chan []byte)
	go func() {
		root, mv, timestamp := bt.store.OpStart(false)
		root.traverse(bt.store, func(kpos, dpos int64, vpos int64) {
			c <- bt.store.fetchDocid(dpos)
		})
		bt.store.OpEnd(false, mv, timestamp)
		close(c)
	}()
	return c
}

func (bt *BTree) ValueSet() <-chan []byte {
	c := make(chan []byte)
	go func() {
		root, mv, timestamp := bt.store.OpStart(false)
		root.traverse(bt.store, func(kpos, dpos int64, vpos int64) {
			c <- bt.store.fetchValue(vpos)
		})
		bt.store.OpEnd(false, mv, timestamp)
		close(c)
	}()
	return c
}

func (bt *BTree) Lookup(key Key) chan []byte {
	c := make(chan []byte)
	go func() {
		root, _, timestamp := bt.store.OpStart(false)
		root.lookup(bt.store, key, func(val []byte) {
			c <- val
		})
		bt.store.OpEnd(false, nil, timestamp)
		close(c)
	}()
	return c
}

func (bt *BTree) Remove(key Key) bool {
	root, mv, timestamp := bt.store.OpStart(true) // root with transaction
	if root.getKnode().size > 0 {
		root, _, _, _ = root.remove(bt.store, key, mv)
	} else {
		panic("Empty index")
	}
	mv.root = root.getKnode().fpos
	bt.store.OpEnd(true, mv, timestamp) // Then this
	return true                         // FIXME: What is this ??
}

func (bt *BTree) Drain() {
	bt.store.wstore.translock <- true
	bt.store.wstore.commit(nil, 0, true)
	<-bt.store.wstore.translock
}

func (bt *BTree) Check() {
	root, _, timestamp := bt.store.OpStart(false)
	if bt.store.Debug {
		log.Println("Check access", root.getKnode().fpos, timestamp)
	}
	log.Println("Checking btree ... root:", root.getKnode().fpos)
	wstore := bt.store.wstore
	if bt.store.Debug {
		log.Printf(
			"mvQ:   %10v     commitQ:      %10v\n",
			wstore.mvQ, wstore.commitQ,
		)
		log.Printf(
			"head-root:     %10v      head-ts:   %10v\n",
			wstore.head.root, wstore.head.timestamp,
		)
	}
	c := CheckContext{nodepath: make([]int64, 0)}
	root.check(bt.store, &c)
	root.checkSeparator(bt.store, make([]int64, 0))
	bt.store.OpEnd(false, nil, timestamp)
	if bt.store.Debug {
		log.Println("Check end", timestamp)
	}
}

func (bt *BTree) Show() {
	fmt.Printf(
		"flist:%v block:%v maxKeys:%v\n\n",
		bt.Flistsize, bt.Blocksize, bt.store.maxKeys(),
	)
	root, mv, timestamp := bt.store.OpStart(false)
	root.show(bt.store, 0)
	bt.store.OpEnd(false, mv, timestamp)
}

func (bt *BTree) ShowKeys() {
	root, mv, timestamp := bt.store.OpStart(false)
	root.showKeys(bt.store, 0)
	bt.store.OpEnd(false, mv, timestamp)
}

func (bt *BTree) Stats(check bool) {
	store := bt.store
	wstore := store.wstore
	currentStales := make([]int64, 0, 100)
	for _, mv := range bt.store.wstore.mvQ {
		currentStales = append(currentStales, mv.stales...)
	}
	fmt.Printf(
		"ncHits:       %10v      lcHits:   %10v     keyHits:      %10v\n",
		wstore.ncHits, wstore.lcHits, wstore.keyHits,
	)
	fmt.Printf(
		"docidHits:    %10v     maxlenNC:  %10v    maxlenLC:      %10v \n",
		wstore.docidHits, wstore.maxlenNC, wstore.maxlenLC,
	)
	fmt.Printf(
		"commitHits:   %10v    popCounts:  %10v    maxlenAccessQ: %10v\n",
		wstore.commitHits, wstore.popCounts, wstore.maxlenAccessQ,
	)
	fmt.Printf(
		"reclaimed:    %10v    recycled:   %10v    commitQ:       %10v\n",
		wstore.reclaimCount, wstore.recycleCount, len(wstore.commitQ),
	)
	fmt.Printf(
		"mvQ:          %10v    maxlenMVQ:  %10v\n",
		len(wstore.mvQ), wstore.maxlenMVQ,
	)
	fmt.Printf(
		"appendCounts: %10v    flushHeads: %10v    flushFreelists:%10v\n",
		wstore.appendCounts, wstore.flushHeads, wstore.flushFreelists,
	)
	fmt.Printf(
		"dumpCounts:   %10v    loadCounts: %10v    mvloadCounts:  %10v\n",
		wstore.dumpCounts, wstore.loadCounts, wstore.MVloadCounts,
	)
	fmt.Printf(
		"readKV:       %10v      appendKV: %10v    stales:        %10v\n",
		wstore.countReadKV, wstore.countAppendKV, len(currentStales),
	)
	fmt.Printf(
		"garbageBlocks:%10v      freelist: %10v    opCount:       %10v\n",
		wstore.garbageBlocks, len(wstore.freelist.offsets), wstore.opCounts,
	)
	if check {
		bt.Check()
	}
	// Level counts
	acc, icount, kcount := bt.LevelCount()
	fmt.Println("Levels :", acc, icount, kcount)
}

func (bt *BTree) LevelCount() ([]int64, int64, int64) {
	root, mv, timestamp := bt.store.OpStart(false)
	acc := make([]int64, 0, 16)
	acc, icount, kcount := root.levelCount(bt.store, 0, acc, 0, 0)
	ln := int64(len(bt.store.wstore.freelist.offsets) - 1)
	fmt.Println("Blocks: ", icount+kcount+ln)
	bt.store.OpEnd(false, mv, timestamp)
	return acc, icount, kcount
}
