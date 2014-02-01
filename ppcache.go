// The following is the general idea on cache structure.
//
//                                  |
//  *------------*    WRITE         |     READ       *------------*
//  |   inode    |       ^          |       ^        |   inode    |
//  | ping-cache |       |          |       |        | pong-cache |
//  |            |       *<-----*-----------*        |            |
//  |   knode    |       |      |   |       *------->|   knode    |
//  | ping-cache |       |      |   |     ncache()   | pong-cache |
//  *------------*       |  commitQ |                *------------*
//        ^              V      ^   |        (Locked access using sync.Mutex)
//        |           *------*  |   |
// commits*-----------| MVCC |<-*   |
// recyles            *------*      |
// reclaims              |          |
//                       *----->ping2Pong() (atomic, no locking)
//                                  |
//                                  |
//
// The cycle of ping-pong,
//
//  - reads will always refer to the pong-cache.
//  - reads will populate the cache from disk, when ever cache lookup fails.
//  - writes will refer to the commitQ maintained by MVCC, if node is not in
//    commitQ it will refer to pong-cache.
//
//  - ping-cache is operated only by the MVCC controller.
//  - MVCC controller will _populate_ the ping-cache when new nodes are
//    generated due to index mutations.
//  - MVCC controller will _evict_ the pong-cache as and when nodes become stale
//    due to index mutations.
//
//  - ping2Pong() happens when snapshot is flushed to disk.
//  - pong becomes ping, and MVCC controller will _populate_ and _evict_ the
//    newly flipped ping-cache based on commited, recycled and reclaimed node,
//    before allowing further mutations.
//

package btree

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

// In-memory data structure to cache intermediate nodes.
type pingPong struct {
	// pong map for intermediate nodes and leaf nodes
	ncpong unsafe.Pointer
	lcpong unsafe.Pointer
	// ping map for intermediate nodes and leaf nodes
	ncping unsafe.Pointer
	lcping unsafe.Pointer
	// pong map for keys and docids
	kdping unsafe.Pointer
	kdpong unsafe.Pointer
	sync.RWMutex
}

func (wstore *WStore) ncacheLookup(fpos int64) Node {
	wstore.RLock()
	defer wstore.RUnlock()

	var node Node
	nc := (*map[int64]Node)(atomic.LoadPointer(&wstore.ncpong))
	if node = (*nc)[fpos]; node == nil {
		lc := (*map[int64]Node)(atomic.LoadPointer(&wstore.lcpong))
		if node = (*lc)[fpos]; node != nil {
			wstore.lcHits += 1
		}
	} else {
		wstore.ncHits += 1
	}
	return node
}

func (wstore *WStore) ncache(node Node) {
	wstore.Lock()
	defer wstore.Unlock()

	fpos := node.getKnode().fpos
	if node.isLeaf() {
		lc := (*map[int64]Node)(atomic.LoadPointer(&wstore.lcpong))
		if len(*lc) < wstore.MaxLeafCache {
			(*lc)[fpos] = node
		}
		wstore.maxlenLC = max(wstore.maxlenLC, int64(len(*lc)))
	} else {
		nc := (*map[int64]Node)(atomic.LoadPointer(&wstore.ncpong))
		(*nc)[fpos] = node
		wstore.maxlenNC = max(wstore.maxlenNC, int64(len(*nc)))
	}
}

func (wstore *WStore) ncacheEvict(fposs []int64) {
	wstore.Lock()
	defer wstore.Unlock()

	nc := (*map[int64]Node)(atomic.LoadPointer(&wstore.ncpong))
	lc := (*map[int64]Node)(atomic.LoadPointer(&wstore.lcpong))
	for _, fpos := range fposs {
		delete(*nc, fpos)
		delete(*lc, fpos)
	}
}

func (wstore *WStore) _pingCache(fpos int64, node Node) {
	nc := (*map[int64]Node)(atomic.LoadPointer(&wstore.ncping))
	lc := (*map[int64]Node)(atomic.LoadPointer(&wstore.lcping))
	if node.isLeaf() {
		(*lc)[fpos] = node
	} else {
		(*nc)[fpos] = node
	}
}

func (wstore *WStore) _pingCacheEvict(fpos int64) {
	nc := (*map[int64]Node)(atomic.LoadPointer(&wstore.ncping))
	lc := (*map[int64]Node)(atomic.LoadPointer(&wstore.lcping))
	delete(*nc, fpos)
	delete(*lc, fpos)
}

func (wstore *WStore) cacheKey(fpos int64, key []byte) {
	wstore.pingKey(DEFER_ADD, fpos, key)
}

func (wstore *WStore) cacheDocid(fpos int64, docid []byte) {
	wstore.pingDocid(DEFER_ADD, fpos, docid)
}

func (wstore *WStore) lookupKey(rfd *os.File, fpos int64) []byte {
	var key []byte
	kdpong := (*map[int64][]byte)(atomic.LoadPointer(&wstore.kdpong))
	if key = (*kdpong)[fpos]; key == nil {
		return wstore.readKV(rfd, fpos)
	} else {
		wstore.keyHits += 1
	}
	return key
}

func (wstore *WStore) lookupDocid(rfd *os.File, fpos int64) []byte {
	var docid []byte
	kdpong := (*map[int64][]byte)(atomic.LoadPointer(&wstore.kdpong))
	if docid = (*kdpong)[fpos]; docid == nil {
		return wstore.readKV(rfd, fpos)
	} else {
		wstore.docidHits += 1
	}
	return docid
}

func (wstore *WStore) assertNotMemberCache(offsets []int64) {
	if wstore.Debug {
		nc := (*map[int64]Node)(atomic.LoadPointer(&wstore.ncping))
		lc := (*map[int64]Node)(atomic.LoadPointer(&wstore.lcping))
		for _, fpos := range offsets {
			if (*nc)[fpos] != nil {
				log.Panicln("to be freed fpos is in ncping-cache", fpos)
			} else if (*lc)[fpos] != nil {
				log.Panicln("to be freed fpos is in ncping-cache", fpos)
			}
		}
	}
}

func (wstore *WStore) ping2Pong() {
	wstore.Lock()

	// Swap nodecache
	ncping := atomic.LoadPointer(&wstore.ncping)
	ncpong := atomic.LoadPointer(&wstore.ncpong)
	atomic.StorePointer(&wstore.ncpong, ncping)
	atomic.StorePointer(&wstore.ncping, ncpong)
	// Swap leafcache
	lcping := atomic.LoadPointer(&wstore.lcping)
	lcpong := atomic.LoadPointer(&wstore.lcpong)
	atomic.StorePointer(&wstore.lcpong, lcping)
	atomic.StorePointer(&wstore.lcping, lcpong)

	// Swap keycache
	kdping := atomic.LoadPointer(&wstore.kdping)
	kdpong := atomic.LoadPointer(&wstore.kdpong)
	atomic.StorePointer(&wstore.kdpong, kdping)
	atomic.StorePointer(&wstore.kdping, kdpong)

	defer wstore.Unlock()

	// Trim leaf cache
	lc := (*map[int64]Node)(atomic.LoadPointer(&wstore.lcping))
	nc := (*map[int64]Node)(atomic.LoadPointer(&wstore.ncping))
	wstore.maxlenLC = max(wstore.maxlenLC, int64(len(*lc)))
	wstore.maxlenNC = max(wstore.maxlenNC, int64(len(*nc)))
	if len(*lc) > wstore.MaxLeafCache {
		i := len(*lc)
		for x := range *lc {
			if i < wstore.MaxLeafCache {
				break
			}
			delete(*lc, x)
		}
	}
}

func (wstore *WStore) displayPing() {
	ncping := (*map[int64]Node)(atomic.LoadPointer(&wstore.ncping))
	fposs := make([]int64, 0, 100)
	for fpos, _ := range *ncping {
		fposs = append(fposs, fpos)
	}

	lcping := (*map[int64]Node)(atomic.LoadPointer(&wstore.lcping))
	fposs = make([]int64, 0, 100)
	for fpos, _ := range *lcping {
		fposs = append(fposs, fpos)
	}
}

func (wstore *WStore) checkPingPong() {
	ncping := (*map[int64]Node)(atomic.LoadPointer(&wstore.ncping))
	ncpong := (*map[int64]Node)(atomic.LoadPointer(&wstore.ncpong))
	if len(*ncping) != len(*ncpong) {
		panic("Mismatch in nc ping-pong lengths")
	}
	for fpos := range *ncping {
		if (*ncpong)[fpos] == nil {
			panic("fpos not found in nc ping-pong")
		}
	}

	//lcping := (*map[int64]Node)(atomic.LoadPointer(&wstore.lcping))
	//lcpong := (*map[int64]Node)(atomic.LoadPointer(&wstore.lcpong))
	//if len(*lcping) != len(*lcpong) {
	//    panic("Mismatch in lc ping-pong lengths")
	//}
	//for fpos := range *lcping {
	//    if (*lcpong)[fpos] == nil {
	//        panic("fpos not found in lc ping-pong")
	//    }
	//}

	kdping := (*map[int64][]byte)(atomic.LoadPointer(&wstore.kdping))
	kdpong := (*map[int64][]byte)(atomic.LoadPointer(&wstore.kdpong))
	if len(*kdping) != len(*kdpong) {
		panic("Mismatch in kd ping-pong lengths")
	}
	for fpos := range *kdping {
		if (*kdpong)[fpos] == nil {
			panic("fpos not found in kd ping-pong")
		}
	}
}
