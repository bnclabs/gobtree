// MVCC controller process.
package btree

import (
	"log"
	//"time"
)

const (
	WS_SAYHI byte = iota
	WS_CLOSE      // {WS_CLOSE}

	// messages to mvcc goroutine
	WS_ACCESS      // {WS_ACCESS} -> timestamp int64
	WS_RELEASE     // {WS_RELEASE, timestamp} -> minAccess int64
	WS_SETSNAPSHOT // {WS_SETSNAPSHOT, offsets []int64, root int64, timestamp int64}

	// messages to defer routine
	WS_PINGCACHE    // {WS_PINGCACHE, what byte, fpos int64, node Node}
	WS_PINGKD       // {WS_PINGKD, fpos int64, key []byte}
	WS_MV           // {WS_MV, mv *MV}
	WS_SYNCSNAPSHOT // {WS_MV, minAccess int64}
)

const (
	IO_FLUSH byte = iota
	IO_APPEND
	IO_CLOSE
)

type ReclaimData struct {
	fpos      int64 // Node file position that needs to be reclaimed to free-list
	timestamp int64 // transaction timestamp under which fpos became stale.
}
type RecycleData ReclaimData

type MVCC struct {
	accessQ   []int64            // sorted slice of timestamps
	req       chan []interface{} // Communication channel for MVCC goroutine.
	translock chan bool          // transaction channel
}

func (wstore *WStore) access(transaction bool) (int64, int64) {
	res := make(chan []interface{})
	wstore.req <- []interface{}{WS_ACCESS, transaction, res}
	rets := <-res
	ts, rootfpos := rets[0].(int64), rets[1].(int64)
	return ts, rootfpos
}

func (wstore *WStore) release(timestamp int64) int64 {
	res := make(chan []interface{})
	wstore.req <- []interface{}{WS_RELEASE, timestamp, res}
	minAccess := (<-res)[0].(int64)
	return minAccess
}

func (wstore *WStore) setSnapShot(offsets []int64, mvroot, mvts int64) {
	res := make(chan []interface{})
	wstore.req <- []interface{}{WS_SETSNAPSHOT, offsets, mvroot, mvts, res}
	<-res
}

func doMVCC(wstore *WStore) {
	req := wstore.req
	tscount := wstore.head.timestamp
	for {
		cmd := <-req
		if cmd == nil {
			break
		}
		switch cmd[0].(byte) {
		case WS_SAYHI: // say hi!
			res := cmd[1].(chan []interface{})
			res <- []interface{}{WS_SAYHI}
		case WS_ACCESS:
			transaction, res := cmd[1].(bool), cmd[2].(chan []interface{})
			if transaction {
				tscount++
			}
			wstore.accessQ = append(wstore.accessQ, tscount)
			if wstore.Debug {
				isSorted(wstore.accessQ)
			}
			wstore.maxlenAccessQ =
				max(wstore.maxlenAccessQ, int64(len(wstore.accessQ)))
			res <- []interface{}{tscount, wstore.head.root}
		case WS_RELEASE:
			minAccess := wstore.minAccess(cmd[1].(int64))
			res := cmd[2].(chan []interface{})
			res <- []interface{}{minAccess}
		case WS_SETSNAPSHOT: // setSnapShot
			offsets := cmd[1].([]int64)
			mvroot, mvts := cmd[2].(int64), cmd[3].(int64)
			res := cmd[4].(chan []interface{})
			wstore.freelist.add(offsets)
			wstore.head.setRoot(mvroot, mvts)
			wstore.ping2Pong()
			res <- nil
		case WS_CLOSE:
			res := cmd[1].(chan []interface{})
			res <- nil
		}
	}
}

func (wstore *WStore) closeChannels() {
	res := make(chan []interface{})
	wstore.req <- []interface{}{WS_CLOSE, res}
	<-res
	close(wstore.req)
	wstore.req = nil

	syncChan := make(chan []interface{})
	wstore.deferReq <- []interface{}{WS_CLOSE, syncChan}
	<-syncChan
	close(wstore.deferReq)
	wstore.deferReq = nil
}

// Demark the timestamp to zero in accessQ and return the minimum value of
// timestamp from accessQ. Also remove demarked timestamps from accessQ uptil
// the lowest timestamp.
// TODO : Can we optimize the two loops into single loop
func (wstore *WStore) minAccess(demarkts int64) int64 {
	var done bool
	// Shrink accessQ by sliding out demarked access.
	for i, ts := range wstore.accessQ {
		if ts == demarkts {
			done = true
			wstore.accessQ[i] = 0
			break
		}
	}
	if done == false {
		panic("Couldn't find timestamp")
	}

	skip := 0
	for _, ts := range wstore.accessQ {
		if ts == 0 {
			skip += 1
			continue
		}
		break
	}
	wstore.accessQ = wstore.accessQ[skip:]
	if len(wstore.accessQ) == 0 {
		return 0
	} else if wstore.accessQ[0] == 0 {
		log.Panicln("After sliding the accessQ window, can't be zero")
	}
	return wstore.accessQ[0]
}

func isSorted(xs []int64) {
	for i := 0; i < len(xs)-1; i++ {
		if xs[i+1] > 0 && xs[i] > xs[i+1] {
			log.Panicln("Non sorted xs", xs)
		}
	}
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
