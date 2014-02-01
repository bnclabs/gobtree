// Manages head sector of btree index-file. Head sector contains the following
// items,
//      rootFileposition int64
//      timestamp int64
//      sectorsize int64
//      flistsize int64
//      blocksize int64
//      maxkeys int64
//      pick int64
//      crc uint32

package btree

import (
	"bytes"
	"encoding/binary"
	"os"
)

// Structure to manage the head sector
type Head struct {
	wstore     *WStore
	dirty      bool  // tells whether `root` has side-effects
	fpos_head1 int64 // file-offset into index file where 1st-head is
	fpos_head2 int64 // file-offset into index file where 2nd-head is
	// Following fields are persisted on disk.
	root       int64  // file-offset into index file that has root block
	timestamp  int64  // snapshot's timestamp is synced with disk aswell.
	sectorsize int64  // head sector-size in bytes.
	flistsize  int64  // free-list size in bytes.
	blocksize  int64  // btree block size in bytes.
	maxkeys    int64  // Maximum number of keys can be store in btree block.
	pick       int64  // either 0 or 1, which freelist to pick. NOT USED !!
	crc        uint32 // CRC value for head sector + freelist block
}

// Create a new Head sector structure.
func newHead(wstore *WStore) *Head {
	hd := Head{
		wstore:     wstore,
		pick:       0,
		sectorsize: wstore.Sectorsize,
		flistsize:  wstore.Flistsize,
		blocksize:  wstore.Blocksize,
		dirty:      false,
		root:       0,
		fpos_head1: 0,
		fpos_head2: wstore.Sectorsize,
	}
	return &hd
}

// Clone `hd` to `newhd`.
func (hd *Head) clone() *Head {
	newhd := newHead(hd.wstore)
	newhd.pick = hd.pick
	newhd.dirty = hd.dirty
	newhd.root = hd.root
	newhd.timestamp = hd.timestamp
	return newhd
}

// Fetch head sector from index file, read root block's file position and
// check whether head1 and head2 copies are consistent.
func (hd *Head) fetch() bool {
	LittleEndian := binary.LittleEndian
	if hd.dirty {
		panic("Cannot read index head when in-memory copy is dirty")
	}
	rfd, _ := os.Open(hd.wstore.Idxfile)

	data1 := make([]byte, hd.sectorsize) // Read from first sector
	data2 := make([]byte, hd.sectorsize) // Read from second sector
	if _, err := rfd.ReadAt(data1, hd.fpos_head1); err != nil {
		panic(err)
	}
	if _, err := rfd.ReadAt(data2, hd.fpos_head2); err != nil {
		panic(err)
	}

	buf := bytes.NewBuffer(data1)
	if err := binary.Read(buf, LittleEndian, &hd.root); err != nil {
		panic("Unable to read root from first head sector")
	}
	if err := binary.Read(buf, LittleEndian, &hd.timestamp); err != nil {
		panic("Unable to read root from first head sector")
	}
	if err := binary.Read(buf, LittleEndian, &hd.sectorsize); err != nil {
		panic("Unable to read sectorsize from first head sector")
	}
	if err := binary.Read(buf, LittleEndian, &hd.flistsize); err != nil {
		panic("Unable to read flistsize from first head sector")
	}
	if err := binary.Read(buf, LittleEndian, &hd.blocksize); err != nil {
		panic("Unable to read blocksize from first head sector")
	}
	if err := binary.Read(buf, LittleEndian, &hd.maxkeys); err != nil {
		panic("Unable to read maxkeys from first head sector")
	}
	if err := binary.Read(buf, LittleEndian, &hd.pick); err != nil {
		panic("Unable to read pick from first head sector")
	}
	if err := binary.Read(buf, LittleEndian, &hd.crc); err != nil {
		panic("Unable to read crc from first head sector")
	}

	if bytes.Equal(data1, data2) {
		return false
	}
	return true
}

// Refer to new root block. When ever an entry / block is updated the entire
// chain has to be re-added.
func (hd *Head) setRoot(fpos int64, timestamp int64) *Head {
	hd.root = fpos
	hd.timestamp = timestamp
	hd.dirty = true
	return hd
}

// flush head-structure to index-file. Updates CRC for freelist.
func (hd *Head) flush(crc uint32) *Head {
	wfd := hd.wstore.idxWfd
	LittleEndian := binary.LittleEndian

	hd.crc = crc

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, LittleEndian, &hd.root)
	binary.Write(buf, LittleEndian, &hd.timestamp)
	binary.Write(buf, LittleEndian, &hd.sectorsize)
	binary.Write(buf, LittleEndian, &hd.flistsize)
	binary.Write(buf, LittleEndian, &hd.blocksize)
	binary.Write(buf, LittleEndian, &hd.maxkeys)
	binary.Write(buf, LittleEndian, &hd.pick)
	binary.Write(buf, LittleEndian, &hd.crc)

	valb := buf.Bytes()
	wfd.WriteAt(valb, hd.fpos_head2) // Write into head sector2
	wfd.WriteAt(valb, hd.fpos_head1) // Write into head sector1

	hd.dirty = false
	hd.wstore.flushHeads += 1
	return hd
}
