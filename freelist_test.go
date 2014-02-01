package btree

import (
	"testing"
)

func Test_maxFreeBlock(t *testing.T) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	wstore := store.wstore
	if (wstore.maxFreeBlocks() * 8) != int(store.Flistsize) {
		t.Fail()
	}
}

func Test_fetchFreelist(t *testing.T) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	wstore := store.wstore
	freelist := wstore.freelist
	firstfpos := wstore.fpos_firstblock
	if (freelist.offsets[0] != firstfpos+int64(wstore.Blocksize)) ||
		(freelist.offsets[1] != firstfpos+(2*int64(wstore.Blocksize))) {
		t.Fail()
	}
}
