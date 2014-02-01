package btree

import (
	"testing"
)

func Test_Head(t *testing.T) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	head := store.wstore.head
	if head.wstore != store.wstore {
		t.Fail()
	}
	if head.root != store.wstore.fpos_firstblock {
		t.Fail()
	}
}
