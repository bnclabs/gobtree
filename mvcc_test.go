package btree

import (
	"testing"
)

func Benchmark_access(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts, _ := store.wstore.access(false)
		store.wstore.release(ts)
	}
}
