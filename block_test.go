package btree

import (
	"testing"
)

func Benchmark_gobenc(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	max := store.maxKeys()
	kn := (&knode{}).newNode(store)
	kn.ks = kn.ks[:0]
	kn.vs = kn.vs[:0]
	for i := 0; i < max; i++ {
		kn.ks = append(kn.ks, int64(i))
		kn.ds = append(kn.ds, int64(i))
		kn.vs = append(kn.vs, int64(i))
	}
	kn.vs = append(kn.vs, 0)
	kn.size = max
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kn.gobEncode()
	}
}

func Benchmark_gobdec(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	max := store.maxKeys()
	kn := (&knode{}).newNode(store)
	kn.ks = kn.ks[:0]
	kn.vs = kn.vs[:0]
	for i := 0; i < max; i++ {
		kn.ks = append(kn.ks, int64(i))
		kn.ds = append(kn.ds, int64(i))
		kn.vs = append(kn.vs, int64(i))
	}
	kn.vs = append(kn.vs, 0)
	kn.size = max
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytebuf := kn.gobEncode()
		kn.gobDecode(bytebuf)
	}
}
