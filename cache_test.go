package btree

import (
	"testing"
)

func Test_Cache(t *testing.T) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	cache := NewDCache(store.Blocksize, 1024*1024, 0xFFFFF)
	for i := 0; i < (int(store.Flistsize/8) / 2); i++ {
		node := (&knode{}).newNode(store)
		nfpos := node.getKnode().fpos
		cache.cache(nfpos, node)
		if cache.cacheLookup(nfpos) == nil {
			t.Error("cacheLookup failed")
		}
		if cache.cacheEvict(nfpos).getKnode().fpos != nfpos {
			t.Error("cacheEvict failed")
		}
		if cache.cacheLookup(nfpos) != nil {
			t.Error("cacheLookup false positive")
		}
	}
}

func Benchmark_Cache(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Close()
	}()

	cache := NewDCache(store.Blocksize, 1024*64, 0xFFFF)
	node := (&knode{}).newNode(store)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.cache(int64(i)<<cache.rshift, node)
	}
}

func Benchmark_CacheEvict(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Close()
	}()

	count := 1000000
	cache := NewDCache(store.Blocksize, 1024*4, 0xFFF)
	node := (&knode{}).newNode(store)
	for i := 0; i < count; i++ {
		cache.cache(int64(i)<<cache.rshift, node)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.cacheEvict(int64(i) << cache.rshift)
	}
}

func Benchmark_CacheLookup(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Close()
	}()

	count := 10000000
	cache := NewDCache(store.Blocksize, 1024*64, 0xFFFF)
	node := (&knode{}).newNode(store)
	for i := 0; i < count; i++ {
		cache.cache(int64(i)<<cache.rshift, node)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.cacheLookup(int64(i%count) << cache.rshift)
	}
}
