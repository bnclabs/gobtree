package btree

import (
	"bytes"
	"math/rand"
	"testing"
)

var datafile = "./data/appendkv_datafile.dat"

func Test_KV(t *testing.T) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	fpos := store.appendKey([]byte("Hello world"))
	if bytes.Equal(store.fetchKey(fpos), []byte("Hello world")) == false {
		t.Fail()
	}
}

var fposs = make([]int64, 0)
var maxEntries = 100000

func Benchmark_appendKV(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()

	data := "abcdefghijklmnopqrstuvwxyz " + "abcdefghijklmnopqrstuvwxyz "
	data += data
	data += data
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.appendValue([]byte(data))
	}
}

func Benchmark_appendFetchSetup(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Close()
	}()

	data := "abcdefghijklmnopqrstuvwxyz " + "abcdefghijklmnopqrstuvwxyz "
	data += data
	data += data
	for i := 0; i < maxEntries; i++ {
		fposs = append(fposs, store.appendKey([]byte(data)))
	}
	b.SkipNow()
}

func Benchmark_appendFetchKV(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Close()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.fetchKey(fposs[rand.Intn(maxEntries)])
	}
}

func Benchmark_appendFetchFinish(b *testing.B) {
	store := testStore(true)
	defer func() {
		store.Destroy()
	}()
	b.SkipNow()
}
