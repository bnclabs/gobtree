// Common functions used across test cases.
package btree

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"time"
)

var _ = fmt.Sprintln(time.Now())

var testconf1 = Config{
	Idxfile: "./data/index_datafile.dat",
	Kvfile:  "./data/appendkv_datafile.dat",
	IndexConfig: IndexConfig{
		Sectorsize: 512,
		Flistsize:  1000 * OFFSET_SIZE,
		Blocksize:  4 * 1024,
	},
	Maxlevel:      6,
	RebalanceThrs: 6,
	AppendRatio:   0.7,
	DrainRate:     10,
	MaxLeafCache:  1000,
	Sync:          false,
	Nocache:       false,
}

type TestKey struct {
	K  string
	Id int64
}
type TestValue struct {
	V string
}

func (tk *TestKey) Bytes() []byte {
	return []byte(tk.K)
}

func (tk *TestKey) Docid() []byte {
	return []byte(fmt.Sprintf("%020v", tk.Id))
}

func (tk *TestKey) CompareLess(s *Store, kfp, dfp int64, isD bool) (int, int64, int64) {
	var otherk, otherd []byte
	var cmp int

	otherk = s.fetchKey(kfp)
	// Compare
	if cmp = bytes.Compare(tk.Bytes(), otherk); cmp == 0 && isD {
		otherd = s.fetchKey(dfp)
		cmp = bytes.Compare(tk.Docid(), otherd)
		if cmp == 0 {
			return cmp, kfp, dfp
		} else {
			return cmp, kfp, -1
		}
	} else if cmp == 0 {
		return cmp, kfp, -1
	} else {
		return cmp, -1, -1
	}
}

func (tk *TestKey) Equal(otherk []byte, otherd []byte) (bool, bool) {
	var keyeq, doceq bool
	if otherk == nil {
		keyeq = false
	} else {
		keyeq = bytes.Equal(tk.Bytes(), otherk)
	}
	if otherd == nil {
		doceq = false
	} else {
		doceq = bytes.Equal(tk.Docid(), otherd)
	}
	return keyeq, doceq
}

func (tv *TestValue) Bytes() []byte {
	return []byte(tv.V)
}

func TestData(count int, seed int64) ([]*TestKey, []*TestValue) {
	if seed < 0 {
		seed = int64(time.Now().Nanosecond())
	}
	rnd := rand.New(rand.NewSource(seed))

	keys := make([]*TestKey, 0, count)
	values := make([]*TestValue, 0, count)
	for i := 0; i < count; i++ {
		keys = append(keys, &TestKey{RandomKey(rnd), int64(i)})
		values = append(values, &TestValue{RandomValue(rnd) + "Value"})
	}
	return keys, values
}

func testStore(remove bool) *Store {
	if remove {
		os.Remove("./data/index_datafile.dat")
		os.Remove("./data/appendkv_datafile.dat")
	}
	return NewStore(testconf1)
}

var keys = make([]string, 0)

func RandomKey(rnd *rand.Rand) string {
	if len(keys) == 0 {
		fd, _ := os.Open("./data/words")
		scanner := bufio.NewScanner(fd)
		for scanner.Scan() {
			keys = append(keys, scanner.Text())
		}
	}
	return keys[rnd.Intn(len(keys))]
}

func RandomValue(rnd *rand.Rand) string {
	return RandomKey(rnd)
}
