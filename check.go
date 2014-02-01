//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package btree

import (
	"os"
)

func (store *Store) check() bool {
	wstore := store.wstore
	freelist := wstore.freelist
	rfd, _ := os.Open(wstore.Idxfile)

	// Check whether configuration settings match.
	fi, _ := rfd.Stat()
	fpos_firstblock := (wstore.Sectorsize * 2) - (wstore.Flistsize * 2)
	blksize := fi.Size() - fpos_firstblock
	if fpos_firstblock != wstore.fpos_firstblock {
		return false
	}
	if blksize%int64(wstore.Blocksize) != 0 {
		return false
	}

	// Check freelist with btree.
	root, _, _ := store.OpStart(false)
	offs := root.listOffsets(store)
	qsortOffsets(offs)
	fulloffs := seq(wstore.fpos_firstblock, fi.Size(), int64(wstore.Blocksize))
	offsets := make([]int64, 0, len(fulloffs))
	i := 0
	for _, x := range offs {
		for i < len(fulloffs) {
			if fulloffs[i] < x {
				offsets = append(offsets, fulloffs[i])
			} else if fulloffs[i] > x {
				break
			}
			i++
		}
	}
	for ; i < len(fulloffs); i++ {
		offsets = append(offsets, fulloffs[i])
	}
	offsets = append(offsets, 0)
	if len(offsets) != len(freelist.offsets) {
		return false
	}

	count := 0
	for _, offset := range offsets {
		for _, floffset := range freelist.offsets {
			if offset == floffset {
				count += 1
				break
			}
		}
	}
	if count != len(offsets) {
		return false
	}
	return true
}

// Inplace quicksort
func qsortOffsets(arr []int64) {
	if len(arr) <= 1 {
		return
	}
	iPivot := qsort_partition(arr)
	qsortOffsets(arr[:iPivot])
	qsortOffsets(arr[iPivot-1:])
}

func qsort_partition(arr []int64) int {
	swap := func(arr []int64, i1, i2 int) {
		arr[i1], arr[i2] = arr[i2], arr[i1]
	}

	idx, lastidx := 0, len(arr)-1
	pivot := arr[lastidx] // rightmost element
	for i := 1; i < len(arr); i++ {
		if arr[i] < pivot {
			swap(arr, i, idx)
			idx++
		}
	}
	swap(arr, lastidx, idx)
	return idx
}

func seq(start, end, step int64) []int64 {
	out := make([]int64, 0, (end-start)/step)
	for i := start; i < end; i += step {
		out = append(out, i)
	}
	return out
}
