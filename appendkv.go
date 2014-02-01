//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// Supplies API to append/fetch key/value/docid from kv-file. kv-file is
// opened and managed by the WStore structure.
// entry format is,
//
//      | 4-byte size | size-byte value |
//
// Maximum size of each entry is int32, that is 2^31.
package btree

import (
	"log"
	"os"
	"reflect"
	"unsafe"
)

// Append/Fetch value as either byte-slice or string
func (store *Store) fetchValue(fpos int64) []byte {
	return store.wstore.readKV(store.kvRfd, fpos)
}

func (store *Store) fetchValueS(fpos int64) string {
	return string(store.wstore.readKV(store.kvRfd, fpos))
}

func (store *Store) appendValue(val []byte) int64 {
	return store.wstore.appendKV(val)
}

func (store *Store) appendValueS(val string) int64 {
	return store.wstore.appendKV([]byte(val))
}

// Append/Fetch key as either byte-slice or string
func (store *Store) fetchKey(fpos int64) []byte {
	x := store.wstore.readKV(store.kvRfd, fpos)
	return x
}

func (store *Store) fetchKeyS(fpos int64) string {
	return string(store.wstore.readKV(store.kvRfd, fpos))
}

func (store *Store) appendKey(key []byte) int64 {
	return store.wstore.appendKV(key)
}

func (store *Store) appendKeyS(key string) int64 {
	return store.wstore.appendKV([]byte(key))
}

// Append/Fetch Docid as either byte-slice or string
func (store *Store) fetchDocid(fpos int64) []byte {
	return store.wstore.readKV(store.kvRfd, fpos)
}

func (store *Store) fetchDocidS(fpos int64) string {
	return string(store.wstore.readKV(store.kvRfd, fpos))
}

func (store *Store) appendDocid(docid []byte) int64 {
	return store.wstore.appendKV(docid)
}

func (store *Store) appendDocidS(docid string) int64 {
	return store.wstore.appendKV([]byte(docid))
}

// Read bytes from `kvStore.rfd` at `fpos`.
func (wstore *WStore) readKV(rfd *os.File, fpos int64) []byte {
	if _, err := rfd.Seek(fpos, os.SEEK_SET); err != nil {
		log.Panicln(err, fpos)
	}

	buf := make([]byte, 4)
	rfd.ReadAt(buf, fpos) // Read size field
	b := make([]byte, bytesToint32(buf))
	if _, err := rfd.ReadAt(b, fpos+4); err != nil {
		panic(err)
	}
	wstore.countReadKV += 1
	return b
}

func (wstore *WStore) appendKV(val []byte) int64 {
	wfd := wstore.kvWfd
	fpos, _ := wfd.Seek(0, os.SEEK_END)
	buf := int32Tobytes(int32(len(val)))
	wfd.WriteAt(buf, fpos)
	if _, err := wfd.WriteAt(val, fpos+4); err != nil {
		panic(err)
	}
	wstore.countAppendKV += 1
	return fpos
}

func bytesToint32(buf []byte) int32 {
	bufp := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	size := (*int32)(unsafe.Pointer(bufp.Data))
	return *size
}

func int32Tobytes(size int32) []byte {
	buf := make([]byte, 4, 4)
	bufp := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	bufp.Data = (uintptr)(unsafe.Pointer(&size))
	return buf
}
