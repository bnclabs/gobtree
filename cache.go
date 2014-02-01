// This module provides a lock free interface to cache data structure. The
// general idea is to create an array of pointers, each pointing to the head
// of DCacheItem linked list.
//
// Since nodes are cached based on their file-position (fpos), fpos is hashed
// to index into the array. Refer indexFor() to know how.
//
// Subsequenly we walk the linked list for a match in fpos and corresponding
// cached node.
//
// For deleting we simply remove the node from the list.
//
// For inserting, we prepend the new DCacheItem as the head of the list, and
// walk the remaining list to delete the item, if it is already present.
//
// (fpos & hashmask) >> rshift
//    |
//    |   *--------*
//    *-->| head   | -->|DcacheItem|-->|DcacheItem|-->|DcacheItem|
//        *--------*
//    |   *--------*
//    *-->| head   | -->|DcacheItem|-->|DcacheItem|-->|DcacheItem|
//        *--------*
//
//        ...
//
//    |   *--------*
//    *-->| head   | -->|DcacheItem|-->|DcacheItem|-->|DcacheItem|
//        *--------*

package btree

import (
	"log"
	"sync/atomic"
	"unsafe"
)

type DCache struct {
	blocksize int64          // size of blocks that are cached.
	hashmask  int64          // will be used to mask fpos for array index
	rshift    byte           // computed based on blocksize
	hash      unsafe.Pointer // *[]unsafe.Pointer
}

// Singley linked list.
type DCacheItem struct {
	fpos int64
	node Node
	next unsafe.Pointer
}

func NewDCache(blocksize, hashsize int64, hashmask int64) *DCache {
	if blocksize <= 0 && (blocksize&(blocksize-1) != 0) {
		log.Panicln("blocksize should be power of 2")
	}

	cache := DCache{blocksize: blocksize, hashmask: hashmask}
	for blocksize != 0 {
		blocksize = blocksize >> 1
		cache.rshift++
	}
	cache.rshift--
	hash := make([]unsafe.Pointer, hashsize)
	cache.hash = unsafe.Pointer(&hash)
	return &cache
}

func (cache *DCache) cache(fpos int64, node Node) bool {
	idx := cache.indexFor(fpos)
	item := DCacheItem{fpos: fpos, node: node}

	// Prepend the new key.
	for {
		hash := (*[]unsafe.Pointer)(atomic.LoadPointer(&(cache.hash)))
		addr := &((*hash)[idx])
		item.next = atomic.LoadPointer(addr)
		if atomic.CompareAndSwapPointer(addr, item.next, unsafe.Pointer(&item)) {
			break
		}
	}

	// Walk the remaining list to remove the old entry, if present.
	for {
		var retry bool
		addr := &item.next
		hd := (*DCacheItem)(atomic.LoadPointer(addr))
		for hd != nil {
			nx := atomic.LoadPointer(&hd.next)
			if hd.fpos == item.fpos {
				if !atomic.CompareAndSwapPointer(addr, unsafe.Pointer(hd), nx) {
					retry = true
				}
				break
			}
			addr = &hd.next
			hd = (*DCacheItem)(nx)
		}
		if retry {
			continue
		}
		break
	}
	return true
}

func (cache *DCache) cacheLookup(fpos int64) Node {
	idx := cache.indexFor(fpos)
	hash := (*[]unsafe.Pointer)(atomic.LoadPointer(&(cache.hash)))
	head := (*DCacheItem)(atomic.LoadPointer(&((*hash)[idx])))
	for head != nil {
		if head.fpos == fpos {
			return head.node
		}
		head = (*DCacheItem)(atomic.LoadPointer(&head.next))
	}
	return nil
}

func (cache *DCache) cacheEvict(fpos int64) Node {
	var node Node
	idx := cache.indexFor(fpos)
	for {
		var retry bool
		hash := (*[]unsafe.Pointer)(atomic.LoadPointer(&(cache.hash)))
		addr := &((*hash)[idx])
		hd := (*DCacheItem)(atomic.LoadPointer(addr))
		for hd != nil {
			nx := atomic.LoadPointer(&hd.next)
			if hd.fpos == fpos {
				if !atomic.CompareAndSwapPointer(addr, unsafe.Pointer(hd), nx) {
					retry = true
				} else {
					node = hd.node
				}
				break
			}
			addr = &hd.next
			hd = (*DCacheItem)(nx)
		}
		if retry {
			continue
		}
		break
	}
	return node
}

func (cache *DCache) indexFor(fpos int64) int {
	return int((fpos >> cache.rshift) & cache.hashmask)
}
