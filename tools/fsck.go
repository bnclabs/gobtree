package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/couchbaselabs/indexing/btree"
	"os"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging")

func main() {
	flag.Parse()
	args := flag.Args()
	rfd, _ := os.Open(args[0])

	rfd.Seek(0, os.SEEK_SET)
	root, sectorsize, flistsize, blocksize, maxkeys, pick, crc := readHead(rfd)
	fmt.Printf("Root       : %v\n", root)
	fmt.Printf("Sectorsize : %v\n", sectorsize)
	fmt.Printf("Flistsize  : %v\n", flistsize)
	fmt.Printf("Blocksize  : %v\n", blocksize)
	fmt.Printf("Blocksize  : %v\n", maxkeys)
	fmt.Printf("Pick       : %v\n", pick)
	fmt.Printf("CRC        : %v\n", crc)

	rfd.Seek(sectorsize*2, os.SEEK_SET)
	offsets := freefpos(rfd, flistsize)
	fmt.Println(len(offsets), offsets)
}

func readHead(rfd *os.File) (int64, int64, int64, int64, int64, int64, uint32) {
	var root, sectorsize, flistsize, blocksize, maxkeys, pick int64
	var crc uint32
	LittleEndian := binary.LittleEndian
	binary.Read(rfd, LittleEndian, &root)
	binary.Read(rfd, LittleEndian, &sectorsize)
	binary.Read(rfd, LittleEndian, &flistsize)
	binary.Read(rfd, LittleEndian, &blocksize)
	binary.Read(rfd, LittleEndian, &maxkeys)
	binary.Read(rfd, LittleEndian, &pick)
	binary.Read(rfd, LittleEndian, &crc)
	return root, sectorsize, flistsize, blocksize, maxkeys, pick, crc
}

func freefpos(rfd *os.File, flistsize int64) []int64 {
	var fpos int64
	bytebuf := make([]byte, flistsize)
	rfd.Read(bytebuf)
	// Load the offsets
	offsets := make([]int64, 0)
	buf := bytes.NewBuffer(bytebuf)
	binary.Read(buf, binary.LittleEndian, &fpos)
	for fpos != 0 {
		offsets = append(offsets, int64(fpos)) // include zero-terminator
		binary.Read(buf, binary.LittleEndian, &fpos)
	}
	offsets = append(offsets, int64(fpos)) // include zero-terminator
	return offsets
}
