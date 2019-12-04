package main

import (
	"github.com/lomik/hdfs-fsimage-dump/lzo"

	"github.com/golang/snappy"
)

func snappy_decompress(src []byte, dst []byte) (n int, err error) {
	decompDst, err := snappy.Decode(nil, src)
	if err != nil {
		return 0, err
	}
	n = len(dst)
	copy(dst, decompDst)
	return
}

func lzo_decompress(src []byte, dst []byte) (n int, err error) {
	n, err = lzo.LzoDecompress(src, dst)
	return
}
