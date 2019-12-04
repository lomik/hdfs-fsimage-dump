package main

import (
	"fmt"
	"io"
)

type Decompress func(src []byte, dst []byte) (n int, err error)

type BlockReader struct {
	reader     io.Reader
	limit      uint32
	decoded    uint32
	buffer     []byte
	buff       []byte
	decompress Decompress
}

func NewBlockReader(r io.Reader, decompress Decompress) (io.Reader, error) {
	var reader = new(BlockReader)
	reader.reader = r
	reader.decompress = decompress
	return reader, nil
}

func (r *BlockReader) Read(b []byte) (n int, err error) {
	// defer func() {
	// 	fmt.Println("READ:", b)
	// }()
	if len(b) <= 0 {
		return 0, nil
	}
	//fmt.Println("BlockReader: Reader status:", r.limit, r.decoded, len(b))
	if len(b) <= len(r.buff) {
		//fmt.Printf("len(b) <= len(r.buff), len(b):%v, r.buff:%v\n", len(b), len(r.buff))
		copy(b, r.buff)
		r.buff = r.buff[len(b):]
		return len(b), nil
	} else {
		//fmt.Printf("len(b) > len(r.buff), len(b):%v, len(r.buff):%v\n", len(b), len(r.buff))
		readed := 0
		if len(r.buff) > 0 {
			copy(b, r.buff)
			readed = len(r.buff)
			r.buff = r.buff[readed:readed]
		}
		n, err := r.readBlock()
		//fmt.Println("after once readBlock:", n, err, r.buff)
		if err != nil {
			return readed, err
		}
		n, err = r.Read(b[readed:])
		if err != nil {
			return readed + n, err
		}
		return readed + n, nil
	}
}

func (r *BlockReader) readBlock() (int, error) {
	//fmt.Println("BlockReader:readBlock enter")
	buff := make([]byte, 4)
	// need handle a new big block
	if r.limit == r.decoded {
		//fmt.Println("BlockReader:readBlock:r.limit == r.decoded, is:", r.limit)
		_, err := io.ReadFull(r.reader, buff)
		if err != nil {
			return 0, err
		}
		r.limit = (uint32(buff[0]) << 24) + (uint32(buff[1]) << 16) + (uint32(buff[2]) << 8) + uint32(buff[3])
		r.decoded = 0
		r.buffer = make([]byte, r.limit)
		r.buff = r.buffer[0:0]
		//fmt.Println("BlockReader:readBlock: new big block size:", r.limit, buff)
	}

	// chuck
	_, err := io.ReadFull(r.reader, buff)
	if err != nil {
		return 0, err
	}
	// length: compressed chuck data length
	length := (uint32(buff[0]) << 24) + (uint32(buff[1]) << 16) + (uint32(buff[2]) << 8) + uint32(buff[3])
	buff2 := make([]byte, length)
	_, err = io.ReadFull(r.reader, buff2)
	if err != nil {
		return 0, err
	}
	//fmt.Println("BlockReader:readBlock: to decompress:", buff, buff2)
	deLen, err := r.decompress(buff2, r.buffer[r.decoded:])
	if err != nil {
		fmt.Println("BlockReader:readBlock: lzoDecompress err:", deLen, err)
		return deLen, err
	}
	//fmt.Println("BlockReader:readBlock: decode:", r.buffer[r.decoded:r.decoded+uint32(deLen)])
	r.decoded += uint32(deLen)
	r.buff = r.buff[0:deLen]
	//fmt.Println("BlockReader:readBlock: r.buff:", r.buff)
	return deLen, nil
}
