package main

import (
	"compress/bzip2"
	"compress/gzip"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/cyberdelia/lzo"
	"github.com/golang/protobuf/proto"
)

type FrameReader2 struct {
	reader io.Reader
	buffer []byte
	length int64
	readed int64
}

func NewFrameReader2(imageFile *os.File, offset int64, length int64, codec string) (*FrameReader2, error) {
	_, err := imageFile.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	var reader io.Reader

	//fmt.Println("NewFrameReader2:codec", codec)

	//fmt.Println(">>>>>>offset, length", offset, length)
	if codec == "org.apache.hadoop.io.compress.DefaultCodec" {
		reader, err = zlib.NewReader(io.LimitReader(imageFile, length))
		if err != nil {
			return nil, err
		}
	} else if codec == "org.apache.hadoop.io.compress.SnappyCodec" {
		reader, err = NewBlockReader(io.LimitReader(imageFile, length), snappy_decompress)
		if err != nil {
			return nil, err
		}
	} else if codec == "org.apache.hadoop.io.compress.GzipCodec" {
		reader, err = gzip.NewReader(io.LimitReader(imageFile, length))
		if err != nil {
			return nil, err
		}
	} else if codec == "org.apache.hadoop.io.compress.BZip2Codec" {
		reader = bzip2.NewReader(io.LimitReader(imageFile, length))
	} else if codec == "com.hadoop.compression.lzo.LzoCodec" {
		reader, err = NewBlockReader(io.LimitReader(imageFile, length), lzo_decompress)
		if err != nil {
			return nil, err
		}
	} else if codec == "com.hadoop.compression.lzo.LzopCodec" {
		reader, err = lzo.NewReader(io.LimitReader(imageFile, length))
		if err != nil {
			return nil, err
		}
	}

	if err != nil {
		fmt.Println(">>>>err", err)
	}

	return &FrameReader2{
		buffer: make([]byte, 10485760),
		reader: reader,
		length: length,
	}, nil
}

var ErrorBrokenSection2 = fmt.Errorf("section is broken2")

func (r *FrameReader2) ReadUvarint() (uint64, error) {
	//if r.readed >= r.length {
	//	return 0, io.EOF
	//}
	//fmt.Println(">>>>>>ReadUvarint")
	var buf [10]byte

	var i int

	// varint max length is 10 bytes
	for i = 0; i < 10; i++ {
		//if r.readed >= r.length {
		//fmt.Println(">>>>>>ReadUvarint1")
		//	return 0, ErrorBrokenSection2
		//}

		n, err := r.reader.Read(buf[i : i+1])
		r.readed += int64(n)
		if err != nil {
			return 0, err
		}
		if n != 1 {
			//fmt.Println(">>>>>>ReadUvarint2")
			return 0, ErrorBrokenSection2
		}

		if buf[i]&0x80 == 0 {
			break
		}
	}

	if i >= 10 {
		//fmt.Println(">>>>>>ReadUvarint3")
		return 0, ErrorBrokenSection2
	}

	v, n := binary.Uvarint(buf[:i+1])
	if n <= 0 {
		//fmt.Println(">>>>>>ReadUvarint4")
		return 0, ErrorBrokenSection2
	}

	return v, nil
}

func (r *FrameReader2) ReadFrame() ([]byte, error) {
	length, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	//fmt.Println(">>>>>>ReadFrame", length, r.readed+int64(length), r.length)
	//if r.readed+int64(length) > r.length {
	//	return nil, ErrorBrokenSection2
	//}
	n, err := io.ReadFull(r.reader, r.buffer[:length])
	r.readed += int64(n)
	if err != nil {
		return nil, err
	}

	return r.buffer[:length], nil
}

func (r *FrameReader2) ReadMessage(msg proto.Message) error {
	body, err := r.ReadFrame()
	if err != nil {
		return err
	}
	if err = proto.Unmarshal(body, msg); err != nil {
		return err
	}
	return nil
}
