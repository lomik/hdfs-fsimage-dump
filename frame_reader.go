package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"os"
)

type FrameReader struct {
	reader io.Reader
	buffer []byte
	length int64
	readed int64
}

func NewFrameReader(imageFile *os.File, offset int64, length int64) (*FrameReader, error) {
	_, err := imageFile.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	return &FrameReader{
		buffer: make([]byte, 10485760),
		reader: bufio.NewReader(imageFile),
		length: length,
	}, nil
}

var ErrorBrokenSection = fmt.Errorf("section is broken")

func (r *FrameReader) ReadUvarint() (uint64, error) {
	if r.readed >= r.length {
		return 0, io.EOF
	}

	var buf [10]byte

	var i int

	// varint max length is 10 bytes
	for i = 0; i < 10; i++ {
		if r.readed >= r.length {
			return 0, ErrorBrokenSection
		}

		n, err := r.reader.Read(buf[i : i+1])
		r.readed += int64(n)
		if err != nil {
			return 0, err
		}
		if n != 1 {
			return 0, ErrorBrokenSection
		}

		if buf[i]&0x80 == 0 {
			break
		}
	}

	if i >= 10 {
		return 0, ErrorBrokenSection
	}

	v, n := binary.Uvarint(buf[:i+1])
	if n <= 0 {
		return 0, ErrorBrokenSection
	}

	return v, nil
}

func (r *FrameReader) ReadFrame() ([]byte, error) {
	length, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}

	if r.readed+int64(length) > r.length {
		return nil, ErrorBrokenSection
	}
	n, err := io.ReadFull(r.reader, r.buffer[:length])
	r.readed += int64(n)
	if err != nil {
		return nil, err
	}

	return r.buffer[:length], nil
}

func (r *FrameReader) ReadMessage(msg proto.Message) error {
	body, err := r.ReadFrame()
	if err != nil {
		return err
	}
	if err = proto.Unmarshal(body, msg); err != nil {
		return err
	}
	return nil
}
