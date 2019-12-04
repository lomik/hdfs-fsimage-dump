package lzo

/*
#cgo LDFLAGS: -llzo2
#include <lzo/lzo1x.h>

static int lzo_initialize(void) { return lzo_init(); }
static int lzo1x_1_mem_compress() { return LZO1X_1_MEM_COMPRESS; }
static int lzo1x_999_mem_compress() { return LZO1X_999_MEM_COMPRESS; }
*/
import "C"

import (
	"fmt"
	"unsafe"
)

var (
	lzoMagic  = []byte{0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a}
	lzoErrors = []string{
		1: "data corrupted",
		2: "out of memory",
		4: "input overrun",
		5: "output overrun",
		6: "data corrupted",
		7: "eof not found",
		8: "input not consumed",
	}
)

func init() {
	if err := C.lzo_initialize(); err != 0 {
		panic("lzo: can't initialize")
	}
}

type errno int

func (e errno) Error() string {
	if 0 <= int(e) && int(e) < len(lzoErrors) {
		s := lzoErrors[e]
		if s != "" {
			return fmt.Sprintf("lzo: %s", s)
		}
	}
	return fmt.Sprintf("lzo: errno %d", int(e))
}

func LzoDecompress(src []byte, dst []byte) (int, error) {
	dstLen := len(dst)
	err := C.lzo1x_decompress_safe((*C.uchar)(unsafe.Pointer(&src[0])), C.lzo_uint(len(src)),
		(*C.uchar)(unsafe.Pointer(&dst[0])), (*C.lzo_uint)(unsafe.Pointer(&dstLen)), nil)
	if err != 0 {
		return 0, errno(err)
	}
	return dstLen, nil
}

func LzoCompress(src []byte, compress func([]byte, []byte, *int) C.int) ([]byte, error) {
	dstSize := 0
	dst := make([]byte, lzoDestinationSize(len(src)))
	err := compress(src, dst, &dstSize)
	if err != 0 {
		return nil, fmt.Errorf("lzo: errno %d", err)
	}
	return dst[0:dstSize], nil
}

func lzoDestinationSize(n int) int {
	return (n + n/16 + 64 + 3)
}

func lzoCompressSpeed(src []byte, dst []byte, dstSize *int) C.int {
	wrkmem := make([]byte, int(C.lzo1x_1_mem_compress()))
	return C.lzo1x_1_compress((*C.uchar)(unsafe.Pointer(&src[0])), C.lzo_uint(len(src)),
		(*C.uchar)(unsafe.Pointer(&dst[0])), (*C.lzo_uint)(unsafe.Pointer(dstSize)),
		unsafe.Pointer(&wrkmem[0]))
}

func lzoCompressBest(src []byte, dst []byte, dstSize *int) C.int {
	wrkmem := make([]byte, int(C.lzo1x_999_mem_compress()))
	return C.lzo1x_999_compress((*C.uchar)(unsafe.Pointer(&src[0])), C.lzo_uint(len(src)),
		(*C.uchar)(unsafe.Pointer(&dst[0])), (*C.lzo_uint)(unsafe.Pointer(dstSize)),
		unsafe.Pointer(&wrkmem[0]))
}
