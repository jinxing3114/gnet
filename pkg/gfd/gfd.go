package gfd

import (
	"encoding/binary"
	"unsafe"
)

const (
	FdStart = 2
	FdSize  = unsafe.Sizeof(int(0))
	Size    = FdSize + FdStart
	ElMax   = 0xFFFF
)

// GFD
// structure introduction
// |eventloop index|      fd     |
// |     2bit      |int type size|
type GFD [Size]byte

func (gfd GFD) Fd() int {
	if FdSize == 4 {
		return int(binary.BigEndian.Uint32(gfd[FdStart:]))
	} else {
		return int(binary.BigEndian.Uint64(gfd[FdStart:]))
	}
}

func (gfd *GFD) ElIndex() int {
	return int(binary.BigEndian.Uint32(gfd[0:FdStart]))
}

func NewGFD(fd, elIndex int) (gfd GFD) {
	gfd[0] = byte(elIndex)
	if FdSize == 4 {
		binary.BigEndian.PutUint32(gfd[FdStart:], uint32(fd))
	} else {
		binary.BigEndian.PutUint64(gfd[FdStart:], uint64(fd))
	}
	return
}

func CheckLegal(gfd GFD) bool {
	if gfd.Fd() <= 0 || gfd.ElIndex() < 0 || gfd.ElIndex() >= ElMax {
		return false
	}
	return true
}
