package gfd

import (
	"encoding/binary"
	"unsafe"
)

const (
	Conn2Start = 2
	FdStart    = 4
	FdSize     = unsafe.Sizeof(int(0))
	Size       = FdSize + FdStart
	ElMax      = 0xFF
	Conn1Max   = 0xFF
	Conn2Max   = 0xFFFF
)

type GFD [Size]byte

func (gfd GFD) FD() int {
	if FdSize == 4 {
		return int(binary.BigEndian.Uint32(gfd[FdStart:]))
	} else {
		return int(binary.BigEndian.Uint64(gfd[FdStart:]))
	}
}

func (gfd *GFD) ElIndex() int {
	return int(gfd[0])
}

func (gfd *GFD) ConnIndex1() int {
	return int(gfd[1])
}

func (gfd *GFD) ConnIndex2() int {
	return int(binary.BigEndian.Uint16(gfd[Conn2Start:FdStart]))
}

func (gfd *GFD) UpdateConnIndex(connIndex1, connIndex2 int) {
	gfd[1] = byte(connIndex1)
	binary.BigEndian.PutUint16(gfd[Conn2Start:FdStart], uint16(connIndex2))
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
