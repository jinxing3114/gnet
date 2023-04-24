package gnet

const (
	GfdElBit     = 24
	GfdConnIndex = 0xFFFFFF
)

type GFD struct {
	fd    int
	index uint32
}

func (gfd GFD) FD() int {
	return gfd.fd
}

func (gfd GFD) elIndex() int {
	return int(gfd.index >> 24)
}

func (gfd GFD) connIndex() int {
	return int(gfd.index & GfdConnIndex)
}

func (gfd *GFD) updateConnIndex(connIndex int) {
	(*gfd).index |= uint32(connIndex) & GfdConnIndex
}

func newGFD(fd, elIndex, connIndex int) GFD {
	return GFD{fd: fd, index: uint32(uint8(elIndex))<<GfdElBit + uint32(connIndex)&GfdConnIndex}
}
