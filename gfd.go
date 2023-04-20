package gnet

const (
	GfdLbBit      = 56
	GfdElBit      = 32
	GfdLbLeftBit  = 8
	GfdLbRightBit = 40
)

type GFD uint64

func (gfd GFD) FD() int {
	return int(gfd)
}

func (gfd GFD) elIndex() int {
	return int(gfd >> GfdLbBit)
}

func (gfd GFD) connIndex() int {
	return int(gfd << GfdLbLeftBit >> GfdLbRightBit)
}

func (gfd GFD) updateElIndex(elIndex int) {
	gfd |= GFD(elIndex) << GfdLbRightBit >> GfdLbLeftBit
}

func newGFD(fd, elIndex, connIndex int) GFD {
	return GFD(uint64(uint32(fd)) + uint64(elIndex<<GfdLbBit) + uint64(connIndex<<GfdElBit))
}
