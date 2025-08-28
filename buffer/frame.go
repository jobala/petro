package buffer

import (
	"sync"
	"sync/atomic"

	"github.com/jobala/petro/storage/disk"
)

func (f *Frame) pin() {
	f.pins.Add(1)
}

func (f *Frame) unpin() int32 {
	return f.pins.Add(-1)
}

func (f *Frame) reset() {
	f.dirty = false
	f.pins.Store(0)
	f.Data = make([]byte, disk.PAGE_SIZE)
}

type Frame struct {
	Data   []byte
	mu     sync.RWMutex
	id     int
	pins   atomic.Int32
	dirty  bool
	pageId int64
}
