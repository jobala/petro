package buffer

import (
	"sync"
	"sync/atomic"

	"github.com/jobala/petro/storage/disk"
)

func (f *frame) pin() {
	f.pins.Add(1)
}

func (f *frame) unpin() int32 {
	return f.pins.Add(-1)
}

func (f *frame) reset() {
	f.dirty = false
	f.pins.Store(0)
	f.data = make([]byte, disk.PAGE_SIZE)
}

type frame struct {
	mu     sync.RWMutex
	id     int
	data   []byte
	pins   atomic.Int32
	dirty  bool
	pageId int64
}
