package buffer

import (
	"github.com/jobala/petro/storage/disk"
	"github.com/vmihailenco/msgpack/v5"
)

func NewReadPageGuard(frame *frame, bpm *BufferpoolManager) *ReadPageGuard {
	return &ReadPageGuard{
		PageGuard: PageGuard{
			frame: frame,
			bpm:   bpm,
		},
	}
}

func NewWritePageGuard(frame *frame, bpm *BufferpoolManager) *WritePageGuard {
	return &WritePageGuard{
		PageGuard: PageGuard{
			frame: frame,
			bpm:   bpm,
		},
	}
}

func (pg *ReadPageGuard) Drop() {
	if pg == nil || pg.frame == nil {
		return
	}

	pg.frame.unpin()
	if pg.frame.pins.Load() == 0 {
		pg.bpm.replacer.setEvictable(pg.frame.id, true)
	}

	pg.frame.mu.RUnlock()
	pg.bpm.mu.Lock()
	pg.bpm.cond.Signal()
	pg.bpm.mu.Unlock()
}

func (pg *WritePageGuard) Drop() {
	if pg == nil || pg.frame == nil {
		return
	}

	pg.frame.unpin()
	if pg.frame.pins.Load() == 0 {
		pg.bpm.replacer.setEvictable(pg.frame.id, true)
	}

	pg.frame.mu.Unlock()
	pg.bpm.mu.Lock()
	pg.bpm.cond.Signal()
	pg.bpm.mu.Unlock()
}

func (pg *ReadPageGuard) GetData() []byte {
	return pg.frame.data
}

func (pg *WritePageGuard) GetDataMut() *[]byte {
	return &pg.frame.data
}

func ToByteSlice[T any](obj T) ([]byte, error) {
	res := make([]byte, disk.PAGE_SIZE)

	data, err := msgpack.Marshal(obj)
	if err != nil {
		return nil, err
	}
	copy(res, data)

	return res, nil
}

func ToStruct[T any](data []byte) (T, error) {
	var res T

	if err := msgpack.Unmarshal(data, &res); err != nil {
		return res, nil
	}

	return res, nil
}

type PageGuard struct {
	frame *frame
	bpm   *BufferpoolManager
}

type ReadPageGuard struct {
	PageGuard
}

type WritePageGuard struct {
	PageGuard
}
