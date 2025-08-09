package buffer

import (
	"bytes"
	"encoding/gob"
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

func (pg *PageGuard) Drop() {
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
	var buffer bytes.Buffer
	gob := gob.NewEncoder(&buffer)
	if err := gob.Encode(obj); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func ToStruct[T any](data []byte) (T, error) {
	var res T
	gob := gob.NewDecoder(bytes.NewReader(data))
	if err := gob.Decode(&res); err != nil {

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
