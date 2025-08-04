package buffer

import (
	"bytes"
	"encoding/gob"
)

func NewReadPageGuard(frame *frame, replacer *lrukReplacer) *ReadPageGuard {
	return &ReadPageGuard{
		PageGuard: PageGuard{
			frame:    frame,
			replacer: replacer,
		},
	}
}

func NewWritePageGuard(frame *frame, replacer *lrukReplacer) *WritePageGuard {
	return &WritePageGuard{
		PageGuard: PageGuard{
			frame:    frame,
			replacer: replacer,
		},
	}
}

func (pg *PageGuard) Drop() {
	pg.frame.unpin()
	if pg.frame.pins.Load() == 0 {
		pg.replacer.setEvictable(pg.frame.id, true)
	}

	pg.frame.mu.Unlock()
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
		return res, err
	}

	return res, nil
}

type PageGuard struct {
	frame    *frame
	replacer *lrukReplacer
}

type ReadPageGuard struct {
	PageGuard
}

type WritePageGuard struct {
	PageGuard
}
