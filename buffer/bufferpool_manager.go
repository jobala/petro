package buffer

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/jobala/petro/storage/disk"
)

func NewBufferpoolManager(size int, replacer *lrukReplacer, diskScheduler *disk.DiskScheduler) *BufferpoolManager {
	frames := make([]*frame, size)
	freeFrames := make([]int, size)

	for i := range size {
		f := &frame{
			id:   i,
			data: make([]byte, disk.PAGE_SIZE),
		}

		frames[i] = f
		freeFrames[i] = i
	}

	return &BufferpoolManager{
		mu:            sync.Mutex{},
		frames:        frames,
		pageTable:     make(map[int64]int),
		replacer:      replacer,
		diskScheduler: diskScheduler,
		freeFrames:    freeFrames,
	}
}

func (b *BufferpoolManager) ReadPage(pageId int64) (*PageGuard, error) {
	b.mu.Lock()
	var frame *frame

	if id, ok := b.pageTable[pageId]; ok {
		frame := b.frames[id]
		b.mu.Unlock()

		b.replacer.recordAccess(frame.id)
		b.replacer.setEvictable(frame.id, false)
		frame.mu.Lock()
		frame.pin()

		return NewPageGuard(frame, b.replacer), nil
	}

	if len(b.freeFrames) > 0 {
		id := b.freeFrames[0]
		frame = b.frames[id]
		b.freeFrames = b.freeFrames[1:]
	} else {
		id, err := b.replacer.evict()
		if err != nil {
			return nil, fmt.Errorf("error getting bufferpool frame")
		}

		frame = b.frames[id]
		b.flush(frame)
	}

	delete(b.pageTable, frame.pageId)
	b.pageTable[pageId] = frame.id
	b.mu.Unlock()

	b.replacer.recordAccess(frame.id)
	b.replacer.setEvictable(frame.id, false)

	frame.mu.Lock()
	frame.reset()
	frame.pin()
	frame.pageId = pageId
	diskReq := disk.NewRequest(pageId, nil, false)
	respCh := b.diskScheduler.Schedule(diskReq)
	resp := <-respCh
	copy(frame.data, resp.Data)

	return NewPageGuard(frame, b.replacer), nil
}

func (b *BufferpoolManager) WritePage(pageId int64, data []byte) (*PageGuard, error) {
	b.mu.Lock()
	var frame *frame

	if id, ok := b.pageTable[pageId]; ok {
		frame := b.frames[id]
		b.mu.Unlock()

		b.replacer.recordAccess(frame.id)
		b.replacer.setEvictable(frame.id, false)
		frame.mu.Lock()
		frame.pin()
		frame.dirty = true
		copy(frame.data, data)

		return NewPageGuard(frame, b.replacer), nil
	}

	if len(b.freeFrames) > 0 {
		id := b.freeFrames[0]
		frame = b.frames[id]
		b.freeFrames = b.freeFrames[1:]
	} else {
		id, err := b.replacer.evict()
		if err != nil {
			return nil, fmt.Errorf("error getting bufferpool frame")
		}

		frame = b.frames[id]
		b.flush(frame)
	}

	delete(b.pageTable, frame.pageId)
	b.pageTable[pageId] = frame.id
	b.mu.Unlock()

	b.replacer.recordAccess(frame.id)
	b.replacer.setEvictable(frame.id, false)

	frame.mu.Lock()
	frame.reset()
	frame.pin()
	frame.dirty = true
	frame.pageId = pageId
	copy(frame.data, data)

	return NewPageGuard(frame, b.replacer), nil
}

func (b *BufferpoolManager) NewPageId() int64 {
	return b.nextPageId.Add(1)
}

func (b *BufferpoolManager) flush(frame *frame) {
	if frame.dirty {
		writeReq := disk.NewRequest(frame.pageId, frame.data, true)
		respCh := b.diskScheduler.Schedule(writeReq)

		// block until data is written to disk
		<-respCh
	}
}

type BufferpoolManager struct {
	mu            sync.Mutex
	frames        []*frame
	pageTable     map[int64]int
	nextPageId    atomic.Int64
	diskScheduler *disk.DiskScheduler
	replacer      *lrukReplacer
	freeFrames    []int
}
