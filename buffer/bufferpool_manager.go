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

	bpm := &BufferpoolManager{
		mu:            sync.Mutex{},
		frames:        frames,
		pageTable:     make(map[int64]int),
		replacer:      replacer,
		diskScheduler: diskScheduler,
		freeFrames:    freeFrames,
	}
	bpm.cond = *sync.NewCond(&bpm.mu)
	return bpm
}

func (b *BufferpoolManager) ReadPage(pageId int64) (*ReadPageGuard, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var frame *frame

	for {
		if id, ok := b.pageTable[pageId]; ok {
			frame := b.frames[id]

			b.replacer.recordAccess(frame.id)
			b.replacer.setEvictable(frame.id, false)
			frame.mu.Lock()
			frame.pin()

			return NewReadPageGuard(frame, b), nil
		}

		// try to get a frame
		if len(b.freeFrames) > 0 {
			id := b.freeFrames[0]
			frame = b.frames[id]
			b.freeFrames = b.freeFrames[1:]
		} else {
			if id, _ := b.replacer.evict(); id != disk.INVALID_PAGE_ID {
				frame = b.frames[id]
				b.flush(frame)
			}
		}

		// got a frame
		if frame != nil {
			delete(b.pageTable, frame.pageId)
			b.pageTable[pageId] = frame.id

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

			return NewReadPageGuard(frame, b), nil
		}

		// failed to get a frame, wait for a frame to become available
		b.cond.Wait()
	}
}

func (b *BufferpoolManager) WritePage(pageId int64) (*WritePageGuard, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var frame *frame

	for {
		if id, ok := b.pageTable[pageId]; ok {
			frame := b.frames[id]

			b.replacer.recordAccess(frame.id)
			b.replacer.setEvictable(frame.id, false)
			frame.mu.Lock()
			frame.pin()
			frame.dirty = true

			return NewWritePageGuard(frame, b), nil
		}

		// try getting a frame
		if len(b.freeFrames) > 0 {
			id := b.freeFrames[0]
			frame = b.frames[id]
			b.freeFrames = b.freeFrames[1:]
		} else {
			if id, _ := b.replacer.evict(); id != disk.INVALID_PAGE_ID {
				frame = b.frames[id]
				b.flush(frame)
			}
		}

		// got the frame, return a page guard
		if frame != nil {
			delete(b.pageTable, frame.pageId)
			b.pageTable[pageId] = frame.id

			b.replacer.recordAccess(frame.id)
			b.replacer.setEvictable(frame.id, false)

			frame.mu.Lock()
			frame.reset()
			frame.pin()
			frame.dirty = true
			frame.pageId = pageId

			return NewWritePageGuard(frame, b), nil
		}

		// failed to get a frame, wait for a frame to become available
		// pageGuard.Drop will send a signal
		fmt.Println("waiting for a frame to become available")
		b.cond.Wait()
	}
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
	cond          sync.Cond
}
