package buffer

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
