package buffer

func NewPageGuard(frame *frame, replacer *lrukReplacer) *PageGuard {
	return &PageGuard{
		frame:    frame,
		replacer: replacer,
	}
}

func (pg *PageGuard) drop() {
	pg.frame.unpin()
	if pg.frame.pins.Load() == 0 {
		pg.replacer.setEvictable(pg.frame.id, true)
	}

	pg.frame.mu.Unlock()
}

func (pg *PageGuard) getData() []byte {
	return pg.frame.data
}

type PageGuard struct {
	frame    *frame
	replacer *lrukReplacer
}
