package index

import (
	"cmp"
)

func (p *bplusInternalPage[K]) init(pageId, parentPageId int64) {
	p.PageType = INTERNAL_PAGE
	p.PageId = pageId
	p.Parent = parentPageId
	p.Keys = make([]K, SLOT_SIZE)
	p.Values = make([]int64, SLOT_SIZE)
	p.MaxSize = SLOT_SIZE // todo: calculate max size
}

type bplusInternalPage[K cmp.Ordered] struct {
	BplusPageHeader[K, int64]
}
