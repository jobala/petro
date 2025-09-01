package index

import (
	"cmp"
)

type PAGE_TYPE = int

const (
	INVALID_PAGE PAGE_TYPE = iota
	INTERNAL_PAGE
	LEAF_PAGE
)

const HEADER_PAGE_ID = 0
const SLOT_SIZE = 100

func (p *bplusLeafPage[K, V]) init(pageId, parentPageId int64) {
	p.PageType = LEAF_PAGE
	p.PageId = pageId
	p.Parent = parentPageId
	p.Keys = make([]K, SLOT_SIZE)
	p.Values = make([]V, SLOT_SIZE)
	p.MaxSize = SLOT_SIZE // todo: calculate max size
}

type bplusLeafPage[K cmp.Ordered, V any] struct {
	BplusPageHeader[K, V]
}
