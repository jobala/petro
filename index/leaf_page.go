package index

import (
	"cmp"
	"slices"
)

type PAGE_TYPE = int

const (
	INVALID_PAGE PAGE_TYPE = iota
	INTERNAL_PAGE
	LEAF_PAGE
)

const HEADER_PAGE_ID = 0

func (p *bplusLeafPage[K, V]) init(pageId, parentPageId int64) {
	p.pageType = LEAF_PAGE
	p.pageId = pageId
	p.parent = parentPageId
	p.maxSize = 256 // todo: calculate max size
}
func (p *bplusLeafPage[K, V]) keyAt(idx int) K {
	return p.keys[idx]
}

func (p *bplusLeafPage[K, V]) valueAt(idx int) V {
	return p.values[idx]
}

func (p *bplusLeafPage[K, V]) getSize() int {
	return int(p.size)
}

func (p *bplusLeafPage[K, V]) getInsertIdx(key K) int {
	left := 0
	right := p.getSize() - 1

	for left <= right {
		mid := left + (right-left)/2
		if p.keyAt(mid) < key {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return left
}

func (p *bplusLeafPage[K, V]) setKeyAt(idx int, key K) {
	p.keys[idx] = key
}

func (p *bplusLeafPage[K, V]) setValAt(idx int, value V) {
	p.values[idx] = value
}

func (p *bplusLeafPage[K, V]) addKeyVal(key K, val V) {
	insertIdx := p.getInsertIdx(key)
	p.keys = slices.Insert(p.keys, insertIdx, key)
	p.values = slices.Insert(p.values, insertIdx, val)
}

type bplusLeafPage[K cmp.Ordered, V any] struct {
	bplusPageHeader
	keys   []K
	values []V
}

type bplusPageHeader struct {
	pageId   int64
	parent   int64
	next     int64
	prev     int64
	size     int32
	maxSize  int32
	pageType PAGE_TYPE
}

func (h *bplusPageHeader) isLeafPage() bool {
	return h.pageType == LEAF_PAGE
}
