package index

import (
	"cmp"
	"slices"
)

type bplusInternalPage[K cmp.Ordered] struct {
	bplusPageHeader
	keys   []K
	values []int64
}

func (p *bplusInternalPage[K]) init(pageId, parentPageId int64) {
	p.pageType = INTERNAL_PAGE
	p.pageId = pageId
	p.parent = parentPageId
	p.maxSize = 256 // todo: calculate max size
}

func (p *bplusInternalPage[K]) isLeafPage() bool {
	return p.pageType == LEAF_PAGE
}

func (p *bplusInternalPage[K]) keyAt(idx int) K {
	return p.keys[idx]
}

func (p *bplusInternalPage[K]) valueAt(idx int) int64 {
	return p.values[idx]
}

func (p *bplusInternalPage[K]) getSize() int {
	return int(p.size)
}

func (p *bplusInternalPage[K]) getInsertIdx(key K) int {
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

func (p *bplusInternalPage[K]) setKeyAt(idx int, key K) {
	p.keys[idx] = key
}

func (p *bplusInternalPage[K]) setValAt(idx int, value int64) {
	p.values[idx] = value
}

func (p *bplusInternalPage[K]) addKeyVal(key K, val int64) {
	insertIdx := p.getInsertIdx(key)
	p.keys = slices.Insert(p.keys, insertIdx, key)
	p.values = slices.Insert(p.values, insertIdx, val)
}
