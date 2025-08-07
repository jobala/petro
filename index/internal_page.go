package index

import (
	"cmp"
	"slices"
)

func (p *bplusInternalPage[K]) init(pageId, parentPageId int64) {
	p.PageType = INTERNAL_PAGE
	p.PageId = pageId
	p.Parent = parentPageId
	p.Keys = make([]K, SLOT_SIZE)
	p.Values = make([]int64, SLOT_SIZE)
	p.MaxSize = 256 // todo: calculate max size
}

func (p *bplusInternalPage[K]) isLeafPage() bool {
	return p.PageType == LEAF_PAGE
}

func (p *bplusInternalPage[K]) keyAt(idx int) K {
	return p.Keys[idx]
}

func (p *bplusInternalPage[K]) valueAt(idx int) int64 {
	return p.Values[idx]
}

func (p *bplusInternalPage[K]) getSize() int {
	return int(p.Size)
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
	p.Keys[idx] = key
}

func (p *bplusInternalPage[K]) setValAt(idx int, value int64) {
	p.Values[idx] = value
}

func (p *bplusInternalPage[K]) addKeyVal(key K, val int64) {
	insertIdx := p.getInsertIdx(key)
	p.Keys = slices.Insert(p.Keys, insertIdx, key)
	p.Values = slices.Insert(p.Values, insertIdx, val)
}

type bplusInternalPage[K cmp.Ordered] struct {
	BplusPageHeader
	Keys   []K
	Values []int64
}
