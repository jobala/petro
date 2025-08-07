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
const SLOT_SIZE = 5

func (p *bplusLeafPage[K, V]) init(pageId, parentPageId int64) {
	p.PageType = LEAF_PAGE
	p.PageId = pageId
	p.Parent = parentPageId
	p.Keys = make([]K, SLOT_SIZE)
	p.Values = make([]V, SLOT_SIZE)
	p.MaxSize = 256 // todo: calculate max size
}
func (p *bplusLeafPage[K, V]) keyAt(idx int) K {
	return p.Keys[idx]
}

func (p *bplusLeafPage[K, V]) valueAt(idx int) V {
	return p.Values[idx]
}

func (p *bplusLeafPage[K, V]) getSize() int {
	return int(p.Size)
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
	p.Keys[idx] = key
}

func (p *bplusLeafPage[K, V]) setValAt(idx int, value V) {
	p.Values[idx] = value
}

func (p *bplusLeafPage[K, V]) addKeyVal(key K, val V) {
	insertIdx := p.getInsertIdx(key)
	p.Keys = slices.Insert(p.Keys, insertIdx, key)
	p.Values = slices.Insert(p.Values, insertIdx, val)
}

type bplusLeafPage[K cmp.Ordered, V any] struct {
	BplusPageHeader
	Keys   []K
	Values []V
}

type BplusPageHeader struct {
	PageId   int64
	Parent   int64
	Next     int64
	Prev     int64
	Size     int32
	MaxSize  int32
	PageType PAGE_TYPE
}

func (h *BplusPageHeader) isLeafPage() bool {
	return h.PageType == LEAF_PAGE
}
