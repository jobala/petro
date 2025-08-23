package index

import (
	"cmp"
	"slices"
	"unsafe"
)

func (p *BplusPageHeader[K, V]) keyAt(idx int) K {
	return p.Keys[idx]
}

func (p *BplusPageHeader[K, V]) valueAt(idx int) V {
	return p.Values[idx]
}

func (p *BplusPageHeader[K, V]) getSize() int {
	return int(p.Size)
}

func (p *BplusPageHeader[K, V]) getInsertIdx(key K) int {
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

func (p *BplusPageHeader[K, V]) setKeyAt(idx int, key K) {
	p.Keys[idx] = key
}

func (p *BplusPageHeader[K, V]) setValAt(idx int, value V) {
	p.Values[idx] = value
}

func (p *BplusPageHeader[K, V]) addKeyVal(key K, val V) {
	insertIdx := p.getInsertIdx(key)
	p.Keys = slices.Insert(p.Keys, insertIdx, key)
	p.Values = slices.Insert(p.Values, insertIdx, val)
}
func (p *BplusPageHeader[K, V]) isLeafPage() bool {
	return p.PageType == LEAF_PAGE
}

func (p *BplusPageHeader[K, V]) toBytes() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), unsafe.Sizeof(&p))
}

func ToStruct[T any](data []byte) *T {
	return (*T)(unsafe.Pointer(&data[0]))
}

type BplusPageHeader[K cmp.Ordered, V any] struct {
	PageId   int64
	Parent   int64
	Next     int64
	Prev     int64
	Size     int32
	MaxSize  int32
	PageType PAGE_TYPE
	Keys     []K
	Values   []V
}
