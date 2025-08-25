package index

import (
	"cmp"
	"unsafe"

	"github.com/jobala/petro/storage/disk"
)

type PAGE_TYPE = int

const (
	INVALID_PAGE PAGE_TYPE = iota
	INTERNAL_PAGE
	LEAF_PAGE
)

const HEADER_PAGE_ID = 0
const SLOT_SIZE = 50

func (p *bplusLeafPage[K, V]) init(pageId, parentPageId int64) {
	p.PageType = LEAF_PAGE
	p.PageId = pageId
	p.Parent = parentPageId
	p.Keys = make([]K, SLOT_SIZE)
	p.Values = make([]V, SLOT_SIZE)
	p.MaxSize = SLOT_SIZE // todo: calculate max size
}

func (p *bplusLeafPage[K, V]) toBytes() []byte {
	res := make([]byte, disk.PAGE_SIZE)
	bytes := unsafe.Slice((*byte)(unsafe.Pointer(p)), unsafe.Sizeof(*p))
	copy(res, bytes)

	return res
}

type bplusLeafPage[K cmp.Ordered, V any] struct {
	BplusPageHeader[K, V]
}
