package index

import (
	"cmp"
	"unsafe"

	"github.com/jobala/petro/storage/disk"
)

func (p *bplusInternalPage[K]) init(pageId, parentPageId int64) {
	p.PageType = INTERNAL_PAGE
	p.PageId = pageId
	p.Parent = parentPageId
	p.Keys = make([]K, SLOT_SIZE)
	p.Values = make([]int64, SLOT_SIZE)
	p.MaxSize = SLOT_SIZE // todo: calculate max size
}

func (p *bplusInternalPage[K]) toBytes() []byte {
	res := make([]byte, disk.PAGE_SIZE)
	bytes := unsafe.Slice((*byte)(unsafe.Pointer(p)), unsafe.Sizeof(*p))
	copy(res, bytes)

	return res
}

type bplusInternalPage[K cmp.Ordered] struct {
	BplusPageHeader[K, int64]
}
