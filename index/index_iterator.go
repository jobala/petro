package index

import (
	"cmp"
	"fmt"

	"github.com/jobala/petro/buffer"
)

func NewIndexIterator[K cmp.Ordered, V any](pageId int64, bpm *buffer.BufferpoolManager) *indexIterator[K, V] {
	guard, _ := bpm.ReadPage(pageId)
	firstPage, _ := buffer.ToStruct[bplusLeafPage[K, V]](guard.GetData())

	return &indexIterator[K, V]{
		currPage: &firstPage,
		bpm:      bpm,
		pos:      0,
	}
}

func (it *indexIterator[K, V]) Next() (V, error) {
	var res V
	if it.pos < it.currPage.getSize() {
		res := it.currPage.valueAt(it.pos)
		it.pos += 1

		return res, nil
	}

	it.pos = 0
	guard, err := it.bpm.ReadPage(it.currPage.Next)
	if err != nil {
		return res, fmt.Errorf("error getting guard for page: %v", err)
	}
	defer guard.Drop()

	nextPage, err := buffer.ToStruct[bplusLeafPage[K, V]](guard.GetData())
	if err != nil {
		return res, fmt.Errorf("error casting page: %v", err)
	}
	it.currPage = &nextPage

	res = it.currPage.valueAt(it.pos)
	it.pos += 1
	return res, nil
}

func (it *indexIterator[K, V]) IsEnd() bool {
	return it.currPage.Next == 0 && it.pos >= it.currPage.getSize()
}

type indexIterator[K cmp.Ordered, V any] struct {
	pos      int
	currPage *bplusLeafPage[K, V]
	bpm      *buffer.BufferpoolManager
}
