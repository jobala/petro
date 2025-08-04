package index

import (
	"fmt"

	"github.com/jobala/petro/buffer"
	"github.com/jobala/petro/storage/disk"
)

func newBplusTree(name string, bpm *buffer.BufferpoolManager) (*bplusTree, error) {
	guard, err := bpm.WritePage(HEADER_PAGE_ID)
	defer guard.Drop()
	if err != nil {
		return nil, fmt.Errorf("error reading header page: %v", err)
	}

	headerData := guard.GetDataMut()
	headerPage, err := buffer.ToStruct[headerPage](*headerData)
	if err != nil {
		return nil, fmt.Errorf("error getting header page: %v", err)
	}

	headerPage.rootPageId = disk.INVALID_PAGE_ID
	*headerData, _ = buffer.ToByteSlice(headerPage)

	// todo: support setting maxsize
	return &bplusTree{
		indexName: name,
		bpm:       bpm,
		header:    headerPage,
	}, nil
}

func (b *bplusTree) findLeafPageId(rootPageId int64, key int) (int64, error) {
	currPageId := rootPageId

	for {
		guard, err := b.bpm.ReadPage(currPageId)
		if err != nil {
			guard.Drop()
			return 0, fmt.Errorf("error reading page: %v", err)
		}

		currPage, err := buffer.ToStruct[page](guard.GetData())
		if err != nil {
			guard.Drop()
			return 0, fmt.Errorf("error casting page: %v", err)
		}

		if currPage.isLeafPage() {
			guard.Drop()
			return currPageId, nil
		}

		childIdx := 0
		for i := 1; i < currPage.getSize(); i++ {
			if key >= currPage.keyAt(i) {
				childIdx = i
			} else {
				break
			}
		}

		currPageId = currPage.valueAt(childIdx)
		guard.Drop()
	}
}

func (b *bplusTree) isEmpty() bool {
	return b.header.rootPageId == disk.INVALID_PAGE_ID
}

func (b *bplusTree) setRootPageId(pageId int64) error {
	b.header.rootPageId = pageId
	writeGuard, err := b.bpm.WritePage(HEADER_PAGE_ID)
	defer writeGuard.Drop()

	if err != nil {
		return fmt.Errorf("error setting rootPageId")
	}

	data, err := buffer.ToByteSlice(b.header)
	if err != nil {
		return fmt.Errorf("error converting header struct to byteslice: %v", err)
	}

	copy(*writeGuard.GetDataMut(), data)
	return nil
}

type bplusTree struct {
	bpm       *buffer.BufferpoolManager
	indexName string
	header    headerPage
}

type headerPage struct {
	rootPageId int64
}
