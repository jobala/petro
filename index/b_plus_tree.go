package index

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"unsafe"

	"github.com/jobala/petro/buffer"
	"github.com/jobala/petro/storage/disk"
)

func NewBplusTree[K cmp.Ordered, V any](name string, bpm *buffer.BufferpoolManager) (*bplusTree[K, V], error) {
	guard, err := bpm.WritePage(HEADER_PAGE_ID)
	defer guard.Drop()
	if err != nil {
		return nil, fmt.Errorf("error reading header page: %v", err)
	}

	headerData := guard.GetDataMut()
	headerPage := ToStruct[headerPage](*headerData)

	headerPage.RootPageId = disk.INVALID_PAGE_ID
	*headerData = headerPage.toBytes()

	return &bplusTree[K, V]{
		indexName: name,
		bpm:       bpm,
		header:    *headerPage,
	}, nil
}

func (b *bplusTree[K, V]) getValue(key K) ([]V, error) {
	res := make([]V, 0)
	leafPageId, err := b.findLeafPageId(b.header.RootPageId, key)
	if err != nil {
		return nil, err
	}

	guard, err := b.bpm.ReadPage(leafPageId)
	if err != nil {
		return nil, err
	}
	defer guard.Drop()

	leafPage := ToStruct[bplusLeafPage[K, V]](guard.GetData())
	valIdx := leafPage.getInsertIdx(key)
	if valIdx < 0 || valIdx >= leafPage.getSize() {
		return nil, fmt.Errorf("key not found: %v", key)
	}

	res = append(res, leafPage.valueAt(valIdx))
	return res, nil
}

func (b *bplusTree[K, V]) insert(key K, value V) (bool, error) {
	if b.isEmpty() {
		pageId := b.bpm.NewPageId()
		guard, err := b.bpm.WritePage(pageId)
		if err != nil {
			guard.Drop()
			return false, err
		}

		leafPage := ToStruct[bplusLeafPage[K, V]](*guard.GetDataMut())

		leafPage.init(pageId, int64(INVALID_PAGE))
		leafPage.Size = 1
		leafPage.setKeyAt(0, key)
		leafPage.setValAt(0, value)

		data := leafPage.toBytes()
		copy(*guard.GetDataMut(), data)

		if err := b.setRootPageId(pageId); err != nil {
			guard.Drop()
			return false, err
		}

		guard.Drop()
	} else {
		leafPageId, err := b.findLeafPageId(b.header.RootPageId, key)
		if err != nil {
			return false, err
		}

		guard, err := b.bpm.WritePage(leafPageId)
		if err != nil {
			guard.Drop()
			return false, err
		}

		leafPage := ToStruct[bplusLeafPage[K, V]](*guard.GetDataMut())

		if leafPage.Size < leafPage.MaxSize {
			leafPage.addKeyVal(key, value)
			leafPage.Size += 1

			data := leafPage.toBytes()
			copy(*guard.GetDataMut(), data)
			guard.Drop()
		} else {
			// create new leaf node
			newLeafId := b.bpm.NewPageId()
			newGuard, err := b.bpm.WritePage(newLeafId)
			if err != nil {
				guard.Drop()
				newGuard.Drop()
				return false, err
			}

			newLeafPage := ToStruct[bplusLeafPage[K, V]](*newGuard.GetDataMut())
			newLeafPage.init(newLeafId, leafPage.Parent)

			insertIdx := leafPage.getInsertIdx(key)
			leafPage.Keys = slices.Insert(leafPage.Keys, insertIdx, key)
			leafPage.Values = slices.Insert(leafPage.Values, insertIdx, value)

			tmpKeyArr := make([]K, leafPage.MaxSize+1)
			tmpValArr := make([]V, leafPage.MaxSize+1)
			copy(tmpKeyArr, leafPage.Keys)
			copy(tmpValArr, leafPage.Values)

			// zero out keys and values in leaf page
			leafPage.Keys = make([]K, leafPage.MaxSize)
			leafPage.Values = make([]V, leafPage.MaxSize)

			tmpNexPage := leafPage.Next
			newLeafPage.Next = tmpNexPage
			leafPage.Next = newLeafId
			newLeafPage.Prev = leafPage.PageId

			midPoint := int(math.Ceil(float64(leafPage.MaxSize) / 2))

			// distribute values between leaf and new leaf
			copy(leafPage.Keys, tmpKeyArr[:midPoint])
			copy(leafPage.Values, tmpValArr[:midPoint])
			copy(newLeafPage.Keys, tmpKeyArr[midPoint:])
			copy(newLeafPage.Values, tmpValArr[midPoint:])

			leafPage.Size = int32(midPoint)
			newLeafPage.Size = int32(int(leafPage.MaxSize)-midPoint) + 1

			leafData := leafPage.toBytes()
			copy(*guard.GetDataMut(), leafData)

			newLeafData := newLeafPage.toBytes()
			copy(*newGuard.GetDataMut(), newLeafData)

			if err := b.insertInParent(guard, newGuard, newLeafPage.keyAt(0)); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func (b *bplusTree[K, V]) insertInParent(leafGuard *buffer.WritePageGuard, newLeafGuard *buffer.WritePageGuard, key K) error {
	leafPage := ToStruct[bplusInternalPage[K]](*leafGuard.GetDataMut())
	newLeafPage := ToStruct[bplusInternalPage[K]](*newLeafGuard.GetDataMut())
	leafParent := leafPage.Parent

	leafIsRoot := leafPage.PageId == b.header.RootPageId
	if leafIsRoot {
		newRootId := b.bpm.NewPageId()
		parentGuard, err := b.bpm.WritePage(newRootId)
		if err != nil {
			parentGuard.Drop()
			return err
		}

		newRootPage := ToStruct[bplusInternalPage[K]](*parentGuard.GetDataMut())
		newRootPage.init(newRootId, disk.INVALID_PAGE_ID)
		newRootPage.setKeyAt(1, key)
		newRootPage.setValAt(0, leafPage.PageId)
		newRootPage.setValAt(1, newLeafPage.PageId)
		newRootPage.Size = 2

		leafPage.Parent = newRootId
		newLeafPage.Parent = newRootId

		if err := b.setRootPageId(newRootId); err != nil {
			leafGuard.Drop()
			newLeafGuard.Drop()
			parentGuard.Drop()
			return err
		}

		data := newRootPage.toBytes()
		copy(*parentGuard.GetDataMut(), data)

		leafData := leafPage.toBytes()
		copy(*leafGuard.GetDataMut(), leafData)

		newLeafData := newLeafPage.toBytes()
		copy(*newLeafGuard.GetDataMut(), newLeafData)

		leafGuard.Drop()
		newLeafGuard.Drop()
		parentGuard.Drop()
	} else {
		leafGuard.Drop()
		parentGuard, err := b.bpm.WritePage(leafParent)

		if err != nil {
			newLeafGuard.Drop()
			parentGuard.Drop()
			return err
		}

		parentPage := ToStruct[bplusInternalPage[K]](*parentGuard.GetDataMut())

		if parentPage.Size < parentPage.MaxSize {
			parentPage.addKeyVal(key, newLeafPage.PageId)
			parentPage.Size += 1

			data := parentPage.toBytes()
			copy(*parentGuard.GetDataMut(), data)
			newLeafGuard.Drop()
			parentGuard.Drop()
		} else {
			insertIdx := parentPage.getInsertIdx(key)
			parentPage.Keys = slices.Insert(parentPage.Keys, insertIdx, key)
			parentPage.Values = slices.Insert(parentPage.Values, insertIdx, newLeafPage.PageId)

			tmpKeyArr := make([]K, parentPage.MaxSize+1)
			tmpValArr := make([]int64, parentPage.MaxSize+1)

			// copy values to tmp and zero out original arrays
			copy(tmpKeyArr, parentPage.Keys)
			copy(tmpValArr, parentPage.Values)
			parentPage.Keys = make([]K, parentPage.MaxSize)
			parentPage.Values = make([]int64, parentPage.MaxSize)

			pPrimeId := b.bpm.NewPageId()
			newLeafPage.Parent = pPrimeId

			newLeafData := newLeafPage.toBytes()
			copy(*newLeafGuard.GetDataMut(), newLeafData)
			newLeafGuard.Drop()

			pGuard, err := b.bpm.WritePage(pPrimeId)
			if err != nil {
				pGuard.Drop()
				parentGuard.Drop()
				newLeafGuard.Drop()
				return err
			}

			pPrime := ToStruct[bplusInternalPage[K]](*pGuard.GetDataMut())
			pPrime.init(pPrimeId, parentPage.Parent)

			midPoint := int(math.Ceil(float64(parentPage.MaxSize) / 2))

			copy(parentPage.Keys, tmpKeyArr[:midPoint])
			copy(parentPage.Values, tmpValArr[:midPoint])
			copy(pPrime.Keys[1:], tmpKeyArr[midPoint+1:])
			copy(pPrime.Values, tmpValArr[midPoint:])

			parentPage.Size = int32(midPoint)
			pPrime.Size = int32(parentPage.MaxSize-int32(midPoint)) + 1

			parentData := parentPage.toBytes()
			copy(*parentGuard.GetDataMut(), parentData)

			primeData := pPrime.toBytes()
			copy(*pGuard.GetDataMut(), primeData)

			if err := b.insertInParent(parentGuard, pGuard, tmpKeyArr[midPoint]); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *bplusTree[K, V]) findLeafPageId(rootPageId int64, key K) (int64, error) {
	currPageId := rootPageId

	for {
		guard, err := b.bpm.ReadPage(currPageId)
		if err != nil {
			guard.Drop()
			return 0, fmt.Errorf("error reading page: %v", err)
		}

		currPage := ToStruct[bplusInternalPage[K]](guard.GetData())
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

func (b *bplusTree[K, V]) isEmpty() bool {
	return b.header.RootPageId == disk.INVALID_PAGE_ID
}

func (b *bplusTree[K, V]) setRootPageId(pageId int64) error {
	b.header.RootPageId = pageId
	writeGuard, err := b.bpm.WritePage(HEADER_PAGE_ID)
	defer writeGuard.Drop()
	if err != nil {
		return fmt.Errorf("error setting rootPageId: %v", err)
	}

	data := b.header.toBytes()
	copy(*writeGuard.GetDataMut(), data)
	return nil
}

type bplusTree[K cmp.Ordered, V any] struct {
	bpm       *buffer.BufferpoolManager
	indexName string
	header    headerPage
}

type headerPage struct {
	RootPageId int64
}

func (h *headerPage) toBytes() []byte {
	res := make([]byte, 4096)
	bytes := unsafe.Slice((*byte)(unsafe.Pointer(h)), unsafe.Sizeof(*h))
	copy(res, bytes)

	return res
}
