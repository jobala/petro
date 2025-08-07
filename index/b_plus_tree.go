package index

import (
	"cmp"
	"fmt"
	"math"
	"slices"

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
	headerPage, err := buffer.ToStruct[headerPage](*headerData)
	if err != nil {
		return nil, fmt.Errorf("error getting header page: %v", err)
	}

	headerPage.RootPageId = disk.INVALID_PAGE_ID
	*headerData, _ = buffer.ToByteSlice(headerPage)

	return &bplusTree[K, V]{
		indexName: name,
		bpm:       bpm,
		header:    headerPage,
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

	leafPage, err := buffer.ToStruct[bplusLeafPage[K, V]](guard.GetData())
	if err != nil {
		return nil, err
	}

	valIdx := leafPage.getInsertIdx(key)
	if valIdx < 0 || valIdx >= leafPage.getSize() {
		return nil, fmt.Errorf("key not found")
	}

	res = append(res, leafPage.valueAt(valIdx))
	return res, nil
}

func (b *bplusTree[K, V]) insert(key K, value V) (bool, error) {
	if b.isEmpty() {
		pageId := b.bpm.NewPageId()
		guard, err := b.bpm.WritePage(pageId)
		defer guard.Drop()
		if err != nil {
			return false, err
		}

		leafPage, err := buffer.ToStruct[bplusLeafPage[K, V]](*guard.GetDataMut())
		if err != nil {
			return false, err
		}

		leafPage.init(pageId, int64(INVALID_PAGE))
		leafPage.Size = 1
		leafPage.setKeyAt(0, key)
		leafPage.setValAt(0, value)

		data, err := buffer.ToByteSlice(leafPage)
		if err != nil {
			return false, err
		}
		copy(*guard.GetDataMut(), data)

		if err := b.setRootPageId(pageId); err != nil {
			return false, err
		}

		return true, nil
	} else {
		leafPageId, err := b.findLeafPageId(b.header.RootPageId, key)
		if err != nil {
			return false, err
		}

		guard, err := b.bpm.WritePage(leafPageId)
		defer guard.Drop()
		if err != nil {
			return false, err
		}

		leafPage, err := buffer.ToStruct[bplusLeafPage[K, V]](*guard.GetDataMut())
		if err != nil {

			return false, err
		}

		if leafPage.Size+1 < leafPage.MaxSize-1 {
			leafPage.addKeyVal(key, value)
			leafPage.Size += 1

			data, err := buffer.ToByteSlice(leafPage)
			if err != nil {
				return false, err
			}
			copy(*guard.GetDataMut(), data)
		} else {
			newLeafId := b.bpm.NewPageId()
			newGuard, err := b.bpm.WritePage(newLeafId)
			defer newGuard.Drop()
			if err != nil {
				return false, err
			}

			newLeafPage, err := buffer.ToStruct[bplusLeafPage[K, V]](*newGuard.GetDataMut())
			if err != nil {
				return false, err
			}
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
			newLeafPage.Size = int32(int(leafPage.MaxSize) - midPoint)

			leafData, err := buffer.ToByteSlice(leafPage)
			if err != nil {
				return false, err
			}
			copy(*guard.GetDataMut(), leafData)

			newLeafData, err := buffer.ToByteSlice(newLeafPage)
			if err != nil {
				return false, err
			}
			copy(*newGuard.GetDataMut(), newLeafData)

			if err := b.insertInParent(&leafPage.BplusPageHeader, &newLeafPage.BplusPageHeader, newLeafPage.keyAt(0)); err != nil {
				return false, err
			}

			// need to update the data in the frame here as well.
			// TODO: find a clean way of syncing struct state with frame data
			leafData, err = buffer.ToByteSlice(leafPage)
			if err != nil {
				return false, err
			}
			copy(*guard.GetDataMut(), leafData)

			newLeafData, err = buffer.ToByteSlice(newLeafPage)
			if err != nil {
				return false, err
			}
			copy(*newGuard.GetDataMut(), newLeafData)
			return true, nil
		}

	}

	return true, nil
}

func (b *bplusTree[K, V]) insertInParent(leafPage *BplusPageHeader, newLeafPage *BplusPageHeader, key K) error {
	leafIsRoot := leafPage.PageId == b.header.RootPageId
	if leafIsRoot {
		newRootId := b.bpm.NewPageId()
		guard, err := b.bpm.WritePage(newRootId)
		defer guard.Drop()
		if err != nil {
			return err
		}

		newRootPage, err := buffer.ToStruct[bplusInternalPage[K]](*guard.GetDataMut())
		if err != nil {
			return err
		}

		newRootPage.init(newRootId, disk.INVALID_PAGE_ID)
		newRootPage.setKeyAt(1, key)
		newRootPage.setValAt(0, leafPage.PageId)
		newRootPage.setValAt(1, newLeafPage.PageId)
		newRootPage.Size = 2

		leafPage.Parent = newRootId
		newLeafPage.Parent = newRootId

		if err := b.setRootPageId(newRootId); err != nil {
			return err
		}

		data, err := buffer.ToByteSlice(newRootPage)
		if err != nil {
			return err
		}
		copy(*guard.GetDataMut(), data)
	} else {
		guard, err := b.bpm.WritePage(leafPage.Parent)
		if err != nil {
			guard.Drop()
			return err
		}

		parentPage, err := buffer.ToStruct[bplusInternalPage[K]](*guard.GetDataMut())
		if err != nil {

			guard.Drop()
			return err
		}

		if parentPage.Size+1 < parentPage.MaxSize-1 {
			parentPage.addKeyVal(key, newLeafPage.PageId)
			parentPage.Size += 1

			data, err := buffer.ToByteSlice(parentPage)
			if err != nil {
				return err
			}
			copy(*guard.GetDataMut(), data)
			guard.Drop()
		} else {
			var tmpKeyArr []K
			var tmpValArr []int64

			// copy values to tmp and zero out original arrays
			copy(tmpKeyArr, parentPage.Keys[:])
			copy(tmpValArr, parentPage.Values[:])
			parentPage.Keys = []K{}
			parentPage.Values = []int64{}

			insertIdx := parentPage.getInsertIdx(key)
			tmpKeyArr = slices.Insert(tmpKeyArr, insertIdx, key)
			tmpValArr = slices.Insert(tmpValArr, insertIdx, newLeafPage.PageId)

			pPrimeId := b.bpm.NewPageId()
			pGuard, err := b.bpm.WritePage(pPrimeId)
			defer pGuard.Drop()
			if err != nil {
				return err
			}

			pPrime, err := buffer.ToStruct[bplusInternalPage[K]](*pGuard.GetDataMut())
			if err != nil {
				return err
			}
			pPrime.init(pPrimeId, parentPage.Parent)

			midPoint := int(math.Ceil(float64(parentPage.MaxSize) / 2))

			copy(parentPage.Keys[:], tmpKeyArr[:midPoint])
			copy(parentPage.Values[:], tmpValArr[:midPoint])
			copy(pPrime.Keys[:], tmpKeyArr[midPoint:])
			copy(pPrime.Values[:], tmpValArr[midPoint:])

			parentPage.Size = int32(midPoint)
			pPrime.Size = int32(len(tmpKeyArr) - midPoint)

			parentData, err := buffer.ToByteSlice(parentPage)
			if err != nil {
				return err
			}
			copy(*guard.GetDataMut(), parentData)

			primeData, err := buffer.ToByteSlice(pPrime)
			if err != nil {
				return err
			}
			copy(*guard.GetDataMut(), primeData)

			guard.Drop()
			pGuard.Drop()
			if err := b.insertInParent(&parentPage.BplusPageHeader, &pPrime.BplusPageHeader, tmpKeyArr[midPoint]); err != nil {
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

		currPage, err := buffer.ToStruct[bplusInternalPage[K]](guard.GetData())
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

func (b *bplusTree[K, V]) isEmpty() bool {
	return b.header.RootPageId == disk.INVALID_PAGE_ID
}

func (b *bplusTree[K, V]) setRootPageId(pageId int64) error {
	b.header.RootPageId = pageId
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

type bplusTree[K cmp.Ordered, V any] struct {
	bpm       *buffer.BufferpoolManager
	indexName string
	header    headerPage
}

type headerPage struct {
	RootPageId int64
}
