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

	*headerData, _ = buffer.ToByteSlice(headerPage)

	return &bplusTree[K, V]{
		indexName:   name,
		bpm:         bpm,
		header:      headerPage,
		firstPageId: 1,
	}, nil
}

func (b *bplusTree[K, V]) Get(key K) ([]V, error) {
	if b.isEmpty() {
		return nil, fmt.Errorf("store is empty")
	}

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
	if leafPage.Keys[valIdx] != key {
		return nil, fmt.Errorf("key not found: %v", key)
	}

	if leafPage.getSize() > 0 && valIdx < 0 || valIdx >= leafPage.getSize() {
		return nil, fmt.Errorf("key not found: %v", key)
	}

	res = append(res, leafPage.valueAt(valIdx))
	return res, nil
}

func (b *bplusTree[K, V]) Put(key K, value V) (bool, error) {
	if b.isEmpty() {
		pageId := b.bpm.NewPageId()
		guard, err := b.bpm.WritePage(pageId)
		if err != nil {
			guard.Drop()
			return false, err
		}

		leafPage, err := buffer.ToStruct[bplusLeafPage[K, V]](*guard.GetDataMut())
		if err != nil {
			guard.Drop()
			return false, err
		}

		leafPage.init(pageId, int64(INVALID_PAGE))
		leafPage.Size = 1
		leafPage.setKeyAt(0, key)
		leafPage.setValAt(0, value)

		data, err := buffer.ToByteSlice(leafPage)
		if err != nil {
			guard.Drop()
			return false, err
		}
		copy(*guard.GetDataMut(), data)

		if err := b.setRootPageId(pageId); err != nil {
			guard.Drop()
			return false, err
		}

		// used by iterator
		b.firstPageId = leafPage.PageId

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

		leafPage, err := buffer.ToStruct[bplusLeafPage[K, V]](*guard.GetDataMut())
		if err != nil {
			guard.Drop()
			return false, err
		}

		if leafPage.Size < leafPage.MaxSize {
			leafPage.addKeyVal(key, value)
			leafPage.Size += 1

			data, err := buffer.ToByteSlice(leafPage)
			if err != nil {
				guard.Drop()
				return false, err
			}
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
			newLeafPage, err := buffer.ToStruct[bplusLeafPage[K, V]](*newGuard.GetDataMut())
			if err != nil {
				guard.Drop()
				newGuard.Drop()
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
			leafPage.Next = newLeafId
			newLeafPage.Next = tmpNexPage

			// newLeafPage.Prev = leafPage.PageId

			midPoint := int(math.Ceil(float64(leafPage.MaxSize) / 2))

			// distribute values between leaf and new leaf
			copy(leafPage.Keys, tmpKeyArr[:midPoint])
			copy(leafPage.Values, tmpValArr[:midPoint])
			copy(newLeafPage.Keys, tmpKeyArr[midPoint:])
			copy(newLeafPage.Values, tmpValArr[midPoint:])

			leafPage.Size = int32(midPoint)
			newLeafPage.Size = int32(int(leafPage.MaxSize)-midPoint) + 1

			leafData, err := buffer.ToByteSlice(leafPage)
			if err != nil {
				guard.Drop()
				newGuard.Drop()
				return false, err
			}
			copy(*guard.GetDataMut(), leafData)

			newLeafData, err := buffer.ToByteSlice(newLeafPage)
			if err != nil {
				guard.Drop()
				newGuard.Drop()
				return false, err
			}
			copy(*newGuard.GetDataMut(), newLeafData)

			if err := b.insertInParent(guard, newGuard, newLeafPage.keyAt(0)); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}
func (b *bplusTree[K, V]) insertInParent(leafGuard *buffer.WritePageGuard, newLeafGuard *buffer.WritePageGuard, key K) error {
	leafPage, _ := buffer.ToStruct[bplusInternalPage[K]](*leafGuard.GetDataMut())
	newLeafPage, _ := buffer.ToStruct[bplusInternalPage[K]](*newLeafGuard.GetDataMut())
	leafParent := leafPage.Parent

	leafIsRoot := leafPage.PageId == b.header.RootPageId
	if leafIsRoot {
		newRootId := b.bpm.NewPageId()
		parentGuard, err := b.bpm.WritePage(newRootId)
		if err != nil {
			parentGuard.Drop()
			return err
		}

		newRootPage, err := buffer.ToStruct[bplusInternalPage[K]](*parentGuard.GetDataMut())
		if err != nil {
			parentGuard.Drop()
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
			leafGuard.Drop()
			newLeafGuard.Drop()
			parentGuard.Drop()
			return err
		}

		data, err := buffer.ToByteSlice(newRootPage)
		if err != nil {
			leafGuard.Drop()
			newLeafGuard.Drop()
			parentGuard.Drop()
			return err
		}
		copy(*parentGuard.GetDataMut(), data)

		leafData, err := buffer.ToByteSlice(leafPage)
		if err != nil {
			leafGuard.Drop()
			newLeafGuard.Drop()
			parentGuard.Drop()

			return err
		}
		copy(*leafGuard.GetDataMut(), leafData)

		newLeafData, err := buffer.ToByteSlice(newLeafPage)
		if err != nil {
			leafGuard.Drop()
			newLeafGuard.Drop()
			parentGuard.Drop()
			return err
		}
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

		parentPage, err := buffer.ToStruct[bplusInternalPage[K]](*parentGuard.GetDataMut())
		if err != nil {
			newLeafGuard.Drop()
			parentGuard.Drop()
			return err
		}

		if parentPage.Size < parentPage.MaxSize {
			parentPage.addKeyVal(key, newLeafPage.PageId)
			parentPage.Size += 1

			data, err := buffer.ToByteSlice(parentPage)
			if err != nil {
				newLeafGuard.Drop()
				parentGuard.Drop()
				return err
			}

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

			newLeafData, err := buffer.ToByteSlice(newLeafPage)
			if err != nil {
				parentGuard.Drop()
				newLeafGuard.Drop()
				return err
			}
			copy(*newLeafGuard.GetDataMut(), newLeafData)
			newLeafGuard.Drop()

			pGuard, err := b.bpm.WritePage(pPrimeId)
			if err != nil {
				pGuard.Drop()
				parentGuard.Drop()
				newLeafGuard.Drop()
				return err
			}

			pPrime, err := buffer.ToStruct[bplusInternalPage[K]](*pGuard.GetDataMut())
			if err != nil {
				pGuard.Drop()
				parentGuard.Drop()
				newLeafGuard.Drop()
				return err
			}
			pPrime.init(pPrimeId, parentPage.Parent)

			midPoint := int(math.Ceil(float64(parentPage.MaxSize) / 2))

			copy(parentPage.Keys, tmpKeyArr[:midPoint])
			copy(parentPage.Values, tmpValArr[:midPoint])
			copy(pPrime.Keys[1:], tmpKeyArr[midPoint+1:])
			copy(pPrime.Values, tmpValArr[midPoint:])

			parentPage.Size = int32(midPoint)
			pPrime.Size = int32(parentPage.MaxSize-int32(midPoint)) + 1

			parentData, err := buffer.ToByteSlice(parentPage)
			if err != nil {
				pGuard.Drop()
				parentGuard.Drop()
				newLeafGuard.Drop()
				return err
			}
			copy(*parentGuard.GetDataMut(), parentData)

			primeData, err := buffer.ToByteSlice(pPrime)
			if err != nil {
				pGuard.Drop()
				parentGuard.Drop()
				return err
			}
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

func (b *bplusTree[K, V]) Delete(key K) (bool, error) {
	if b.isEmpty() {
		return false, fmt.Errorf("store is empty")
	}

	leafId, err := b.findLeafPageId(b.header.RootPageId, key)
	if err != nil {
		return false, err
	}

	leafGuard, err := b.bpm.WritePage(leafId)
	if err != nil {
		return false, err
	}
	leafPage, err := buffer.ToStruct[bplusLeafPage[K, V]](*leafGuard.GetDataMut())
	if err != nil {
		leafGuard.Drop()
		return false, err
	}

	pos := -1
	for i := 0; i < int(leafPage.Size); i++ {
		if leafPage.keyAt(i) == key {
			pos = i
			break
		}
	}
	if pos == -1 {
		leafGuard.Drop()
		return false, fmt.Errorf("key not found: %v", key)
	}

	leafPage.Keys = slices.Delete(leafPage.Keys, pos, pos+1)
	leafPage.Values = slices.Delete(leafPage.Values, pos, pos+1)
	leafPage.Size--

	{
		data, err := buffer.ToByteSlice(leafPage)
		if err != nil {
			leafGuard.Drop()
			return false, err
		}
		copy(*leafGuard.GetDataMut(), data)
	}

	if leafPage.PageId == b.header.RootPageId {
		if leafPage.Size == 0 {
			leafGuard.Drop()
			if err := b.setRootPageId(disk.INVALID_PAGE_ID); err != nil {
				return false, err
			}
			return true, nil
		}
		leafGuard.Drop()
		return true, nil
	}

	minLeaf := int32(math.Ceil(float64(leafPage.MaxSize) / 2))
	if leafPage.Size >= minLeaf {
		leafGuard.Drop()
		return true, nil
	}

	parentId := leafPage.Parent
	leafGuard.Drop()
	parentGuard, err := b.bpm.WritePage(parentId)
	if err != nil {
		return false, err
	}
	parentPage, err := buffer.ToStruct[bplusInternalPage[K]](*parentGuard.GetDataMut())
	if err != nil {
		parentGuard.Drop()
		return false, err
	}

	childIdx := -1
	for i := 0; i < int(parentPage.Size); i++ {
		if parentPage.valueAt(i) == leafId {
			childIdx = i
			break
		}
	}
	if childIdx == -1 {
		parentGuard.Drop()
		return false, fmt.Errorf("leaf %d not found in parent %d", leafId, parentId)
	}

	loadLeaf := func(id int64) (*buffer.WritePageGuard, *bplusLeafPage[K, V], error) {
		g, err := b.bpm.WritePage(id)
		if err != nil {
			return nil, nil, err
		}
		lp, err := buffer.ToStruct[bplusLeafPage[K, V]](*g.GetDataMut())
		if err != nil {
			g.Drop()
			return nil, nil, err
		}
		return g, &lp, nil
	}

	leftId := int64(INVALID_PAGE)
	rightId := int64(INVALID_PAGE)
	if childIdx-1 >= 0 {
		leftId = parentPage.valueAt(childIdx - 1)
	}
	if childIdx+1 < int(parentPage.Size) {
		rightId = parentPage.valueAt(childIdx + 1)
	}

	tryBorrowOrMerge := func(borrowLeft bool) (bool, error) {
		var sibId int64
		var sepKeyIdx int
		if borrowLeft {
			if leftId == int64(INVALID_PAGE) {
				return false, nil
			}
			sibId = leftId
			sepKeyIdx = childIdx
		} else {
			if rightId == int64(INVALID_PAGE) {
				return false, nil
			}
			sibId = rightId
			sepKeyIdx = childIdx + 1
		}

		firstId, secondId := leafId, sibId
		firstIsLeaf := true
		if secondId < firstId {
			firstId, secondId = secondId, firstId
			firstIsLeaf = false
		}

		firstGuard, firstPage, err := loadLeaf(firstId)
		if err != nil {
			return false, err
		}
		secondGuard, secondPage, err := loadLeaf(secondId)
		if err != nil {
			firstGuard.Drop()
			return false, err
		}

		var leafG, sibG *buffer.WritePageGuard
		var leafP, sibP *bplusLeafPage[K, V]
		if firstIsLeaf {
			leafG, leafP = firstGuard, firstPage
			sibG, sibP = secondGuard, secondPage
		} else {
			sibG, sibP = firstGuard, firstPage
			leafG, leafP = secondGuard, secondPage
		}

		minL := int32(math.Ceil(float64(leafP.MaxSize) / 2))
		if leafP.Size >= minL {
			leafData, _ := buffer.ToByteSlice(leafP)
			copy(*leafG.GetDataMut(), leafData)
			leafG.Drop()
			sibG.Drop()
			return true, nil
		}

		minSib := int32(math.Ceil(float64(sibP.MaxSize) / 2))
		if sibP.Size > minSib {
			if borrowLeft {
				k := sibP.keyAt(int(sibP.Size) - 1)
				v := sibP.valueAt(int(sibP.Size) - 1)
				sibP.Keys = sibP.Keys[:sibP.Size-1]
				sibP.Values = sibP.Values[:sibP.Size-1]
				sibP.Size--

				leafP.Keys = slices.Insert(leafP.Keys, 0, k)
				leafP.Values = slices.Insert(leafP.Values, 0, v)
				leafP.Size++

				if sepKeyIdx >= 1 && sepKeyIdx < int(parentPage.Size) {
					parentPage.setKeyAt(sepKeyIdx, leafP.keyAt(0))
				}
			} else {
				k := sibP.keyAt(0)
				v := sibP.valueAt(0)
				sibP.Keys = sibP.Keys[1:]
				sibP.Values = sibP.Values[1:]
				sibP.Size--

				leafP.Keys = append(leafP.Keys[:leafP.Size], k)
				leafP.Values = append(leafP.Values[:leafP.Size], v)
				leafP.Size++

				if sepKeyIdx >= 1 && sepKeyIdx < int(parentPage.Size) && sibP.Size > 0 {
					parentPage.setKeyAt(sepKeyIdx, sibP.keyAt(0))
				}
			}

			if d, err := buffer.ToByteSlice(leafP); err == nil {
				copy(*leafG.GetDataMut(), d)
			} else {
				leafG.Drop()
				sibG.Drop()
				return false, err
			}
			if d, err := buffer.ToByteSlice(sibP); err == nil {
				copy(*sibG.GetDataMut(), d)
			} else {
				leafG.Drop()
				sibG.Drop()
				return false, err
			}
			if d, err := buffer.ToByteSlice(parentPage); err == nil {
				copy(*parentGuard.GetDataMut(), d)
			} else {
				leafG.Drop()
				sibG.Drop()
				return false, err
			}

			leafG.Drop()
			sibG.Drop()
			return true, nil
		}

		if borrowLeft {
			sibP.Keys = append(sibP.Keys[:sibP.Size], leafP.Keys[:leafP.Size]...)
			sibP.Values = append(sibP.Values[:sibP.Size], leafP.Values[:leafP.Size]...)
			sibP.Size += leafP.Size
			sibP.Next = leafP.Next

			parentPage.Keys = slices.Delete(parentPage.Keys, childIdx, childIdx+1)
			parentPage.Values = slices.Delete(parentPage.Values, childIdx, childIdx+1)
			parentPage.Size--

			if d, err := buffer.ToByteSlice(sibP); err == nil {
				copy(*sibG.GetDataMut(), d)
			} else {
				leafG.Drop()
				sibG.Drop()
				return false, err
			}
			if d, err := buffer.ToByteSlice(parentPage); err == nil {
				copy(*parentGuard.GetDataMut(), d)
			} else {
				leafG.Drop()
				sibG.Drop()
				return false, err
			}

			leafG.Drop()
			sibG.Drop()

			return true, b.fixInternalAfterDelete(parentGuard)
		} else {
			leafP.Keys = append(leafP.Keys[:leafP.Size], sibP.Keys[:sibP.Size]...)
			leafP.Values = append(leafP.Values[:leafP.Size], sibP.Values[:sibP.Size]...)
			leafP.Size += sibP.Size
			leafP.Next = sibP.Next

			parentPage.Keys = slices.Delete(parentPage.Keys, childIdx+1, childIdx+2)
			parentPage.Values = slices.Delete(parentPage.Values, childIdx+1, childIdx+2)
			parentPage.Size--

			if d, err := buffer.ToByteSlice(leafP); err == nil {
				copy(*leafG.GetDataMut(), d)
			} else {
				leafG.Drop()
				sibG.Drop()
				return false, err
			}
			if d, err := buffer.ToByteSlice(parentPage); err == nil {
				copy(*parentGuard.GetDataMut(), d)
			} else {
				leafG.Drop()
				sibG.Drop()
				return false, err
			}

			leafG.Drop()
			sibG.Drop()

			return true, b.fixInternalAfterDelete(parentGuard)
		}
	}

	done, err := tryBorrowOrMerge(true)
	if err != nil {
		parentGuard.Drop()
		return false, err
	}
	if done {
		parentGuard.Drop()
		return true, nil
	}

	done, err = tryBorrowOrMerge(false)
	if err != nil {
		parentGuard.Drop()
		return false, err
	}
	parentGuard.Drop()
	return done, nil
}

func (b *bplusTree[K, V]) fixInternalAfterDelete(parentGuard *buffer.WritePageGuard) error {
	parentPage, err := buffer.ToStruct[bplusInternalPage[K]](*parentGuard.GetDataMut())
	if err != nil {
		return err
	}

	if parentPage.PageId == b.header.RootPageId {
		if parentPage.Size == 1 {
			onlyChild := parentPage.valueAt(0)
			if err := b.setRootPageId(onlyChild); err != nil {
				return err
			}
			childGuard, err := b.bpm.WritePage(onlyChild)
			if err != nil {
				return err
			}
			childInternal, _ := buffer.ToStruct[bplusInternalPage[K]](*childGuard.GetDataMut())
			if childInternal.isLeafPage() {
				childLeaf, _ := buffer.ToStruct[bplusLeafPage[K, any]](*childGuard.GetDataMut())
				childLeaf.Parent = disk.INVALID_PAGE_ID
				if d, err := buffer.ToByteSlice(childLeaf); err == nil {
					copy(*childGuard.GetDataMut(), d)
				} else {
					childGuard.Drop()
					return err
				}
			} else {
				childInternal.Parent = disk.INVALID_PAGE_ID
				if d, err := buffer.ToByteSlice(childInternal); err == nil {
					copy(*childGuard.GetDataMut(), d)
				} else {
					childGuard.Drop()
					return err
				}
			}
			childGuard.Drop()
		}
		return nil
	}

	minInternal := int32(math.Ceil(float64(parentPage.MaxSize) / 2))
	if parentPage.Size >= minInternal {
		return nil
	}

	grandId := parentPage.Parent
	parentId := parentPage.PageId

	parentGuard.Drop()

	grandGuard, err := b.bpm.WritePage(grandId)
	if err != nil {
		return err
	}
	grandPage, err := buffer.ToStruct[bplusInternalPage[K]](*grandGuard.GetDataMut())
	if err != nil {
		grandGuard.Drop()
		return err
	}

	idx := -1
	for i := 0; i < int(grandPage.Size); i++ {
		if grandPage.valueAt(i) == parentId {
			idx = i
			break
		}
	}
	if idx == -1 {
		grandGuard.Drop()
		return fmt.Errorf("parent %d not found in grandparent %d", parentId, grandId)
	}

	leftId := int64(INVALID_PAGE)
	rightId := int64(INVALID_PAGE)
	if idx-1 >= 0 {
		leftId = grandPage.valueAt(idx - 1)
	}
	if idx+1 < int(grandPage.Size) {
		rightId = grandPage.valueAt(idx + 1)
	}

	loadInternal := func(id int64) (*buffer.WritePageGuard, *bplusInternalPage[K], error) {
		g, err := b.bpm.WritePage(id)
		if err != nil {
			return nil, nil, err
		}
		p, err := buffer.ToStruct[bplusInternalPage[K]](*g.GetDataMut())
		if err != nil {
			g.Drop()
			return nil, nil, err
		}
		return g, &p, nil
	}

	if leftId != int64(INVALID_PAGE) {
		firstId, secondId := parentId, leftId
		firstIsParent := true
		if secondId < firstId {
			firstId, secondId = secondId, firstId
			firstIsParent = false
		}

		firstG, firstP, err := loadInternal(firstId)
		if err != nil {
			grandGuard.Drop()
			return err
		}
		secondG, secondP, err := loadInternal(secondId)
		if err != nil {
			firstG.Drop()
			grandGuard.Drop()
			return err
		}

		var parG, sibG *buffer.WritePageGuard
		var parP, sibP *bplusInternalPage[K]
		if firstIsParent {
			parG, parP = firstG, firstP
			sibG, sibP = secondG, secondP
		} else {
			sibG, sibP = firstG, firstP
			parG, parP = secondG, secondP
		}

		minSib := int32(math.Ceil(float64(sibP.MaxSize) / 2))
		if sibP.Size > minSib {
			sepKeyIdx := idx
			sepKey := grandPage.keyAt(sepKeyIdx)

			movePtr := sibP.valueAt(int(sibP.Size - 1))
			sibP.Values = sibP.Values[:sibP.Size-1]
			lastKeyOfSib := sibP.keyAt(int(sibP.Size - 0))
			sibP.Keys = sibP.Keys[:int(sibP.Size)]
			sibP.Size--

			parP.Values = slices.Insert(parP.Values, 0, movePtr)
			parP.Keys = slices.Insert(parP.Keys, 1, sepKey)
			parP.Size++

			grandPage.setKeyAt(sepKeyIdx, lastKeyOfSib)

			childG, err := b.bpm.WritePage(movePtr)
			if err == nil {
				ci, _ := buffer.ToStruct[bplusInternalPage[K]](*childG.GetDataMut())
				if ci.isLeafPage() {
					cl, _ := buffer.ToStruct[bplusLeafPage[K, any]](*childG.GetDataMut())
					cl.Parent = parP.PageId
					if d, e := buffer.ToByteSlice(cl); e == nil {
						copy(*childG.GetDataMut(), d)
					}
				} else {
					ci.Parent = parP.PageId
					if d, e := buffer.ToByteSlice(ci); e == nil {
						copy(*childG.GetDataMut(), d)
					}
				}
				childG.Drop()
			}

			if d, e := buffer.ToByteSlice(sibP); e == nil {
				copy(*sibG.GetDataMut(), d)
			}
			if d, e := buffer.ToByteSlice(parP); e == nil {
				copy(*parG.GetDataMut(), d)
			}
			if d, e := buffer.ToByteSlice(grandPage); e == nil {
				copy(*grandGuard.GetDataMut(), d)
			}

			sibG.Drop()
			parG.Drop()
			grandGuard.Drop()
			return nil
		}

		sepKeyIdx := idx
		sepKey := grandPage.keyAt(sepKeyIdx)

		if int(sibP.Size) < len(sibP.Keys) {
			sibP.setKeyAt(int(sibP.Size), sepKey)
		} else {
			sibP.Keys = append(sibP.Keys, sepKey)
		}
		sibP.Values = append(sibP.Values[:sibP.Size], parP.Values[:parP.Size]...)
		if parP.Size > 1 {
			sibP.Keys = append(sibP.Keys[:int(sibP.Size)+1], parP.Keys[1:int(parP.Size)]...)
		}
		oldSize := sibP.Size
		sibP.Size = sibP.Size + parP.Size

		for i := int(oldSize); i < int(sibP.Size); i++ {
			ptr := sibP.valueAt(i)
			childG, err := b.bpm.WritePage(ptr)
			if err == nil {
				ci, _ := buffer.ToStruct[bplusInternalPage[K]](*childG.GetDataMut())
				if ci.isLeafPage() {
					cl, _ := buffer.ToStruct[bplusLeafPage[K, any]](*childG.GetDataMut())
					cl.Parent = sibP.PageId
					if d, e := buffer.ToByteSlice(cl); e == nil {
						copy(*childG.GetDataMut(), d)
					}
				} else {
					ci.Parent = sibP.PageId
					if d, e := buffer.ToByteSlice(ci); e == nil {
						copy(*childG.GetDataMut(), d)
					}
				}
				childG.Drop()
			}
		}

		grandPage.Keys = slices.Delete(grandPage.Keys, sepKeyIdx, sepKeyIdx+1)
		grandPage.Values = slices.Delete(grandPage.Values, idx, idx+1)
		grandPage.Size--

		if d, e := buffer.ToByteSlice(sibP); e == nil {
			copy(*sibG.GetDataMut(), d)
		}
		if d, e := buffer.ToByteSlice(grandPage); e == nil {
			copy(*grandGuard.GetDataMut(), d)
		}
		sibG.Drop()
		parG.Drop()

		err = b.fixInternalAfterDelete(grandGuard)
		grandGuard.Drop()
		return err
	}

	if rightId != int64(INVALID_PAGE) {
		firstId, secondId := parentId, rightId
		firstIsParent := true
		if secondId < firstId {
			firstId, secondId = secondId, firstId
			firstIsParent = false
		}

		firstG, firstP, err := loadInternal(firstId)
		if err != nil {
			grandGuard.Drop()
			return err
		}
		secondG, secondP, err := loadInternal(secondId)
		if err != nil {
			firstG.Drop()
			grandGuard.Drop()
			return err
		}

		var parG, sibG *buffer.WritePageGuard
		var parP, sibP *bplusInternalPage[K]
		if firstIsParent {
			parG, parP = firstG, firstP
			sibG, sibP = secondG, secondP
		} else {
			sibG, sibP = firstG, firstP
			parG, parP = secondG, secondP
		}

		minSib := int32(math.Ceil(float64(sibP.MaxSize) / 2))
		if sibP.Size > minSib {
			sepKeyIdx := idx + 1
			sepKey := grandPage.keyAt(sepKeyIdx)

			movePtr := sibP.valueAt(0)
			var sibFirstKey K
			if sibP.Size > 1 {
				sibFirstKey = sibP.keyAt(1)
			}

			sibP.Values = sibP.Values[1:]
			if int(sibP.Size) > 1 {
				sibP.Keys = append(sibP.Keys[:1], sibP.Keys[2:int(sibP.Size)]...)
			}
			sibP.Size--

			parP.Values = append(parP.Values[:parP.Size], movePtr)
			parP.Keys = append(parP.Keys[:int(parP.Size)], sepKey)
			parP.Size++

			if sibP.Size > 0 {
				grandPage.setKeyAt(sepKeyIdx, sibFirstKey)
			}

			childG, err := b.bpm.WritePage(movePtr)
			if err == nil {
				ci, _ := buffer.ToStruct[bplusInternalPage[K]](*childG.GetDataMut())
				if ci.isLeafPage() {
					cl, _ := buffer.ToStruct[bplusLeafPage[K, any]](*childG.GetDataMut())
					cl.Parent = parP.PageId
					if d, e := buffer.ToByteSlice(cl); e == nil {
						copy(*childG.GetDataMut(), d)
					}
				} else {
					ci.Parent = parP.PageId
					if d, e := buffer.ToByteSlice(ci); e == nil {
						copy(*childG.GetDataMut(), d)
					}
				}
				childG.Drop()
			}

			if d, e := buffer.ToByteSlice(sibP); e == nil {
				copy(*sibG.GetDataMut(), d)
			}
			if d, e := buffer.ToByteSlice(parP); e == nil {
				copy(*parG.GetDataMut(), d)
			}
			if d, e := buffer.ToByteSlice(grandPage); e == nil {
				copy(*grandGuard.GetDataMut(), d)
			}

			sibG.Drop()
			parG.Drop()
			grandGuard.Drop()
			return nil
		}

		sepKeyIdx := idx + 1
		sepKey := grandPage.keyAt(sepKeyIdx)

		if int(parP.Size) < len(parP.Keys) {
			parP.setKeyAt(int(parP.Size), sepKey)
		} else {
			parP.Keys = append(parP.Keys, sepKey)
		}
		parP.Values = append(parP.Values[:parP.Size], sibP.Values[:sibP.Size]...)
		if sibP.Size > 1 {
			parP.Keys = append(parP.Keys[:int(parP.Size)+1], sibP.Keys[1:int(sibP.Size)]...)
		}
		oldSize := parP.Size
		parP.Size = parP.Size + sibP.Size

		for i := int(oldSize); i < int(parP.Size); i++ {
			ptr := parP.valueAt(i)
			childG, err := b.bpm.WritePage(ptr)
			if err == nil {
				ci, _ := buffer.ToStruct[bplusInternalPage[K]](*childG.GetDataMut())
				if ci.isLeafPage() {
					cl, _ := buffer.ToStruct[bplusLeafPage[K, any]](*childG.GetDataMut())
					cl.Parent = parP.PageId
					if d, e := buffer.ToByteSlice(cl); e == nil {
						copy(*childG.GetDataMut(), d)
					}
				} else {
					ci.Parent = parP.PageId
					if d, e := buffer.ToByteSlice(ci); e == nil {
						copy(*childG.GetDataMut(), d)
					}
				}
				childG.Drop()
			}
		}

		grandPage.Keys = slices.Delete(grandPage.Keys, sepKeyIdx, sepKeyIdx+1)
		grandPage.Values = slices.Delete(grandPage.Values, idx+1, idx+2)
		grandPage.Size--

		if d, e := buffer.ToByteSlice(parP); e == nil {
			copy(*parG.GetDataMut(), d)
		}
		if d, e := buffer.ToByteSlice(grandPage); e == nil {
			copy(*grandGuard.GetDataMut(), d)
		}

		sibG.Drop()
		parG.Drop()

		err = b.fixInternalAfterDelete(grandGuard)
		grandGuard.Drop()
		return err
	}

	grandGuard.Drop()
	return nil
}

func (b *bplusTree[K, V]) isEmpty() bool {
	// TODO: use appropriate variable name
	return b.header.RootPageId == 0
}

func (b *bplusTree[K, V]) Flush() {
	b.bpm.FlushAll()
}

func (b *bplusTree[K, V]) setRootPageId(pageId int64) error {
	b.header.RootPageId = pageId
	writeGuard, err := b.bpm.WritePage(HEADER_PAGE_ID)
	defer writeGuard.Drop()
	if err != nil {
		return fmt.Errorf("error setting rootPageId: %v", err)
	}

	data, err := buffer.ToByteSlice(b.header)
	if err != nil {
		return fmt.Errorf("error converting header struct to byteslice: %v", err)
	}

	copy(*writeGuard.GetDataMut(), data)
	return nil
}

type bplusTree[K cmp.Ordered, V any] struct {
	bpm         *buffer.BufferpoolManager
	indexName   string
	header      headerPage
	firstPageId int64
}

type headerPage struct {
	RootPageId int64
	/* TODO: track the following
	1. first leaf page
	2. last issued paged id
	*/
}
