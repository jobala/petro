package index

type PAGE_TYPE = int

const (
	INVALID_PAGE PAGE_TYPE = iota
	INTERNAL_PAGE
	LEAF_PAGE
)

const HEADER_PAGE_ID = 0

func (p *page) init(pageType PAGE_TYPE, pageId, parentPageId int64) {
	p.pageType = pageType
	p.pageId = pageId
	p.parent = parentPageId
	p.maxSize = 256 // todo: calculate max size
}

func (p *page) isLeafPage() bool {
	return p.pageType == LEAF_PAGE
}

func (p *page) keyAt(idx int) int {
	return p.keys[idx]
}

func (p *page) valueAt(idx int) int64 {
	return p.values[idx]
}

func (p *page) getSize() int {
	return int(p.size)
}

func (p *page) getInsertIdx(key int) int {
	left := 0
	right := p.getSize() - 1

	for left <= right {
		mid := left + (right-left)/2
		if mid < key {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return left
}

func (p *page) setKeyAt(idx, key int) {
	panic("setKeyAt is  not implemented")
}

func (p *page) setValAt(idx int, value int64) {
	panic("setValueAt is not  implemented")
}

func (p *page) addKeyVal(key int, val int64) {
	panic("addKeyVal is not implemented")
}

// todo: calculate available space
type page struct {
	header
	keys   [256]int
	values [256]int64
}

type header struct {
	size     int32
	maxSize  int32
	pageId   int64
	parent   int64
	pageType PAGE_TYPE
	next     int64
	prev     int64
}
