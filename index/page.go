package index

type PAGE_TYPE = int

const (
	INVALID_PAGE PAGE_TYPE = iota
	INTERNAL_PAGE
	LEAF_PAGE
)

const HEADER_PAGE_ID = 0

func newPage(pageType PAGE_TYPE, pageId, parentPageId int64) *page {
	return &page{
		header: header{
			pageId:   pageId,
			parent:   parentPageId,
			pageType: pageType,
			maxSize:  256, // todo: calculate max size based on page and header size
		},
	}
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

// todo: calculate available space
type page struct {
	header
	keys   [255]int
	values [255]int64
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
