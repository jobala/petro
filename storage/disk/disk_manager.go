package disk

import (
	"fmt"
	"os"
)

func NewDiskManager(file *os.File) *diskManager {
	return &diskManager{
		dbFile:       file,
		pageCapacity: DEFAULT_PAGE_CAPACITY,
		freeSlots:    []int{},
		pages:        map[int]int{},
	}
}

func (dm *diskManager) writePage(pageId int, data []byte) (int, error) {
	return 0, nil
}

func (dm *diskManager) readPage(pageId int) (int, []byte) {
	return 0, []byte{}
}

func (dm *diskManager) deletePage(pageId int) {

}

func (dm *diskManager) allocate() (int, error) {
	if len(dm.freeSlots) > 0 {
		offset := dm.freeSlots[0]
		dm.freeSlots = dm.freeSlots[1:]

		return offset, nil
	}

	if len(dm.pages)+1 > dm.pageCapacity {
		dm.pageCapacity *= 2
		if err := os.Truncate(dm.dbFile.Name(), int64(dm.pageCapacity)*PAGE_SIZE); err != nil {
			return -1, fmt.Errorf("error resizing db file: %v", err)
		}
	}

	return dm.getNextOffset(), nil
}

func (dm *diskManager) getNextOffset() int {
	return len(dm.pages) * PAGE_SIZE
}

type diskManager struct {
	dbFile       *os.File
	pages        map[int]int
	freeSlots    []int
	pageCapacity int
}
