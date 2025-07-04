package disk

import (
	"fmt"
	"os"
)

func NewManager(file *os.File) *diskManager {
	return &diskManager{
		dbFile:       file,
		pageCapacity: DEFAULT_PAGE_CAPACITY,
		freeSlots:    []int{},
		pages:        map[int]int{},
	}
}

func (dm *diskManager) writePage(pageId int, data []byte) error {
	var offset int
	offset, pageFound := dm.pages[pageId]

	if !pageFound {
		offset, err := dm.allocatePage()
		if err != nil {
			return err
		}
		dm.pages[pageId] = offset
	}

	_, err := dm.dbFile.WriteAt(data, int64(offset))

	if err != nil {
		return fmt.Errorf("error writing at offset %d: %v", offset, err)
	}

	return nil
}

func (dm *diskManager) readPage(pageId int) ([]byte, error) {
	var offset int
	offset, pageFound := dm.pages[pageId]

	if !pageFound {
		offset, err := dm.allocatePage()
		if err != nil {
			return nil, err
		}
		dm.pages[pageId] = offset
	}

	buf := make([]byte, PAGE_SIZE)
	if _, err := dm.dbFile.ReadAt(buf, int64(offset)); err != nil {
		return nil, fmt.Errorf("error reading from offset %d: %v", offset, err)
	}

	return buf, nil
}

func (dm *diskManager) deletePage(pageId int) {
	if offset, ok := dm.pages[pageId]; ok {
		dm.freeSlots = append(dm.freeSlots, offset)
		delete(dm.pages, pageId)
	}
}

func (dm *diskManager) allocatePage() (int, error) {
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
