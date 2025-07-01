package disk

func newDiskManager(file string) *diskManager {
	return &diskManager{
		dbFile: file,
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

func (dm *diskManager) allocate() int {
	/*
		1. Check if we can get page from freePages
		2. If we can return the offset
		3. If we can't check if we are within database size limits
		4. If we are allocate pages
		5. If we are beyond database limits, resize database file and allocate new page
	*/
	return 0
}

type diskManager struct {
	dbFile    string
	pages     map[int]int
	freeSlots []int
}
