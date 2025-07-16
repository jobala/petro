package disk

import (
	"fmt"
	"os"
)

func NewManager(file *os.File) *diskManager {
	return &diskManager{
		dbFile: file,
	}
}

func (dm *diskManager) writePage(pageId int, data []byte) error {
	offset := int64(pageId * PAGE_SIZE)

	if _, err := dm.dbFile.WriteAt(data, offset); err != nil {
		return fmt.Errorf("error writing at offset %d: %v", offset, err)
	}

	return nil
}

func (dm *diskManager) readPage(pageId int) ([]byte, error) {
	offset := int64(pageId * PAGE_SIZE)

	buf := make([]byte, PAGE_SIZE)
	if _, err := dm.dbFile.ReadAt(buf, offset); err != nil {
		return nil, fmt.Errorf("error reading from offset %d: %v", offset, err)
	}

	return buf, nil
}

type diskManager struct {
	dbFile *os.File
}
