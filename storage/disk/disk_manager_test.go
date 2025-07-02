package disk

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDiskManager(t *testing.T) {
	t.Run("test page allocation", func(t *testing.T) {
		dbFile := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(dbFile.Name())
		})

		dm := NewDiskManager(dbFile)
		offset1, err := dm.allocatePage()
		dm.pages[0] = offset1
		assert.NoError(t, err)

		offset2, err := dm.allocatePage()
		dm.pages[1] = offset2
		assert.NoError(t, err)

		assert.Equal(t, 0, offset1)
		assert.Equal(t, 4096, offset2)
	})

	t.Run("allocate reuses free slots", func(t *testing.T) {
		dbFile := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(dbFile.Name())
		})

		dm := NewDiskManager(dbFile)
		dm.freeSlots = []int{8192}

		offset, err := dm.allocatePage()
		assert.NoError(t, err)

		assert.Equal(t, 8192, offset)
		assert.Empty(t, dm.freeSlots)
	})

	t.Run("test db file gets resized when full", func(t *testing.T) {
		// creates a 4kb file
		dbFile := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(dbFile.Name())
		})

		dm := NewDiskManager(dbFile)
		dm.pageCapacity = 1
		dm.pages = map[int]int{
			0: 0,
		}

		offset, err := dm.allocatePage()
		assert.NoError(t, err)

		assert.Equal(t, 4096, offset)
		assert.Equal(t, 2, dm.pageCapacity)

		// dbFile is increased in size
		fileInfo, err := os.Stat(dbFile.Name())
		assert.NoError(t, err)
		assert.Equal(t, int64(PAGE_SIZE)*2, fileInfo.Size())
	})

	t.Run("test reading and writing a page", func(t *testing.T) {
		dbFile := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(dbFile.Name())
		})

		dm := NewDiskManager(dbFile)
		dm.pageCapacity = 1

		buf := make([]byte, PAGE_SIZE)
		copy(buf, []byte("hello world"))

		err := dm.writePage(1, buf)
		assert.NoError(t, err)

		res, err := dm.readPage(1)
		assert.NoError(t, err)

		assert.Equal(t, res, buf)

	})

	t.Run("test page deletion", func(t *testing.T) {
		dbFile := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(dbFile.Name())
		})

		dm := NewDiskManager(dbFile)
		dm.pageCapacity = 1
		dm.pages[1] = 0
		assert.Equal(t, len(dm.freeSlots), 0)

		dm.deletePage(1)
		assert.Equal(t, len(dm.freeSlots), 1)
	})
}

func CreateDbFile(t *testing.T) *os.File {
	t.Helper()
	dbFile := path.Join(t.TempDir(), "test.db")

	file, err := os.OpenFile(dbFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Sprintf("failed creating db file\n%v", err))
	}

	// create 4kb file
	_ = os.Truncate(file.Name(), PAGE_SIZE)
	fileInfo, err := os.Stat(file.Name())
	assert.NoError(t, err)
	assert.Equal(t, int64(PAGE_SIZE), fileInfo.Size())
	return file
}
