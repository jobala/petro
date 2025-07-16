package disk

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDiskManager(t *testing.T) {
	t.Run("test reading and writing a page", func(t *testing.T) {
		dbFile := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(dbFile.Name())
		})

		dm := NewManager(dbFile)

		buf := make([]byte, PAGE_SIZE)
		copy(buf, []byte("hello world"))

		err := dm.writePage(1, buf)
		assert.NoError(t, err)

		res, err := dm.readPage(1)
		assert.NoError(t, err)

		assert.Equal(t, res, buf)
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
