package buffer

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/jobala/petro/storage/disk"
	"github.com/stretchr/testify/assert"
)

func TestBufferPoolManager(t *testing.T) {
	t.Run("reads a page from disk", func(t *testing.T) {
		file := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(file.Name())
		})

		replacer := NewLrukReplacer(5, 2)
		diskMgr := disk.NewManager(file)
		diskScheduler := disk.NewScheduler(diskMgr)
		bufferMgr := NewBufferpoolManager(5, replacer, diskScheduler)

		pageId := 1
		data := []byte("hello, world!")
		syncWrite(pageId, data, diskScheduler)

		res, _ := bufferMgr.ReadPage(int64(pageId))
		fmt.Println(string(res))
	})

	t.Run("supports concurrent readers", func(t *testing.T) {})
	t.Run("evicts least recently used page", func(t *testing.T) {})
	t.Run("writes a page to disk", func(t *testing.T) {})
	t.Run("in memory pages are flushed to disk before eviction", func(t *testing.T) {})
}

func CreateDbFile(t *testing.T) *os.File {
	t.Helper()
	dbFile := path.Join(t.TempDir(), "test.db")

	file, err := os.OpenFile(dbFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Sprintf("failed creating db file\n%v", err))
	}

	// create 4kb file
	_ = os.Truncate(file.Name(), disk.PAGE_SIZE)
	fileInfo, err := os.Stat(file.Name())
	assert.NoError(t, err)
	assert.Equal(t, int64(disk.PAGE_SIZE), fileInfo.Size())
	return file
}

func syncWrite(pageId int, data []byte, diskScheduler *disk.DiskScheduler) {
	resCh := make(chan disk.DiskResp)
	copy(data, []byte("hello world"))

	writeReq := disk.DiskReq{
		PageId: pageId,
		Write:  true,
		Data:   data,
		RespCh: resCh,
	}

	diskScheduler.Schedule(writeReq)
	<-resCh
}
