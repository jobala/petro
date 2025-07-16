package buffer

import (
	"bytes"
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
		data := make([]byte, disk.PAGE_SIZE)
		copy(data, []byte("hello, world!"))
		syncWrite(pageId, data, diskScheduler)

		res, err := bufferMgr.ReadPage(int64(pageId))
		assert.NoError(t, err)

		assert.Equal(t, data, res)
		assert.Equal(t, data, bufferMgr.frames[0].data)
	})

	t.Run("evicts least recently used page", func(t *testing.T) {
		file := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(file.Name())
		})

		replacer := NewLrukReplacer(2, 2)
		diskMgr := disk.NewManager(file)
		diskScheduler := disk.NewScheduler(diskMgr)
		bufferMgr := NewBufferpoolManager(2, replacer, diskScheduler)

		content := []string{"1", "2", "3"}
		for pageId, d := range content {
			data := make([]byte, disk.PAGE_SIZE)
			copy(data, []byte(d))
			syncWrite(pageId+1, data, diskScheduler)
		}

		// access page 2 many times
		for range 5 {
			_, err := bufferMgr.ReadPage(int64(2))
			assert.NoError(t, err)
		}

		// access page 1 to make page 2 least recently used
		_, err := bufferMgr.ReadPage(int64(1))
		assert.NoError(t, err)

		// accessing page 3 should evict page 1
		for i := range len(content) {
			res, err := bufferMgr.ReadPage(int64(i + 1))
			assert.NoError(t, err)
			assert.Equal(t, string(bytes.Trim(res, "\x00")), content[i])
		}

		// page id 1, should have been evicted
		assert.Equal(t, bufferMgr.frames[0].pageId, int64(2))
		assert.Equal(t, bufferMgr.frames[1].pageId, int64(3))
	})

	t.Run("supports concurrent readers", func(t *testing.T) {})
	t.Run("writes a page to disk", func(t *testing.T) {})
	t.Run("dirty pages are flushed to disk before eviction", func(t *testing.T) {})
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

	writeReq := disk.DiskReq{
		PageId: pageId,
		Write:  true,
		Data:   data,
		RespCh: resCh,
	}

	diskScheduler.Schedule(writeReq)
	<-resCh
}
