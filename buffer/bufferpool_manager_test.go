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

		pageGuard, err := bufferMgr.ReadPage(int64(pageId))
		defer pageGuard.Drop()
		assert.NoError(t, err)

		assert.Equal(t, data, pageGuard.GetData())
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
			pageGuard, err := bufferMgr.ReadPage(int64(2))
			assert.NoError(t, err)
			pageGuard.Drop()
		}

		// access page 1 to make page 2 least recently used
		pageGuard, err := bufferMgr.ReadPage(int64(1))
		assert.NoError(t, err)
		pageGuard.Drop()

		// accessing page 3 should evict page 1
		for i := range len(content) {
			pageGuard, err := bufferMgr.ReadPage(int64(i + 1))

			assert.NoError(t, err)
			assert.Equal(t, string(bytes.Trim(pageGuard.GetData(), "\x00")), content[i])
			pageGuard.Drop()
		}

		// page id 1, should have been evicted
		assert.Equal(t, bufferMgr.frames[0].pageId, int64(2))
		assert.Equal(t, bufferMgr.frames[1].pageId, int64(3))

		// buffermanager's pagetable shouldn't have evicted pageId
		_, ok := bufferMgr.pageTable[1]
		assert.Equal(t, false, ok)
	})

	t.Run("writes a page to disk", func(t *testing.T) {
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

		pageGuard, err := bufferMgr.WritePage(int64(pageId))
		copy(*pageGuard.GetDataMut(), data)
		defer pageGuard.Drop()

		assert.NoError(t, err)
		assert.Equal(t, data, bufferMgr.frames[0].data)
		assert.True(t, bufferMgr.frames[0].dirty, true)

		bufferMgr.flush(bufferMgr.frames[0])
		res := syncRead(pageId, diskScheduler)
		assert.Equal(t, data, res)
	})

	t.Run("dirty evicted pages are flushed to disk", func(t *testing.T) {
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

			pageGuard, err := bufferMgr.WritePage(int64(pageId + 1))
			copy(*pageGuard.GetDataMut(), data)
			pageGuard.Drop()

			assert.NoError(t, err)
		}

		// page 1 should have been evicted and flushed to disk
		res := syncRead(1, diskScheduler)
		assert.Equal(t, content[0], string(bytes.Trim(res, "\x00")))
	})

	t.Run("can read and write", func(t *testing.T) {
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
			pageGuard, err := bufferMgr.WritePage(int64(pageId + 1))
			copy(*pageGuard.GetDataMut(), data)
			pageGuard.Drop()

			assert.NoError(t, err)
		}

		for pageId, data := range content {
			pageGuard, err := bufferMgr.ReadPage(int64(pageId + 1))
			pageGuard.Drop()

			assert.NoError(t, err)
			assert.Equal(t, data, string(bytes.Trim(pageGuard.GetData(), "\x00")))
		}
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

func syncRead(pageId int, diskScheduler *disk.DiskScheduler) []byte {
	readReq := disk.NewRequest(int64(pageId), nil, false)
	respCh := diskScheduler.Schedule(readReq)
	res := <-respCh

	return res.Data
}
