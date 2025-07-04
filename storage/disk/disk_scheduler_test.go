package disk

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDiskScheduler(t *testing.T) {
	t.Run("schedule is non blocking", func(t *testing.T) {
		file := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(file.Name())
		})

		diskMgr := NewManager(file)
		ds := NewScheduler(diskMgr)

		resCh := make(chan DiskResp)
		data := make([]byte, PAGE_SIZE)
		copy(data, []byte("hello world"))

		writeReq := DiskReq{
			PageId: 1,
			Write:  true,
			Data:   data,
			RespCh: resCh,
		}

		start := time.Now()
		ds.Schedule(writeReq)
		elapsed := time.Since(start)

		assert.Less(t, elapsed, time.Millisecond)
	})

	t.Run("can schedule read and write requests", func(t *testing.T) {
		file := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(file.Name())
		})

		diskMgr := NewManager(file)
		ds := NewScheduler(diskMgr)

		resCh := make(chan DiskResp)
		data := make([]byte, PAGE_SIZE)
		copy(data, []byte("hello world"))

		writeReq := DiskReq{
			PageId: 1,
			Write:  true,
			Data:   data,
			RespCh: resCh,
		}

		respCh := make(chan DiskResp)
		readReq := DiskReq{
			PageId: 1,
			Write:  false,
			RespCh: respCh,
		}

		ds.Schedule(writeReq)
		ds.Schedule(readReq)

		<-writeReq.RespCh
		res := <-readReq.RespCh
		assert.Equal(t, res.Data, data)

		time.Sleep(5 * time.Second)
	})

}
