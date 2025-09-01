package index

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/jobala/petro/buffer"
	"github.com/jobala/petro/storage/disk"
	"github.com/stretchr/testify/assert"
)

func TestBPlusTree(t *testing.T) {
	t.Run("stored values can be retrieved", func(t *testing.T) {
		file := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(file.Name())
		})

		bpm := createBpm(file)
		bplus, err := NewBplusTree[string, int]("test", bpm)
		assert.NoError(t, err)

		register := map[string]int{
			"john": 25,
			"doe":  45,
			"jane": 40,
		}

		for k, v := range register {
			inserted, err := bplus.Put(k, v)
			assert.NoError(t, err)
			assert.True(t, inserted)
		}

		for k, v := range register {
			val, err := bplus.Get(k)

			assert.NoError(t, err)
			assert.Equal(t, v, val[0])
		}

	})

	t.Run("can store items larger than page's max size", func(t *testing.T) {
		file := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(file.Name())
		})

		bpm := createBpm(file)
		bplus, err := NewBplusTree[int, int]("test", bpm)
		assert.NoError(t, err)

		for i := 100; i >= 0; i-- {
			inserted, err := bplus.Put(i, i)
			assert.NoError(t, err)
			assert.True(t, inserted)
		}

		for i := range 100 {
			val, err := bplus.Get(i)
			if err != nil {
				fmt.Println(err)
			}

			assert.NoError(t, err)
			assert.Equal(t, i, val[0])
		}
	})

	t.Run("values are stored in order", func(t *testing.T) {
		file := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(file.Name())
		})

		bpm := createBpm(file)
		bplus, err := NewBplusTree[int, int]("test", bpm)
		assert.NoError(t, err)

		// insert values in reverse order
		for i := 100; i >= 0; i-- {
			inserted, err := bplus.Put(i, i)
			assert.NoError(t, err)
			assert.True(t, inserted)
		}

		// generate control-check that is in-order
		expected := []int{}
		for i := range 101 {
			expected = append(expected, i)
		}

		// retrieve stored values
		indexIter := bplus.GetIterator()
		res := []int{}
		for !indexIter.IsEnd() {
			_, val, err := indexIter.Next()
			assert.NoError(t, err)
			res = append(res, val)
		}

		assert.Equal(t, res, expected)
	})

	t.Run("test batch insert", func(t *testing.T) {
		file := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(file.Name())
		})

		bpm := createBpm(file)
		bplus, err := NewBplusTree[string, int]("test", bpm)
		assert.NoError(t, err)

		register := map[string]int{
			"john": 25,
			"doe":  45,
			"jane": 40,
		}

		err = bplus.PutBatch(register)
		assert.NoError(t, err)

		for k, v := range register {
			val, err := bplus.Get(k)

			assert.NoError(t, err)
			assert.Equal(t, v, val[0])
		}
	})

	t.Run("retrieve items within a range", func(t *testing.T) {
		file := CreateDbFile(t)
		t.Cleanup(func() {
			_ = os.Remove(file.Name())
		})

		bpm := createBpm(file)
		bplus, err := NewBplusTree[int, int]("test", bpm)
		assert.NoError(t, err)

		// insert values in reverse order
		for i := 100; i >= 0; i-- {
			inserted, err := bplus.Put(i, i)
			assert.NoError(t, err)
			assert.True(t, inserted)
		}

		// generate control-check that is in-order within a range
		expected := []int{}
		start := 30
		stop := 70
		for i := start; i <= stop; i++ {
			expected = append(expected, i)
		}

		res, err := bplus.GetKeyRange(start, stop)
		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})
}

func createBpm(file *os.File) *buffer.BufferpoolManager {
	replacer := buffer.NewLrukReplacer(5, 2)
	diskMgr := disk.NewManager(file)
	diskScheduler := disk.NewScheduler(diskMgr)

	return buffer.NewBufferpoolManager(5, replacer, diskScheduler)
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
