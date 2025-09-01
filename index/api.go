package index

import (
	"cmp"
	"os"

	"github.com/jobala/petro/buffer"
	"github.com/jobala/petro/storage/disk"
)

func New[K cmp.Ordered, V any](file *os.File) (*bplusTree[K, V], error) {
	replacer := buffer.NewLrukReplacer(buffer.BUFFER_CAPACITY, 2)
	diskMgr := disk.NewManager(file)
	diskScheduler := disk.NewScheduler(diskMgr)
	bpm := buffer.NewBufferpoolManager(buffer.BUFFER_CAPACITY, replacer, diskScheduler)

	return NewBplusTree[K, V]("default", bpm)
}

func (b *bplusTree[K, V]) GetIterator() *indexIterator[K, V] {
	return NewIndexIterator[K, V](b.firstPageId, b.bpm)
}

func (b *bplusTree[K, V]) GetKeyRange(start, stop K) ([]V, error) {
	indexIter := b.GetIterator()

	res := []V{}
	for !indexIter.IsEnd() {
		key, val, err := indexIter.Next()
		if err != nil {
			return res, err
		}
		if key >= start && key <= stop {
			res = append(res, val)
		}

		if key == stop {
			break
		}
	}

	return res, nil
}

func (b *bplusTree[K, V]) PutBatch(items map[K]V) error {
	for k, v := range items {
		if _, err := b.Put(k, v); err != nil {
			return err
		}
	}

	return nil
}
