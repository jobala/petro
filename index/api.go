package index

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

func (b *bplusTree[K, V]) BatchInsert(items map[K]V) error {
	for k, v := range items {
		if _, err := b.Insert(k, v); err != nil {
			return err
		}
	}

	return nil
}
