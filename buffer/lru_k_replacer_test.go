package buffer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLrukReplacer(t *testing.T) {

	t.Run("test node addition", func(t *testing.T) {
		replacer := NewLrukReplacer(5, 5)

		replacer.addNode(&lrukNode{frameId: 1})
		replacer.addNode(&lrukNode{frameId: 2})
		replacer.addNode(&lrukNode{frameId: 3})

		assert.Equal(t, lruToArr(replacer.head.next), []int{3, 2, 1})
	})

	t.Run("test only evictable nodes are removed", func(t *testing.T) {
		replacer := NewLrukReplacer(5, 5)

		replacer.addNode(&lrukNode{frameId: 1})
		replacer.addNode(&lrukNode{frameId: 2, isEvictable: true})
		replacer.addNode(&lrukNode{frameId: 3})

		// this will return an error, 1 is not evictable
		err := replacer.remove(1)
		assert.Error(t, err)

		// this will work, 2 is evictable
		err = replacer.remove(2)
		assert.NoError(t, err)

		assert.Equal(t, lruToArr(replacer.head.next), []int{3, 1})

	})

	t.Run("accessing a node moves it to the front of the queue", func(t *testing.T) {
		replacer := NewLrukReplacer(5, 5)

		replacer.addNode(&lrukNode{frameId: 1, k: 5})
		replacer.addNode(&lrukNode{frameId: 2, k: 5})
		replacer.addNode(&lrukNode{frameId: 3, k: 5})
		assert.Equal(t, lruToArr(replacer.head.next), []int{3, 2, 1})

		replacer.recordAccess(1)
		assert.Equal(t, lruToArr(replacer.head.next), []int{1, 3, 2})
	})
}

func lruToArr(head *lrukNode) []int {
	res := []int{}

	for head.next != nil {
		res = append(res, head.frameId)
		head = head.next
	}

	fmt.Println("done with the loop", res)

	return res
}
