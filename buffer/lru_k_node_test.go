package buffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLrukNode(t *testing.T) {
	t.Run("returns true if has k access", func(t *testing.T) {
		node := &lrukNode{k: 3}
		assert.False(t, node.hasKAccess())

		node.addTimestamp(1)
		node.addTimestamp(2)
		node.addTimestamp(3)

		assert.True(t, node.hasKAccess())
	})

	t.Run("records timestamp", func(t *testing.T) {
		node := &lrukNode{k: 3}

		node.addTimestamp(1)
		node.addTimestamp(2)
		node.addTimestamp(3)
		assert.Equal(t, node.history, []int{1, 2, 3})

		node.addTimestamp(4)
		assert.Equal(t, node.history, []int{2, 3, 4})

	})

	t.Run("returns kth access", func(t *testing.T) {
		node := &lrukNode{k: 3}
		assert.Equal(t, node.kthAccess(), -1)

		node.addTimestamp(1)
		node.addTimestamp(2)
		assert.Equal(t, node.kthAccess(), 1)
	})
}
