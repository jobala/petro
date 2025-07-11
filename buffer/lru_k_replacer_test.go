package buffer

import "testing"

func TestLRU_K_Replacer(t *testing.T) {

	t.Run("accessing a node moves it to the front of the queue", func(t *testing.T) {})
	t.Run("prefers removal of nodes with less than K accesses", func(t *testing.T) {})
	t.Run("prefers removal of node with oldest K access", func(t *testing.T) {})
}
