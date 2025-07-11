package buffer

import (
	"fmt"
	"sync"
)

func NewLrukReplacer(capacity, k int) *lrukReplacer {
	head := &lrukNode{frameId: INVALID_FRAME_ID}
	tail := &lrukNode{frameId: INVALID_FRAME_ID}

	head.next = tail
	tail.prev = head

	return &lrukReplacer{
		k:             k,
		mu:            sync.Mutex{},
		nodeStore:     map[int]*lrukNode{},
		currSize:      0,
		currTimestamp: 0,
		head:          head,
		tail:          tail,
		replacerSize:  capacity,
	}
}

func (lru *lrukReplacer) remove(frameId int) error {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	node, ok := lru.nodeStore[frameId]
	if !ok {
		return nil
	}

	if !node.isEvictable {
		return fmt.Errorf("evicting a non-evictable frame")
	}

	back := node.prev
	front := node.next

	back.next = front
	front.prev = back

	delete(lru.nodeStore, frameId)
	lru.currSize += 1

	return nil
}

func (lru *lrukReplacer) recordAccess(frameId int) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	node := lru.nodeStore[frameId]
	node.addTimestamp(lru.currTimestamp)

	// move to front of queue
	lru.removeNode(node)
	lru.addNode(node)
}

func (lru *lrukReplacer) removeNode(node *lrukNode) {
	back := node.prev
	front := node.next

	back.next = front
	front.prev = back
}

func (lru *lrukReplacer) addNode(newNode *lrukNode) {
	// add node to doubly linkedlist
	tmp := lru.head.next
	lru.head.next = newNode
	newNode.next = tmp
	tmp.prev = newNode

	lru.nodeStore[newNode.frameId] = newNode
}

func (lru *lrukReplacer) setEvictable(frameId int, setEvictable bool) {}
func (lru *lrukReplacer) evict()                                      {}

func (lru *lrukReplacer) size() int { return lru.currSize }

type lrukReplacer struct {
	mu            sync.Mutex
	nodeStore     map[int]*lrukNode
	replacerSize  int
	currSize      int
	currTimestamp int
	k             int
	head          *lrukNode
	tail          *lrukNode
}
