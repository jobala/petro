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

func (lru *lrukReplacer) recordAccess(frameId int) {
	lru.mu.Lock()
	lru.currTimestamp += 1
	node, ok := lru.nodeStore[frameId]
	lru.mu.Unlock()

	if ok {
		node.addTimestamp(lru.currTimestamp)

		// move to front of queue
		lru.removeNode(node)
		lru.addNode(node)
		return
	}

	lru.addNode(&lrukNode{frameId: frameId})
}

func (lru *lrukReplacer) setEvictable(frameId int, evictable bool) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	node, ok := lru.nodeStore[frameId]
	if !ok {
		return
	}

	if node.isEvictable && !evictable {
		node.isEvictable = evictable
		lru.currSize -= 1
	}

	if !node.isEvictable && evictable {
		node.isEvictable = evictable
		lru.currSize += 1
	}
}

func (lru *lrukReplacer) evict() (int, error) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	var node *lrukNode
	curr := lru.tail

	for curr != nil {
		if curr.isEvictable {
			node = curr
			break
		}

		curr = curr.prev
	}

	// no evictable nodes found
	if curr == nil {
		return INVALID_FRAME_ID, nil
	}

	// continue search for better eviction candidate
	curr = curr.prev
	for curr != lru.head {
		if !curr.isEvictable {
			curr = curr.prev
			continue
		}

		if !curr.hasKAccess() && node.hasKAccess() {
			node = curr
		} else if !curr.hasKAccess() && !node.hasKAccess() && curr.kthAccess() < node.kthAccess() {
			node = curr
		} else if curr.hasKAccess() && node.hasKAccess() && curr.kthAccess() < node.kthAccess() {
			node = curr
		}

		curr = curr.prev
	}

	frameId := node.frameId

	return frameId, nil
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

func (lru *lrukReplacer) size() int { return lru.currSize }

func (lru *lrukReplacer) removeNode(node *lrukNode) {
	back := node.prev
	front := node.next

	back.next = front
	front.prev = back
}

func (lru *lrukReplacer) addNode(newNode *lrukNode) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	newNode.k = lru.k

	tmp := lru.head.next
	lru.head.next = newNode
	newNode.prev = lru.head

	newNode.next = tmp
	tmp.prev = newNode

	lru.nodeStore[newNode.frameId] = newNode
}

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
