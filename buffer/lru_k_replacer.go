package buffer

import "sync"

func NewLrukReplacer(capacity, k int) *lruKReplacer {
	head := &lrukNode{frameId: INVALID_FRAME_ID}
	tail := &lrukNode{frameId: INVALID_FRAME_ID}

	head.next = tail
	tail.prev = head

	return &lruKReplacer{
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

func (lru *lruKReplacer) addNode(newNode *lrukNode) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	// add node to doubly linkedlist
	tmp := lru.head.next
	lru.head.next = newNode
	newNode.next = tmp
	tmp.prev = newNode

	lru.nodeStore[newNode.frameId] = newNode
	lru.currSize += 1
}

func (lru *lruKReplacer) remove(frameId int) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	node := lru.nodeStore[frameId]
	back := node.prev
	front := node.next

	back.next = front
	front.prev = back

	delete(lru.nodeStore, frameId)
	lru.currSize -= 1
}

func (lru *lruKReplacer) recordAccess(frameId int) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	node := lru.nodeStore[frameId]
	node.addTimestamp(lru.currTimestamp)

	// move to front of queue
	lru.remove(frameId)
	lru.addNode(node)
}

func (lru *lruKReplacer) setEvictable(frameId int, setEvictable bool) {}
func (lru *lruKReplacer) evict()                                      {}

func (lru *lruKReplacer) size() int { return lru.currSize }

type lruKReplacer struct {
	mu            sync.Mutex
	nodeStore     map[int]*lrukNode
	replacerSize  int
	currSize      int
	currTimestamp int
	k             int
	head          *lrukNode
	tail          *lrukNode
}
