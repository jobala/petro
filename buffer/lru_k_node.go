package buffer

const INVALID_FRAME_ID = -1

type lrukNode struct {
	prev        *lrukNode
	next        *lrukNode
	frameId     int
	k           int
	history     []int
	isEvictable bool
}

func (n *lrukNode) hasKAccess() bool {
	return n.k == len(n.history)
}

func (n *lrukNode) kthAccess() int {
	if len(n.history) > 0 {
		return n.history[0]
	}

	return -1
}

func (n *lrukNode) addTimestamp(timestamp int) {
	if len(n.history) < n.k {
		n.history = append(n.history, timestamp)
		return
	}

	n.history = n.history[1:]
	n.history = append(n.history, timestamp)
}
