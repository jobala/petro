package disk

import (
	"sync"
)

func NewScheduler(diskManager *diskManager) *DiskScheduler {
	ds := &DiskScheduler{
		reqCh:       make(chan DiskReq, 100),
		pageQueue:   make(map[int]chan DiskReq),
		pageQueueMu: sync.Mutex{},
		diskManager: diskManager,
		mu:          sync.Mutex{},
	}

	go ds.handleDiskReq()
	return ds
}

func NewRequest(pageId int64, data []byte, isWrite bool) DiskReq {
	respCh := make(chan DiskResp)
	return DiskReq{
		PageId: int(pageId),
		Data:   data,
		Write:  false,
		RespCh: respCh,
	}
}

func (ds *DiskScheduler) Schedule(req DiskReq) <-chan DiskResp {
	ds.reqCh <- req
	return req.RespCh
}

func (ds *DiskScheduler) handleDiskReq() {
	for req := range ds.reqCh {
		ds.pageQueueMu.Lock()
		_, ok := ds.pageQueue[req.PageId]
		if !ok {
			ds.pageQueue[req.PageId] = make(chan DiskReq, 10)
		}
		ds.pageQueueMu.Unlock()

		ds.pageQueue[req.PageId] <- req

		// !ok means we created a new page queue, therefore we should start a
		// new worker to handle the queue's page requests
		if !ok {
			go ds.pageWorker(req.PageId, ds.pageQueue[req.PageId])
		}
	}
}

func (ds *DiskScheduler) pageWorker(pageId int, reqQueue chan DiskReq) {
	for {
		select {
		case req := <-reqQueue:
			if req.Write {
				if err := ds.diskManager.writePage(req.PageId, req.Data); err != nil {
					req.RespCh <- DiskResp{Success: false}
				} else {
					req.RespCh <- DiskResp{Success: true}
				}
			} else {
				if data, err := ds.diskManager.readPage(req.PageId); err != nil {
					req.RespCh <- DiskResp{Success: false}
				} else {
					req.RespCh <- DiskResp{Success: true, Data: data}
				}
			}

		default:
			// done handling request for this page, can remove it from queue
			ds.pageQueueMu.Lock()
			delete(ds.pageQueue, pageId)
			ds.pageQueueMu.Unlock()
			return
		}

	}

}

type DiskScheduler struct {
	reqCh       chan DiskReq
	diskManager *diskManager

	pageQueue   map[int]chan DiskReq
	pageQueueMu sync.Mutex
	mu          sync.Mutex
}

type DiskReq struct {
	PageId int
	Data   []byte
	Write  bool
	RespCh chan DiskResp
}

type DiskResp struct {
	Success bool
	Data    []byte
}
