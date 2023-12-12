package godb

import (
	"container/list"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

// 用读锁或者写锁 进行页面的 读操作
type RWLock int

const (
	ReadLock  RWLock = iota
	writeLock RWLock = iota
)

type BufferPool struct {
	// TODO: some code goes here
	// O(1)LRU_plus 算法
	pool map[heapHash]*list.Element
	lst  *list.List
	Cap  int
	//lab3
	poolLock              sync.Mutex
	aliveTransactions     map[TransactionID]struct{}
	transactionReadLocks  map[TransactionID](map[any]struct{})
	transactionWriteLocks map[TransactionID](map[any]struct{})

	adjacencyList map[TransactionID](map[TransactionID]struct{})
}

type pair struct {
	key   heapHash
	value *Page
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	// TODO: some code goes here
	var bp BufferPool
	bp.Cap = numPages
	bp.lst = list.New()
	bp.pool = make(map[heapHash]*list.Element)
	//lab3
	bp.aliveTransactions = make(map[TransactionID]struct{})
	bp.transactionReadLocks = make(map[TransactionID](map[any]struct{}))
	bp.transactionWriteLocks = make(map[TransactionID](map[any]struct{}))

	bp.adjacencyList = make(map[TransactionID](map[TransactionID]struct{}))
	return &bp
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	// TODO: some code goes here
	for bp.lst.Len() != 0 {
		e := bp.lst.Front()
		page := e.Value.(pair).value
		pg := (*page).(*heapPage)
		pg.file.flushPage(page)
		delete(bp.pool, e.Value.(pair).key)
		bp.lst.Remove(e)
	}
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// TODO: some code goes here

}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here

}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	bp.poolLock.Lock()
	defer bp.poolLock.Unlock()

	bp.aliveTransactions[tid] = struct{}{}
	bp.transactionReadLocks[tid] = make(map[any]struct{})
	bp.transactionWriteLocks[tid] = make(map[any]struct{})
	bp.adjacencyList[tid] = make(map[TransactionID]struct{})
	return nil
}

func (bp *BufferPool) dfs(tid TransactionID, visited map[TransactionID]bool, visitedThisIter map[TransactionID]bool) bool {
	visited[tid] = true
	visitedThisIter[tid] = true
	for next, _ := range bp.adjacencyList[tid] {
		if !visited[next] {
			cycleFound := bp.dfs(next, visited, visitedThisIter)
			if cycleFound {
				return true
			}
		} else if visitedThisIter[next] {
			return true
		}
	}
	visitedThisIter[tid] = false
	return false
}

func (bp *BufferPool) findCycle() bool {
	visited := make(map[TransactionID]bool)
	visitedThisIter := make(map[TransactionID]bool)
	for tid, _ := range bp.aliveTransactions {
		visited[tid] = false
		visitedThisIter[tid] = false
	}
	for tid, _ := range bp.aliveTransactions {
		if !visited[tid] {
			if bp.dfs(tid, visited, visitedThisIter) {
				return true
			}
		}
	}
	return false
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. For lab 1, you do not need to
// implement locking or deadlock detection. [For future labs, before returning the page,
// attempt to lock it with the specified permission. If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock]. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
/*

从指定的 DBFile（例如 HeapFile）中检索代表指定的事务的页面。
如果页面没有缓存在缓冲池中，您可以使用[DBFile.readPage]从磁盘读取它。
如果缓冲池已满（即已经存储了 numPages 页），则应逐出一个页。
不应该驱逐脏页，因为这会违反 NO STEAL。 如果缓冲池充满了脏页，则应该返回错误。
 对于实验 1，您不需要实现锁定或死锁检测。

[对于未来的实验，在返回页面之前，尝试使用指定的权限锁定它。
如果锁不可用，则应阻塞直到锁空闲。 如果发生死锁，则中止死锁中的事务之一]。

您可能希望将 BufferPool 中的页面列表存储在由 [DBFile.pageKey] 键控的映射中。
*/
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	// TODO: some code goes here
	key := file.pageKey(pageNo).(heapHash)
	bp.poolLock.Lock()
	_, ok := bp.aliveTransactions[tid]
	if !ok {
		bp.poolLock.Unlock()
		return nil, errors.New("transaction is not alive")
	}
	bp.poolLock.Unlock()

	for {
		bp.poolLock.Lock()
		bad := false
		if perm == ReadPerm {
			// check write locks
			for other_tid, _ := range bp.aliveTransactions {
				if other_tid == tid {
					continue
				}
				writeLocks := bp.transactionWriteLocks[other_tid]
				for lock, _ := range writeLocks {
					if lock == key {
						bp.adjacencyList[tid][other_tid] = struct{}{}
						bad = true
					}
				}
			}
		} else if perm == WritePerm {
			// check read and write locks
			for other_tid, _ := range bp.aliveTransactions {
				if other_tid == tid {
					continue
				}
				readLocks := bp.transactionReadLocks[other_tid]
				for lock, _ := range readLocks {
					if lock == key {
						bp.adjacencyList[tid][other_tid] = struct{}{}
						bad = true
					}
				}
				writeLocks := bp.transactionWriteLocks[other_tid]
				for lock, _ := range writeLocks {
					if lock == key {
						bp.adjacencyList[tid][other_tid] = struct{}{}
						bad = true
					}
				}
			}
		}
		randTime := rand.Intn(30) - 15
		if bp.findCycle() {
			bp.poolLock.Unlock()
			bp.AbortTransaction(tid)
			time.Sleep(time.Duration(15+randTime) * time.Millisecond)
			return nil, errors.New("transaction aborted")
		}
		if bad {
			bp.poolLock.Unlock()
			time.Sleep(time.Duration(15+randTime) * time.Millisecond)
		} else {
			break
		}
	}

	defer bp.poolLock.Unlock()

	if perm == ReadPerm {
		bp.transactionReadLocks[tid][key] = struct{}{}
	} else if perm == WritePerm {
		bp.transactionWriteLocks[tid][key] = struct{}{}
	}

	node, ok := bp.pool[key]
	if ok {
		bp.lst.MoveToFront(node)
		return node.Value.(pair).value, nil
	}
	cnt := 0
	for bp.lst.Len() == bp.Cap {
		if cnt == bp.Cap {
			return nil, fmt.Errorf("all is dirty")
		}
		last := bp.lst.Back()
		p := last.Value.(pair).value
		if (*p).isDirty() {
			bp.lst.MoveToFront(last)
			cnt++
		} else {
			bp.lst.Remove(last)
			delete(bp.pool, last.Value.(pair).key)
			break
		}
	}

	page, err := file.readPage(pageNo)
	if err != nil {
		return nil, err
	}
	e := bp.lst.PushFront(pair{key, page})
	bp.pool[key] = e
	return e.Value.(pair).value, nil
}
