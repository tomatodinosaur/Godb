package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// HeapFile is an unordered collection of tuples Internally, it is arranged as a
// set of heapPage objects
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	// TODO: some code goes here
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	bufPool *BufferPool
	sync.Mutex
	desc     *TupleDesc
	Filename string
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	// TODO: some code goes here
	file, err := os.OpenFile(fromFile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	hf := new(HeapFile)
	hf.bufPool = bp
	hf.desc = td
	hf.Filename = fromFile
	return hf, nil //replace me
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	// TODO: some code goes here
	fileinfo, err := os.Stat(f.Filename)
	if err == nil {
		return int(fileinfo.Size() / int64(PageSize))
	}
	return 0
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for j := 0; j < f.NumPages(); j++ {
			pg, err := bp.GetPage(f, j, tid, ReadPerm)
			if pg == nil || err != nil {
				fmt.Println("page nil or error", err)
				break
			}
			if (*pg).isDirty() {
				(*f).flushPage(pg)
				(*pg).setDirty(false)
			}

		}

		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk.  This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to the
// appropriate offset, read the bytes in, and construct a [heapPage] object, using
// the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (*Page, error) {
	// TODO: some code goes here
	file, err := os.OpenFile(f.Filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := make([]byte, PageSize)
	_, err1 := file.ReadAt(buf, int64(pageNo*PageSize))
	if err1 != nil {
		return nil, err1
	}
	hg := newHeapPage(f.desc, pageNo, f)
	hg.initFromBuffer(bytes.NewBuffer(buf))

	var ans Page = hg
	return &ans, nil
}

// Add the tuple to the HeapFile.  This method should search through pages in
// the heap file, looking for empty slots and adding the tuple in the first
// empty slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile.  We will
// add support for concurrent modifications in lab 3.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	// TODO: some code goes here
	for i := 0; i < f.NumPages(); i++ {
		hp, err := f.bufPool.GetPage(f, i, tid, 1)
		if err != nil {
			return err
		}
		page := (*hp).(*heapPage)
		_, ok := page.insertTuple(t)
		if ok == nil {
			page.setDirty(true)
			return nil
		}
	}
	//All pages full
	new_page := newHeapPage(f.desc, f.NumPages(), f)
	_, ok := new_page.insertTuple(t)
	if ok != nil {
		return ok
	}
	//Flush to diskfile
	var pg Page = new_page
	f.flushPage(&pg)
	new_page.setDirty(false)

	return nil

}

// Remove the provided tuple from the HeapFile.  This method should use the
// [Tuple.Rid] field of t to determine which tuple to remove.
// This method is only called with tuples that are read from storage via the
// [Iterator] method, so you can so you can supply the value of the Rid
// for tuples as they are read via [Iterator].  Note that Rid is an empty interface,
// so you can supply any object you wish.  You will likely want to identify the
// heap page and slot within the page that the tuple came from.
/*
// 从 HeapFile 中删除提供的元组。 此方法应使用 t 的 [Tuple.Rid] 字段来确定要删除哪个元组。
/ 仅使用通过 [Iterator] 方法从存储中读取的元组调用此方法，因此您可以提供 Rid 的值
  对于通过 [Iterator] 读取的元组。 请注意，Rid 是一个空接口，因此您可以提供任何您想要的对象。
	您可能需要识别元组来自的页面中的堆页面和槽。
*/
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	// TODO: some code goes here
	pageid := t.Rid.(Rid).pageid
	hp, err := f.bufPool.GetPage(f, pageid, tid, 1)
	if err != nil {
		return err
	}
	page := (*hp).(*heapPage)
	ok := page.deleteTuple(t.Rid)
	if ok != nil {
		return ok
	}
	page.setDirty(true)
	return nil
}

// Method to force the specified page back to the backing file at the appropriate
// location.  This will be called by BufferPool when it wants to evict a page.
// The Page object should store information about its offset on disk (e.g.,
// that it is the ith page in the heap file), so you can determine where to write it
// back.
func (f *HeapFile) flushPage(p *Page) error {
	// TODO: some code goes here
	file, err := os.OpenFile(f.Filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	page := (*p).(*heapPage)
	new_buf, _ := page.toBuffer()

	_, err1 := file.WriteAt(new_buf.Bytes(), int64(page.pageNo*PageSize))
	if err1 != nil {
		return err1
	}
	return nil
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return f.desc
}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
/*
// [运算符] 迭代器方法
// 返回一个迭代堆文件中记录的函数
// 注意，该方法应该使用BufferPool方法GetPage从HeapFile中读取页面，而不是直接读取页面，
// 由于 BufferPool 缓存页面并管理事务的页面级锁定状态
// 您应该确保此方法返回的元组的 Rid 对象设置适当，以便 [deleteTuple] 可以工作（请参阅那里的其他注释）。
*/
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {

	// TODO: some code goes here
	i := 0
	var iter func() (*Tuple, error)
	var page *heapPage = new(heapPage)
	page.pageNo = -1
	fmt.Println(f.NumPages())

	return func() (*Tuple, error) {
		for i < f.NumPages() {
			if page.pageNo != i {
				hp, err := f.bufPool.GetPage(f, i, tid, 1)
				if err != nil {
					return nil, err
				}
				page = (*hp).(*heapPage)
				iter = page.tupleIter()
			}
			for {
				tup, _ := iter()
				if tup != nil {
					return tup, nil
				}
				i++
				break
			}
		}
		return nil, nil
	}, nil

}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {

	// TODO: some code goes here
	hash := heapHash{f.Filename, pgNo}
	return hash

}
