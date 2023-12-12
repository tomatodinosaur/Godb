package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"
)

/*
	HeapPage implements the Page interface for pages of HeapFiles. We have

provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.
*/

type Header struct {
	slots int32
	useds int32
}

type heapPage struct {
	// TODO: some code goes here
	hdr    Header
	tuples []*Tuple
	desc   *TupleDesc
	used   []bool

	file   *HeapFile
	pageNo int
	Dirty  bool
}

type Rid struct {
	pageid int
	slotid int
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	// TODO: some code goes here
	hpage := new(heapPage)
	hpage.desc = desc
	hpage.pageNo = pageNo
	hpage.file = f
	bytesPerTuple := 0
	for i := 0; i < len(desc.Fields); i++ {
		if desc.Fields[i].Ftype == IntType {
			bytesPerTuple += int(unsafe.Sizeof(int64(0)))
		} else if desc.Fields[i].Ftype == StringType {
			bytesPerTuple += ((int)(unsafe.Sizeof(byte('a')))) * StringLength
		}
	}
	remPageSize := PageSize - 8             // bytes after header
	numSlots := remPageSize / bytesPerTuple //integer division will round down

	hpage.hdr.slots = int32(numSlots)
	hpage.hdr.useds = 0
	hpage.used = make([]bool, hpage.hdr.slots)
	hpage.tuples = make([]*Tuple, hpage.hdr.slots)
	return hpage //replace me
}

func (h *heapPage) getNumSlots() int {
	// TODO: some code goes here
	return int(h.hdr.slots - h.hdr.useds)
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	// TODO: some code goes here
	if h.hdr.useds == h.hdr.slots {
		return Rid{-1, -1}, fmt.Errorf("full insert fail")
	}
	for i := 0; i < len(h.tuples); i++ {
		if !h.used[i] {
			h.tuples[i] = t
			t.Rid = Rid{h.pageNo, i}
			h.used[i] = true
			h.hdr.useds++
			return t.Rid, nil
		}
	}
	return Rid{-1, -1}, fmt.Errorf("insert fail")
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	r := rid.(Rid)
	idx := r.slotid
	if !h.used[idx] {
		return fmt.Errorf("delete not exisit")
	}
	h.used[idx] = false
	h.hdr.useds--
	return nil //replace me

}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.Dirty //replace me
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	h.Dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	var dbfile DBFile = p.file
	return &dbfile
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// TODO: some code goes here
	b := new(bytes.Buffer)
	binary.Write(b, binary.LittleEndian, h.hdr)
	for i := 0; i < len(h.tuples); i++ {
		if h.used[i] {
			err := h.tuples[i].writeTo(b)
			if err != nil {
				return b, err
			}
		}
	}
	pading := make([]byte, PageSize-b.Len())
	_, err1 := b.Write(pading)
	if err1 != nil {
		return nil, err1
	}
	return b, nil //replace me
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// TODO: some code goes here
	binary.Read(buf, binary.LittleEndian, &h.hdr.slots)
	binary.Read(buf, binary.LittleEndian, &h.hdr.useds)
	for i := 0; i < int(h.hdr.useds); i++ {
		h.tuples[i], _ = readTupleFrom(buf, h.desc)
		h.tuples[i].Rid = Rid{h.pageNo, i}
		h.used[i] = true
	}
	return nil //replace me
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO: some code goes here
	i := 0
	return func() (*Tuple, error) {
		for i < len(p.tuples) {
			if p.used[i] {
				res := new(Tuple)
				res.Desc = p.tuples[i].Desc
				res.Fields = append(res.Fields, p.tuples[i].Fields...)
				res.Rid = p.tuples[i].Rid
				i++
				return res, nil
			} else {
				i++
			}
		}
		return nil, nil
	}
}
