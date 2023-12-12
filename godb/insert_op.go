package godb

// TODO: some code goes here
type InsertOp struct {
	// TODO: some code goes here
	InsertFile DBFile
	child      Operator
}

// Construtor.  The insert operator insert the records in the child
// Operator into the specified DBFile.
func NewInsertOp(File DBFile, child Operator) *InsertOp {
	// TODO: some code goes here
	return &InsertOp{File, child}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	ft := FieldType{"count", "", IntType}
	fts := []FieldType{ft}
	res := TupleDesc{fts}
	return &res
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	iter, _ := iop.child.Iterator(tid)
	count := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		iop.InsertFile.insertTuple(t, tid)
		count++
	}
	return func() (*Tuple, error) {
		res := new(Tuple)
		res.Desc = *iop.Descriptor()
		res.Fields = append(res.Fields, IntField{int64(count)})
		return res, nil
	}, nil
}
