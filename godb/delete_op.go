package godb

type DeleteOp struct {
	// TODO: some code goes here
	deleteFile DBFile
	child      Operator
}

// Construtor.  The delete operator deletes the records in the child
// Operator from the specified DBFile.
func NewDeleteOp(File DBFile, child Operator) *DeleteOp {
	// TODO: some code goes here
	return &DeleteOp{File, child}
}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	ft := FieldType{"count", "", IntType}
	fts := []FieldType{ft}
	res := TupleDesc{fts}
	return &res
}

// Return an iterator function that deletes all of the tuples from the child
// iterator from the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were deleted.  Tuples should be deleted using the [DBFile.deleteTuple]
// method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	iter, _ := dop.child.Iterator(tid)
	count := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		ok := dop.deleteFile.deleteTuple(t, tid)
		if ok == nil {
			count++
		}
	}
	return func() (*Tuple, error) {
		res := new(Tuple)
		res.Desc = *dop.Descriptor()
		res.Fields = append(res.Fields, IntField{int64(count)})
		return res, nil
	}, nil

}
