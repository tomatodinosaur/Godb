package godb

type LimitOp struct {
	child     Operator //required fields for parser
	limitTups Expr
	//add additional fields here, if needed
}

// Limit constructor -- should save how many tuples to return and the child op.
// lim is how many tuples to return and child is the child op.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	// TODO: some code goes here
	return &LimitOp{child, lim}
}

// Return a TupleDescriptor for this limit
func (l *LimitOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the
// results of the child iterator, and limit the result set to the first
// [lim] tuples it sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	iter, _ := l.child.Iterator(tid)
	count := 0
	return func() (*Tuple, error) {
		t, _ := iter()
		if t == nil {
			return nil, nil
		}
		lim, _ := l.limitTups.EvalExpr(t)
		if count == int(lim.(IntField).Value) {
			return nil, nil
		} else {
			count++
			return t, nil
		}
	}, nil
}
