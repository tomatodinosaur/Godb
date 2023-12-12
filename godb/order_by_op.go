package godb

import (
	"sort"
)

// TODO: some code goes here
type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	//add additional fields here
	asc []bool
}

// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	// TODO: some code goes here
	return &OrderBy{orderByFields, child, ascending}, nil

}

func (o *OrderBy) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return o.child.Descriptor()
}

// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here

	iter, _ := o.child.Iterator(tid)
	v := make([]*Tuple, 0)
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		v = append(v, t)
	}

	sort.Slice(v, func(i int, j int) bool {
		for k := 0; k < len(o.orderBy); k++ {
			ifasc := o.asc[k]
			ival, _ := o.orderBy[k].EvalExpr(v[i])
			jval, _ := o.orderBy[k].EvalExpr(v[j])
			tp := o.orderBy[k].GetExprType().Ftype
			switch tp {
			case IntType:
				ivalue := ival.(IntField).Value
				jvalue := jval.(IntField).Value
				if ivalue == jvalue {
					continue
				}
				if ifasc {
					return ivalue < jvalue
				} else {
					return ivalue > jvalue
				}
			case StringType:
				ivalue := ival.(StringField).Value
				jvalue := jval.(StringField).Value
				if ivalue == jvalue {
					continue
				}
				if ifasc {
					return ivalue < jvalue
				} else {
					return ivalue > jvalue
				}
			}
		}
		return true
	})

	i := 0
	return func() (*Tuple, error) {
		if i == len(v) {
			return nil, nil
		}
		ans := v[i]
		i++
		return ans, nil
	}, nil //replace me
}
