package godb

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	//add additional fields here
	// TODO: some code goes here
	distinct bool
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	// TODO: some code goes here
	return &Project{selectFields, outputNames, child, distinct}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	// TODO: some code goes here
	fts := []FieldType{}
	for i, e := range p.selectFields {
		ft := FieldType{p.outputNames[i], "", e.GetExprType().Ftype}
		fts = append(fts, ft)
	}
	res := TupleDesc{fts}
	return &res
}

func (p *Project) ProjectDes() *TupleDesc {
	// TODO: some code goes here
	fts := []FieldType{}
	for _, e := range p.selectFields {
		ft := FieldType{e.GetExprType().Fname, "", e.GetExprType().Ftype}
		fts = append(fts, ft)
	}
	res := TupleDesc{fts}
	return &res
}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	mp := make(map[any]int)
	iter, _ := p.child.Iterator(tid)
	return func() (*Tuple, error) {
		for {
			t, _ := iter()
			if t == nil {
				return nil, nil
			}
			tup := new(Tuple)
			tup.Desc = *p.Descriptor()
			for i := 0; i < len(p.selectFields); i++ {
				select_res, _ := p.selectFields[i].EvalExpr(t)
				tup.Fields = append(tup.Fields, select_res)
			}
			key := tup.tupleKey()
			if p.distinct {
				_, ok := mp[key]
				if !ok {
					mp[key] = 1
					return tup, nil
				}
			} else {
				return tup, nil
			}
		}
	}, nil
}
