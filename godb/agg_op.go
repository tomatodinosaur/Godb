package godb

type Aggregator struct {
	// Expressions that when applied to tuples from the child operators,
	// respectively, return the value of the group by key tuple
	// 当分别从子运算符应用于元组时，表达式返回按键元组分组的值
	groupByFields []Expr

	// Aggregation states that serves as a template as to which types of
	// aggregations in which order are to be computed for every group
	// 聚合状态用作模板，决定为每个组计算哪种类型的聚合以及按何种顺序计算。
	newAggState []AggState //聚合状态

	child Operator // the child operator for the inputs to aggregate
}

type AggType int

const (
	IntAggregator    AggType = iota
	StringAggregator AggType = iota
)

const DefaultGroup int = 0 // for handling the case of no group-by

// Constructor for an aggregator with a group-by
func NewGroupedAggregator(emptyAggState []AggState, groupByFields []Expr, child Operator) *Aggregator {
	return &Aggregator{groupByFields, emptyAggState, child}
}

// Constructor for an aggregator with no group-by
func NewAggregator(emptyAggState []AggState, child Operator) *Aggregator {
	return &Aggregator{nil, emptyAggState, child}
}

// Return a TupleDescriptor for this aggregation. If the aggregator has no group-by, the
// returned descriptor should contain the union of the fields in the descriptors of the
// aggregation states. If the aggregator has a group-by, the returned descriptor will
// additionally start with the group-by fields, and then the aggregation states descriptors
// like that without group-by.
//
// HINT: for groupByFields, you can use [Expr.GetExprType] to get the FieldType
// HINT: use the merge function you implemented for TupleDesc in lab1 to merge the two TupleDescs
/*
	返回此聚合的 TupleDescriptor。
	如果聚合器没有分组依据，则返回的描述符应包含聚合状态描述符中字段的并集。
	如果聚合器有 group-by，则返回的描述符将另外以 group-by 字段开头，然后聚合状态描述符与没有 group-by 的描述符一样。
	提示：对于 groupByFields，您可以使用 [Expr.GetExprType] 获取 FieldType
	提示：使用您在 lab1 中为 TupleDesc 实现的合并函数来合并两个 TupleDesc
*/

/*
Tuplu DESC
<group1,group2...> sum avg count
*/
func (a *Aggregator) Descriptor() *TupleDesc {
	// TODO: some code goes here
	fts := []FieldType{}
	if a.groupByFields != nil {
		for i := 0; i < len(a.groupByFields); i++ {
			fts = append(fts, a.groupByFields[i].GetExprType())
		}
	}
	for i := 0; i < len(a.newAggState); i++ {
		fts = append(fts, a.newAggState[i].GetTupleDesc().Fields...)
	}
	res := TupleDesc{fts}
	return &res
}

// Aggregate operator implementation: This function should iterate over the results of
// the aggregate. The aggregate should be the result of aggregating each group's tuples
// and the iterator should iterate through each group's result. In the case where there
// is no group-by, the iterator simply iterates through only one tuple, representing the
// aggregation of all child tuples.
/*
	聚合运算符实现：该函数应该迭代聚合的结果。
	聚合应该是聚合每个组的元组的结果，并且迭代器应该迭代每个组的结果。
	在没有分组依据的情况下，迭代器仅迭代一个元组，表示所有子元组的聚合。
*/
func (a *Aggregator) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// the child iterator
	childIter, err := a.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, GoDBError{MalformedDataError, "child iter unexpectedly nil"}

	}
	// the map that stores the aggregation state of each group
	aggState := make(map[any]*[]AggState)
	if a.groupByFields == nil {
		var newAggState []AggState
		for _, as := range a.newAggState {
			copy := as.Copy()
			if copy == nil {
				return nil, GoDBError{MalformedDataError, "aggState Copy unexpectedly returned nil"}
			}
			newAggState = append(newAggState, copy)
		}
		aggState[DefaultGroup] = &newAggState
	}
	// the list of group key tuples
	var groupByList []*Tuple
	// the iterator for iterating thru the finalized aggregation results for each group
	var finalizedIter func() (*Tuple, error)
	return func() (*Tuple, error) {
		// iterates thru all child tuples
		for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
			if err != nil {
				return nil, err
			}
			if t == nil {
				return nil, nil
			}

			if a.groupByFields == nil { // adds tuple to the aggregation in the case of no group-by
				for i := 0; i < len(a.newAggState); i++ {
					(*aggState[DefaultGroup])[i].AddTuple(t)
				}
			} else { // adds tuple to the aggregation with grouping
				keygenTup, err := extractGroupByKeyTuple(a, t)
				if err != nil {
					return nil, err
				}
				//每种类型的key值代表一类分组，每种分组都需要 映射所有的聚合
				key := keygenTup.tupleKey()
				if aggState[key] == nil {
					var newAgg []AggState
					for _, as := range a.newAggState {
						copy := as.Copy()
						if copy == nil {
							return nil, GoDBError{MalformedDataError, "aggState Copy unexpectedly returned nil"}
						}
						newAgg = append(newAgg, copy)
					}
					//asNew := make([]AggState, len(a.newAggState))
					aggState[key] = &newAgg
					groupByList = append(groupByList, keygenTup)
				}

				addTupleToGrpAggState(a, t, aggState[key])
			}
		}

		if finalizedIter == nil { // builds the iterator for iterating thru the finalized aggregation results for each group
			if a.groupByFields == nil {
				var tup *Tuple
				for i := 0; i < len(a.newAggState); i++ {
					newTup := (*aggState[DefaultGroup])[i].Finalize()
					tup = joinTuples(tup, newTup)
				}
				finalizedIter = func() (*Tuple, error) { return nil, nil }
				return tup, nil
			} else {
				finalizedIter = getFinalizedTuplesIterator(a, groupByList, aggState)
			}
		}
		return finalizedIter()
	}, nil
}

// Given a tuple t from a child iteror, return a tuple that identifies t's group.
// The returned tuple should contain the fields from the groupByFields list
// passed into the aggregator constructor.  The ith field can be extracted
// from the supplied tuple using the EvalExpr method on the ith expression of
// groupByFields.
// If there is any error during expression evaluation, return the error.
/*
	给定子迭代器的元组 t，返回标识 t 组的元组。
	返回的元组应包含传递到聚合器构造函数的 groupByFields 列表中的字段。
	可以使用 groupByFields 的第 i 个表达式上的 EvalExpr 方法从提供的元组中提取第 i 个字段。
	如果表达式求值过程中出现任何错误，则返回错误。
*/
func extractGroupByKeyTuple(a *Aggregator, t *Tuple) (*Tuple, error) {
	// TODO: some code goes here
	res := new(Tuple)
	fts := []FieldType{}
	for i := 0; i < len(a.groupByFields); i++ {
		ft := a.groupByFields[i].GetExprType()
		fts = append(fts, ft)
	}
	td := TupleDesc{}
	td.Fields = fts
	res.Desc = td
	for i := 0; i < len(a.groupByFields); i++ {
		x, _ := a.groupByFields[i].EvalExpr(t)
		res.Fields = append(res.Fields, x)
	}
	return res, nil
}

// Given a tuple t from child and (a pointer to) the array of partially computed aggregates
// grpAggState, add t into all partial aggregations using the [AggState AddTuple] method.
// If any of the array elements is of grpAggState is null (i.e., because this is the first
// invocation of this method, create a new aggState using aggState.Copy() on appropriate
// element of the a.newAggState field and add the new aggState to grpAggState.
/*
// 给定来自 child 的元组 t 和（指向）部分计算聚合 grpAggState 的数组，
	 使用 [AggState AddTuple] 方法将 t 添加到所有部分聚合中。
// 如果 grpAggState 的任何数组元素为 null（即，因为这是此方法的第一次调用，
   请在 a.newAggState 字段的适当元素上使用 aggState.Copy() 创建一个新的 aggState 并添加新的 aggState 到 grpAggState。
*/
func addTupleToGrpAggState(a *Aggregator, t *Tuple, grpAggState *[]AggState) {
	// TODO: some code goes here
	for i := 0; i < len(a.newAggState); i++ {
		(*grpAggState)[i].AddTuple(t)
	}
}

// Given that all child tuples have been added, return an iterator that iterates
// through the finalized aggregate result one group at a time. The returned tuples should
// be structured according to the TupleDesc returned from the Descriptor() method.
// HINT: you can call [aggState.Finalize()] to get the field for each AggState.
// Then, you should get the groupByTuple and merge it with each of the AggState tuples using the
// joinTuples function in tuple.go you wrote in lab 1.
/*
假设所有子元组都已添加，则返回一个迭代器，一次迭代一组最终的聚合结果。
返回的元组应根据 Descriptor() 方法返回的 TupleDesc 进行构造。
提示：您可以调用 [aggState.Finalize()] 来获取每个 AggState 的字段。
然后，您应该获取 groupByTuple 并使用您在实验 1 中编写的 tuple.go 中的 joinTuples 函数将其与每个 AggState 元组合并。
*/
func getFinalizedTuplesIterator(a *Aggregator, groupByList []*Tuple, aggState map[any]*[]AggState) func() (*Tuple, error) {
	number := 0 // "captured" counter to track the current tuple we are iterating over
	return func() (*Tuple, error) {
		// TODO: some code goes here
		if number == len(groupByList) {
			return nil, nil
		}
		var tup *Tuple
		group := groupByList[number].tupleKey()
		for i := 0; i < len(a.newAggState); i++ {
			newTup := (*aggState[group])[i].Finalize()
			tup = joinTuples(tup, newTup)
		}
		tup = joinTuples(groupByList[number], tup)
		number++
		return tup, nil // TODO change me
	}
}
