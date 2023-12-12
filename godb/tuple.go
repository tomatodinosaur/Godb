package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/mitchellh/hashstructure/v2"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
// DBType是GoDB中元组字段的类型，例如IntType或StringType
type DBType int

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

var typeNames map[DBType]string = map[DBType]string{IntType: "int", StringType: "string"}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
/*
FieldType 是元组中字段的类型，例如它的名称、表和 [godb.DBType]。
TableQualifier 可能是空字符串，
也可能不是空字符串，具体取决于查询中是否指定了表
*/
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
// TupleDesc 是元组的“类型”，例如字段名称和类型
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true if
// all of their field objects are equal and they
// are the same length
/*
// 比较两个元组描述，如果返回 true
// 它们的所有字段对象都是相等的并且它们长度相同
*/
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	// TODO: some code goes here
	n := len(d1.Fields)
	if n != len(d2.Fields) {
		return false
	}
	for i := 0; i < n; i++ {
		if d1.Fields[i] != d2.Fields[i] {
			return false
		}
	}
	return true

}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
/*
给定 FieldType f 和 TupleDesc desc，在 f 的 desc 中找到最佳匹配字段。
匹配被定义为具有相同的 Ftype 和相同的名称，
如果 f 具有 TableQualifier，则首选具有相同 TableQualifier 的匹配
我们提供了这个实现，因为它的细节对于解析器的行为来说是特殊的，我们不要求您编写它
*/
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}

}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
/*
// 制作元组 desc 的副本。 请注意，在 go 中，将切片分配给另一个切片对象不会复制该切片的内容。 看一下内置函数“复制”。
*/
func (td *TupleDesc) copy() *TupleDesc {
	// TODO: some code goes here
	temp := new(TupleDesc)
	temp.Fields = make([]FieldType, 0)
	temp.Fields = append(temp.Fields, td.Fields...)
	return temp //replace me
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
/*
将 TupleDesc 中每个字段的 TableQualifier 指定为提供的别名。 我们提供了这个函数，因为它仅供解析器使用。
*/
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
// 将两个 TupleDesc 合并在一起。 生成的 TupleDesc
// 应包含 desc2 的字段
// 附加到 desc 的字段上。
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	// TODO: some code goes here
	filed := make([]FieldType, 0)
	filed = append(filed, desc.Fields...)
	if desc2 != nil {
		filed = append(filed, desc2.Fields...)
	}
	return &TupleDesc{filed} //replace mes
}

// ================== Tuple Methods ======================

// Interface used for tuple field values
// Since it implements no methods, any object can be used
// but having an interface for this improves code readability
// where tuple values are used
/*
/ 用于元组字段值的接口由于它不实现任何方法，
	因此可以使用任何对象，但拥有一个接口可以提高使用元组值的代码可读性
*/
type DBValue interface {
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
// Tuple表示从数据库读取的元组的内容
// 它包括元组描述符和字段的值
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	//用于跟踪页面以及读取该页面的位置
	Rid recordID //used to track the page and position this page was read from
}

type recordID interface {
}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
/*
// 将元组的内容序列化为字节数组 由于所有元组都是固定大小的，因此此方法应该简单地将字段按顺序写入到提供的缓冲区中。
//
  请参阅函数 [binary.Write]。 对象应该以小端顺序序列化。
	可以通过转换为 []byte 将字符串转换为字节数组。
	请注意，所有字符串都需要填充为 StringLength 字节（在 types.go 中设置）。
	 例如，如果 StringLength 设置为 5，
	 则字符串 'mit' 应写为 'm', 'i', 't', 0, 0 如果缓冲区没有足够的容量来存储元组，
	 则可能会返回错误。
*/
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	// TODO: some code goes here
	for i := 0; i < len(t.Fields); i++ {
		if t.Desc.Fields[i].Ftype == IntType {

			err := binary.Write(b, binary.LittleEndian, int64(t.Fields[i].(IntField).Value))
			if err != nil {
				return err
			}
		} else {
			str := t.Fields[i].(StringField).Value
			for i := len(str); i < StringLength; i++ {
				str += "0"
			}
			err := binary.Write(b, binary.LittleEndian, []byte(str))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
/*
// 从指定的[TupleDesc]中读取元组的内容
// 指定缓冲区，返回一个Tuple。
//
// 请参阅 [binary.Read]。 对象应该以小端顺序反序列化。
//
// 所有字符串都存储为 StringLength 字节对象。
//
// 长度< StringLength 的字符串将用零填充，并且这些尾随零应该从字符串中删除。 []byte 可以直接转换为字符串。
//
// 如果缓冲区没有足够的数据来反序列化元组，可能会返回错误。
*/
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	// TODO: some code goes here
	temp := new(Tuple)
	temp.Desc.Fields = append(temp.Desc.Fields, desc.Fields...)
	intbuf := make([]byte, 8)
	strbuf := make([]byte, StringLength)
	for i := 0; i < len(desc.Fields); i++ {
		if desc.Fields[i].Ftype == IntType {
			binary.Read(b, binary.LittleEndian, intbuf)
			temp.Fields = append(temp.Fields, IntField{int64(binary.LittleEndian.Uint64(intbuf))})
		} else {
			binary.Read(b, binary.LittleEndian, strbuf)
			str := string(strbuf)
			for len(str) > 0 {
				if str[len(str)-1] != '0' {
					break
				}
				str = str[:len(str)-1]
			}
			temp.Fields = append(temp.Fields, StringField{str})
		}
	}

	return temp, nil //replace me

}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
// 比较两个元组是否相等。 Equality 意味着 TupleDescs 相等
// 并且所有字段都是相等的。 TupleDescs 应该与
// [TupleDesc.equals] 方法，但字段可以直接比较相等
// 运算符。
func (t1 *Tuple) equals(t2 *Tuple) bool {
	// TODO: some code goes here
	if !t1.Desc.equals(&t2.Desc) {
		return false
	}
	if len(t1.Fields) != len(t2.Fields) {
		return false
	}
	for i := 0; i < len(t1.Fields); i++ {
		if t1.Fields[i] != t2.Fields[i] {
			return false
		}
	}
	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2 appended to t1.
// 将两个元组合并在一起，生成一个新元组，其中 t2 的字段附加到 t1。
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	// TODO: some code goes here
	t := new(Tuple)
	var tf *TupleDesc
	if t1 == nil {
		tf = &t2.Desc
	}
	if t2 == nil {
		tf = &t1.Desc
	}
	if t1 != nil && t2 != nil {
		tf = t1.Desc.merge(&t2.Desc)
	}
	t.Desc = *tf
	t.Fields = make([]DBValue, 0)
	if t1 != nil {
		t.Fields = append(t.Fields, t1.Fields...)
	}
	if t2 != nil {
		t.Fields = append(t.Fields, t2.Fields...)
	}
	return t
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
/*
// 将提供的表达式应用于 t 和 t2，并比较结果，返回 orderByState 值。
// 采用任意表达式而不是字段，因为例如对于 ORDER BY SQL 可能会 ORDER BY 任意表达式，例如 substr(name, 1, 2)
// 请注意，在大多数情况下 Expr 将是一个 [godb.FieldExpr]，它只是
   从提供的元组中提取命名字段。
// 在元组上调用 [Expr.EvalExpr] 方法将返回所提供元组上的表达式的值。
*/
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	// TODO: some code goes here
	tvalue, err1 := field.EvalExpr(t)
	if err1 != nil {
		return -1, err1
	}
	t2value, err2 := field.EvalExpr(t2)
	if err2 != nil {
		return -1, err2
	}
	switch tvalue.(type) {
	case IntField:
		_, ok := t2value.(IntField)
		if ok {
			if tvalue.(IntField).Value > t2value.(IntField).Value {
				return OrderedGreaterThan, nil
			}
			if tvalue.(IntField).Value == t2value.(IntField).Value {
				return OrderedEqual, nil
			}
			return OrderedLessThan, nil
		} else {
			return -1, fmt.Errorf("cannot compare")
		}
	case StringField:
		_, ok := t2value.(StringField)
		if ok {
			if tvalue.(StringField).Value > t2value.(StringField).Value {
				return OrderedGreaterThan, nil
			}
			if tvalue.(StringField).Value == t2value.(StringField).Value {
				return OrderedEqual, nil
			}
			return OrderedLessThan, nil
		} else {
			return -1, fmt.Errorf("cannot compare")
		}
	}

	return -1, nil // replace me
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
/*
从元组中投影出提供的字段。 应该返回一个新的元组，其中仅包含字段中命名的字段。
不应该要求在 TableQualifier 上匹配，
但应该优先选择在 TableQualifier 上匹配的字段
（例如，fields 中的字段 t1.name 应与 t 中的条目 t2.name 匹配，
但前提是不存在条目 t1.name 在 t)
*/
type projectIndex struct {
	Fname string
	Table string
}

func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	// TODO: some code goes here
	mp := make(map[projectIndex]int)
	for i, f := range t.Desc.Fields {
		index := projectIndex{f.Fname, f.TableQualifier}
		mp[index] = i
		defaultindex := projectIndex{f.Fname, ""}
		mp[defaultindex] = i
	}
	temp := new(Tuple)
	temp.Desc.Fields = make([]FieldType, len(fields))
	temp.Fields = make([]DBValue, len(fields))
	for i := 0; i < len(fields); i++ {
		fname := fields[i].Fname
		table := fields[i].TableQualifier
		index := projectIndex{fname, table}
		idx, ok := mp[index]
		if ok {
			temp.Desc.Fields[i] = t.Desc.Fields[idx]
			temp.Fields[i] = t.Fields[idx]
		} else {
			defaultindex := projectIndex{fname, ""}
			idx, ok := mp[defaultindex]
			if ok {
				temp.Desc.Fields[i] = t.Desc.Fields[idx]
				temp.Fields[i] = t.Fields[idx]
			}
		}
	}
	return temp, nil //replace me
}

// Compute a key for the tuple to be used in a map structure
// 计算要在映射结构中使用的元组的键
func (t *Tuple) tupleKey() any {

	//todo efficiency here is poor - hashstructure is probably slow
	hash, _ := hashstructure.Hash(t, hashstructure.FormatV2, nil)

	return hash
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
// 返回一个字符串，表示元组的表头
// 提供 TupleDesc。
//
// Aligned 指示元组是否应采用表格格式
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
// 返回表示元组的字符串
// Aligned 指示元组是否应采用表格格式
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = fmt.Sprintf("%d", f.Value)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr

}
