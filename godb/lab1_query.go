package godb

import "os"

// This function should load the csv file in fileName into a heap file (see
// [HeapFile.LoadFromCSV]) and then compute the sum of the integer field in
// string and return its value as an int The supplied csv file is comma
// delimited and has a header If the file doesn't exist or can't be opened, or
// the field doesn't exist, or the field is not and integer, should return an
// err. Note that when you create a HeapFile, you will need to supply a file
// name;  you can supply a non-existant file, in which case it will be created.
// However, subsequent invocations of this method will result in tuples being
// reinserted into this file unless you delete (e.g., with [os.Remove] it before
// calling NewHeapFile.
/*
// 此函数应将 fileName 中的 csv 文件加载到堆文件中（请参阅
  [HeapFile.LoadFromCSV])，然后计算字符串中整数字段的总和并将其值作为 int 返回
	提供的 csv 文件以逗号分隔并具有标题
	如果文件不存在或无法打开，或者 该字段不存在，或者该字段不是整数，应该返回错误。
	请注意，当您创建 HeapFile 时，您需要提供文件名； 您可以提供一个不存在的文件，在这种情况下将创建该文件。
  但是，后续调用此方法将导致元组被重新插入到此文件中，除非您删除（例如，在调用 NewHeapFile 之前使用 [os.Remove] 删除它）。
*/
func computeFieldSum(fileName string, td TupleDesc, sumField string) (int, error) {
	sum := 0
	lab1_bp := "lab1bp.dat"
	if _, err := os.Stat(lab1_bp); err == nil {
		os.Remove(lab1_bp)
	} else {
		f, err := os.OpenFile(lab1_bp, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return 0, err
		}
		defer f.Close()
	}
	bp := NewBufferPool(30)
	hp, err := NewHeapFile(lab1_bp, &td, bp)
	if err != nil {
		return 0, err
	}
	csvFile, _ := os.Open(fileName)
	err = hp.LoadFromCSV(csvFile, true, ",", false)
	if err != nil {
		return 0, nil
	}
	tid := NewTID()
	iter, _ := hp.Iterator(tid)
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		for _, field := range t.Fields {
			if _, a := field.(IntField); a {
				sum = sum + (int)(field.(IntField).Value)
			}
		}
	}
	return sum, nil // replace me
}
