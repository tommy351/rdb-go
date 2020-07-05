package rdb

// SetHead contains the key and the length of a set. It is returned when a set
// is read first time.
type SetHead struct {
	DataKey
	Length int64
}

// SetEntry is returned when a new set entry is read.
type SetEntry struct {
	DataKey
	Index  int64
	Length int64
	Value  interface{}
}

// SetData is returned when all entries in a set are all read.
type SetData struct {
	DataKey
	Value []interface{}
}

type setMapper struct{}

func (setMapper) MapHead(head *collectionHead) (interface{}, error) {
	return &SetHead{
		DataKey: head.DataKey,
		Length:  head.Length,
	}, nil
}

func (setMapper) MapEntry(element *collectionEntry) (interface{}, error) {
	return &SetEntry{
		DataKey: element.DataKey,
		Index:   element.Index,
		Length:  element.Length,
		Value:   element.Value,
	}, nil
}

func (setMapper) MapSlice(slice *collectionSlice) (interface{}, error) {
	return &SetData{
		DataKey: slice.DataKey,
		Value:   slice.Value,
	}, nil
}
