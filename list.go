package rdb

import "io"

type ListHead struct {
	DataKey
	Length int64
}

type ListEntry struct {
	DataKey
	Index  int64
	Length int64
	Value  interface{}
}

type ListData struct {
	DataKey
	Value []interface{}
}

type listMapper struct{}

func (listMapper) MapHead(head *collectionHead) (interface{}, error) {
	return &ListHead{
		DataKey: head.DataKey,
		Length:  head.Length,
	}, nil
}

func (listMapper) MapEntry(element *collectionEntry) (interface{}, error) {
	return &ListEntry{
		DataKey: element.DataKey,
		Index:   element.Index,
		Length:  element.Length,
		Value:   element.Value,
	}, nil
}

func (listMapper) MapSlice(slice *collectionSlice) (interface{}, error) {
	return &ListData{
		DataKey: slice.DataKey,
		Value:   slice.Value,
	}, nil
}

type listZipListValueReader struct{}

func (listZipListValueReader) ReadValue(r io.Reader) (interface{}, error) {
	return readZipListEntry(r)
}
