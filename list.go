package rdb

import "io"

type ListHead struct {
	DataKey
	Length int64
}

type ListElement struct {
	DataKey
	Index  int64
	Length int64
	Value  interface{}
}

type ListData struct {
	DataKey
	Value []interface{}
}

var _ collectionMapper = listMapper{}

type listMapper struct{}

func (listMapper) MapHead(head *collectionHead) (interface{}, error) {
	return &ListHead{
		DataKey: head.DataKey,
		Length:  head.Length,
	}, nil
}

func (listMapper) MapElement(element *collectionElement) (interface{}, error) {
	return &ListElement{
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

var _ valueReader = listZipListValueReader{}

type listZipListValueReader struct{}

func (listZipListValueReader) ReadValue(r io.Reader) (interface{}, error) {
	return readZipListEntry(r)
}
