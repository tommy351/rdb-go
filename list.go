package rdb

import (
	"fmt"
	"io"
)

// ListHead contains the key and the length of a list. It is returned when a list
// is read first time. The length may be incorrect when the list is backed by a
// quicklist data structure.
type ListHead struct {
	DataKey
	Length int64
}

// ListEntry is returned when a new list entry is read.
type ListEntry struct {
	DataKey
	Index  int64
	Length int64
	Value  interface{}
}

// ListData is returned when all entries in a list are all read.
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
	value, err := readZipListEntry(r)

	if err != nil {
		return nil, fmt.Errorf("failed to read list value from ziplist: %w", err)
	}

	return value, nil
}
